"""C2 gates: isolated PostgreSQL, real gRPC, SQLite and bytes, Linux processes."""

import hashlib
import os
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Thread
from uuid import uuid4

import grpc
import pytest
import v2_storage_pb2 as pb
import v2_storage_pb2_grpc as rpc
from test_v2_c1 import claim, upload
from test_v2_metadata import foundation as foundation
from test_v2_metadata import sql

from metadata.v2.repository import DomainError, JobLease, execute
from metadata.v2.storage_client import StorageClient, evidence, key_for, spec_for
from metadata.v2.worker import Work, Worker, WorkerConfig
from storage.v2.ledger import Ledger, Scope
from storage.v2.server import create_server


@pytest.fixture
def cluster(foundation, tmp_path):
    repo, resources = foundation, []
    try:
        for i in range(2):
            node, inc = f"node-{i}", uuid4()
            ledger = Ledger(
                tmp_path / node,
                Scope(str(repo.namespace_id), str(repo.epoch), node, str(inc)),
                provision=True,
                chunk_limit=3,
            )
            server, port, executor = create_server(ledger)
            resources.append((ledger, server, executor))
            server.start()
            repo.provision_node(
                node, inc, host="127.0.0.1", port=port, failure_domain=node, budget=300
            )
            repo.activate_node(
                node, inc, observed_namespace=repo.namespace_id, observed_epoch=repo.epoch
            )
        yield repo, resources
    finally:
        for ledger, server, executor in resources:
            server.stop(0).wait()
            executor.shutdown(wait=True)
            ledger.close()


def store(part, data):
    proofs = []
    for d in part["destinations"]:
        with StorageClient.channel(d) as channel:
            stub = rpc.StorageServiceStub(channel)
            stub.PrepareAllocation(
                pb.PrepareAllocationRequest(
                    key=key_for(d), generation=d["generation"], chunk=spec_for(d)
                ),
                timeout=5,
            )
            frames = [
                pb.StoreChunkFrame(
                    header=pb.StoreChunkHeader(
                        key=key_for(d), generation=d["generation"], chunk=spec_for(d)
                    )
                ),
                pb.StoreChunkFrame(block=pb.DataBlock(offset=0, data=data)),
            ]
            record = stub.StoreChunk(iter(frames), timeout=5).allocation
            proofs.append(evidence(record, d))
    return proofs


def confirmed(repo, data=b"abcdefghi"):
    session = upload(repo, data)
    for i in range(session["part_count"]):
        block = data[3 * i : 3 * i + 3]
        part = claim(repo, session, index=i, data=block)
        repo.confirm_part(
            session["upload_id"], i, part["claim_token"], part["revision"], store(part, block)
        )
    return session


def run_job(worker, kind):
    jobs = worker.repo.claim_jobs(worker.owner, 1, kinds=(kind,))
    assert len(jobs) == 1
    return worker.execute(jobs[0])


def due(repo):
    sql(repo, "UPDATE jobs SET run_after=clock_timestamp()-interval '1 second'")


def expire_jobs(repo):
    sql(
        repo,
        "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' "
        "WHERE state='RUNNING'",
    )


def charged(repo):
    return sql(repo, "SELECT coalesce(sum(charged_bytes),0) AS n FROM node_budgets")[0]["n"]


def job_state(repo, kind):
    return sql(repo, "SELECT state FROM jobs WHERE kind=:kind ORDER BY created_at DESC", kind=kind)[
        0
    ]["state"]


def test_lost_store_ack_sweeper_reconcile_then_stream_finalize(cluster):
    repo, resources = cluster
    session = upload(repo)
    part = claim(repo, session)
    store(part, b"abc")  # Transfer dies before confirm.
    sql(repo, "UPDATE upload_parts SET claim_expires_at=clock_timestamp()-interval '1 second'")
    assert repo.sweep_claims() == 1
    worker = Worker(repo)
    try:
        result = run_job(worker, "RECONCILE")
        assert result["state"] == "VERIFIED" and not result["claim_held"]
        assert repo.get_session(session["upload_id"])["active_claims"] == 0
        repo.complete_upload(session["upload_id"])
        assert run_job(worker, "FINALIZE")["state"] == "COMPLETED"
        assert job_state(repo, "FINALIZE") == "DONE" and charged(repo) == 6
        for ledger, _, _ in resources:
            assert len(ledger.inventory(ledger.scope)[0]) == 1
            assert next((ledger.root / "chunks").iterdir()).read_bytes() == b"abc"
    finally:
        worker.close()


def test_background_reconcile_does_not_claim_for_absent_consumer(cluster):
    repo, _ = cluster
    session = upload(repo)
    part = claim(repo, session)
    repo.uncertain(session["upload_id"], 0, part["claim_token"], part["revision"])
    worker = Worker(repo)
    try:
        assert run_job(worker, "RECONCILE") is None
        assert job_state(repo, "RECONCILE") == "RETRY"
        assert charged(repo) == 6
        assert sql(repo, "SELECT state,claim_held FROM upload_parts")[0] == {
            "state": "RECONCILING",
            "claim_held": False,
        }
        assert {a["generation"] for a in sql(repo, "SELECT generation FROM allocations")} == {1}
        due(repo)
        owner = uuid4()
        result = worker.retry_for_consumer(part["chunk_id"], owner)
        assert result["claim_owner"] == owner and result["claim_held"]
        assert {d["generation"] for d in result["destinations"]} == {2}
        proofs = store(result, b"abc")
        repo.confirm_part(
            session["upload_id"], 0, result["claim_token"], result["revision"], proofs
        )
        assert charged(repo) == 6
    finally:
        worker.close()


def test_cleanup_pages_and_lost_delete_ack_hold_charge_until_retry(cluster):
    repo, resources = cluster
    session = confirmed(repo, b"abc" * 10)
    repo.abort(session["upload_id"])

    class LostAck(StorageClient):
        lost = False

        def unary(self, method, d, work):
            result = super().unary(method, d, work)
            if method == "DeleteAllocation" and not self.lost:
                self.lost = True
                raise DomainError("ACK_LOST")
            return result

    pages = []
    worker = Worker(repo, storage=LostAck(), hook=lambda p: pages.append(p))
    try:
        run_job(worker, "CLEANUP")
        assert charged(repo) == 60 and job_state(repo, "CLEANUP") == "RETRY"
        assert sql(repo, "SELECT count(*) n FROM allocations WHERE state='RELEASED'")[0]["n"] == 0
        due(repo)
        run_job(worker, "CLEANUP")
        assert pages.count("cleanup_intent") == 3  # failed first page, then 8 + 2 parts
        assert charged(repo) == 0 and job_state(repo, "CLEANUP") == "DONE"
        assert {a["state"] for a in sql(repo, "SELECT state FROM write_attempts")} == {"CLOSED"}
        for ledger, _, _ in resources:
            rows = ledger.inventory(ledger.scope)[0]
            assert len(rows) == 10 and all(r["cleanup_complete"] for r in rows)
            assert not list((ledger.root / "chunks").iterdir())
    finally:
        worker.close()


def test_cleanup_cannot_finish_with_earlier_unresolved_page(cluster):
    repo, _ = cluster
    session = confirmed(repo, b"abc" * 9)
    repo.abort(session["upload_id"])
    job = repo.claim_jobs(uuid4(), 1, kinds=("CLEANUP",))[0]
    lease = JobLease(job["job_id"], job["lease_owner"], job["fencing_token"])
    repo.authorize_cleanup(session["upload_id"], lease, after_index=7)
    with pytest.raises(DomainError, match="OUTCOME_PENDING"):
        repo.finish_cleanup(session["upload_id"], lease)
    assert charged(repo) == 54


@pytest.mark.parametrize("kind", ["RECONCILE", "FINALIZE", "CLEANUP"])
def test_retry_budget_keeps_cleanup_obligation(cluster, kind):
    repo, _ = cluster
    session = upload(repo)
    part = claim(repo, session)
    if kind == "RECONCILE":
        repo.uncertain(session["upload_id"], 0, part["claim_token"], part["revision"])
    else:
        repo.confirm_part(
            session["upload_id"], 0, part["claim_token"], part["revision"], store(part, b"abc")
        )
        if kind == "FINALIZE":
            repo.complete_upload(session["upload_id"])
        else:
            repo.abort(session["upload_id"])

    class Unavailable(StorageClient):
        def unary(self, *args):
            raise DomainError("STORAGE_UNAVAILABLE")

        def read(self, *args):
            raise DomainError("STORAGE_UNAVAILABLE")

    worker = Worker(repo, storage=Unavailable(), config=WorkerConfig(max_attempts=2))
    try:
        run_job(worker, kind)
        assert job_state(repo, kind) == "RETRY"
        due(repo)
        run_job(worker, kind)
        assert job_state(repo, kind) == "DEAD"
        assert charged(repo) == 6
        session = repo.get_session(session["upload_id"])
        assert session["state"] == ("ABORTED" if kind == "CLEANUP" else "FAILED")
        assert session["admission_released_at"] is not None
        assert sql(repo, "SELECT held_count FROM admission_counters")[0]["held_count"] == 0
        assert sql(repo, "SELECT 1 FROM jobs WHERE kind='CLEANUP'")
    finally:
        worker.close()


@pytest.mark.parametrize("data", [b"", b"abc", b"abcdefghi"])
def test_finalize_hashes_ordered_actual_bytes_and_empty(cluster, data):
    repo, _ = cluster
    session = confirmed(repo, data)
    repo.complete_upload(session["upload_id"])
    worker = Worker(repo, config=WorkerConfig(page_size=1))
    try:
        assert run_job(worker, "FINALIZE")["state"] == "COMPLETED"
        file = sql(repo, "SELECT * FROM files")[0]
        assert (
            file["state"] == "AVAILABLE"
            and file["actual_sha256"] == hashlib.sha256(data).hexdigest()
        )
    finally:
        worker.close()


def test_full_hash_mismatch_fails_and_cleanup_runs(cluster):
    repo, _ = cluster
    session = upload(repo, b"xyz")
    part = claim(repo, session, data=b"abc")
    repo.confirm_part(
        session["upload_id"], 0, part["claim_token"], part["revision"], store(part, b"abc")
    )
    repo.complete_upload(session["upload_id"])
    worker = Worker(repo)
    try:
        assert run_job(worker, "FINALIZE")["error_code"] == "CHECKSUM_MISMATCH"
        assert charged(repo) == 6
        run_job(worker, "CLEANUP")
        assert charged(repo) == 0
    finally:
        worker.close()


@pytest.mark.parametrize("change", ["abort", "expire_lease", "reclaim", "epoch"])
def test_late_finalize_never_publishes(cluster, change):
    repo, _ = cluster
    session = confirmed(repo)
    repo.complete_upload(session["upload_id"])

    def hook(point):
        if point != "before_publish":
            return
        if change == "abort":
            repo.abort(session["upload_id"])
        elif change == "epoch":
            sql(repo, "UPDATE cluster_control SET mode='RECOVERING'")
        else:
            expire_jobs(repo)
            if change == "reclaim":
                assert repo.claim_jobs(uuid4(), 1, kinds=("FINALIZE",))

    worker = Worker(repo, hook=hook)
    try:
        assert run_job(worker, "FINALIZE") is None
        assert sql(repo, "SELECT state FROM files")[0]["state"] != "AVAILABLE"
        assert charged(repo) == 18
    finally:
        worker.close()


def test_finalize_fallback_does_not_hash_failed_replica_prefix(cluster):
    repo, _ = cluster
    session = confirmed(repo)
    repo.complete_upload(session["upload_id"])

    class Partial(StorageClient):
        failed = False

        def read(self, d, digest, work):
            if not self.failed:
                self.failed = True
                # Exercise the real parser with a truncated stream, then fallback.
                with self.channel(d) as channel:
                    stream = rpc.StorageServiceStub(channel).GetChunk(
                        pb.GetChunkRequest(key=key_for(d), expected_chunk=spec_for(d)), timeout=5
                    )
                    assert next(stream).WhichOneof("payload") == "header"
                    assert next(stream).WhichOneof("payload") == "block"
                    stream.cancel()
                raise DomainError("TRUNCATED")
            return super().read(d, digest, work)

    worker = Worker(repo, storage=Partial())
    try:
        assert run_job(worker, "FINALIZE")["state"] == "COMPLETED"
    finally:
        worker.close()


def test_lease_renewal_and_expired_renew_is_rejected(cluster):
    repo, _ = cluster
    session = confirmed(repo, b"")
    repo.complete_upload(session["upload_id"])
    job = repo.claim_jobs(uuid4(), 1, kinds=("FINALIZE",))[0]
    lease = JobLease(job["job_id"], job["lease_owner"], job["fencing_token"])
    sql(repo, "UPDATE jobs SET lease_expires_at=clock_timestamp()+interval '2 seconds'")
    config = WorkerConfig(renew_seconds=0.05)
    with Work(repo, lease, config, Event()) as work:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            expiry = sql(
                repo, "SELECT lease_expires_at>clock_timestamp()+interval '30 seconds' n FROM jobs"
            )[0]["n"]
            if expiry:
                break
            time.sleep(0.02)
        assert expiry
        expire_jobs(repo)
        assert work.cancelled.wait(3)
        with pytest.raises(DomainError, match="WORK_CANCELLED"):
            work.check()
    with pytest.raises(DomainError, match="STALE_JOB"):
        repo.renew_job(lease.job_id, lease.owner, lease.token)


def test_bounded_loop_cleanup_progress_and_shutdown(cluster):
    repo, _ = cluster
    session = confirmed(repo)
    repo.complete_upload(session["upload_id"])
    cleanup_session = confirmed(repo, b"abc")
    repo.abort(cleanup_session["upload_id"])
    reading, released = Event(), Event()

    class Slow(StorageClient):
        def read(self, d, digest, work):
            reading.set()
            while not released.wait(0.01):
                work.check()
            return super().read(d, digest, work)

    worker = Worker(repo, storage=Slow(), config=WorkerConfig(poll_seconds=0.02))
    thread = Thread(target=worker.run)
    thread.start()
    try:
        assert reading.wait(5)
        assert worker.ready()
        deadline = time.monotonic() + 5
        while job_state(repo, "CLEANUP") != "DONE" and time.monotonic() < deadline:
            time.sleep(0.02)
        assert job_state(repo, "CLEANUP") == "DONE"
        assert len(worker.futures) <= 2
        assert list(worker.futures.values()).count("FINALIZE") <= 1
        before = time.monotonic()
        worker.stopping.set()
        thread.join(5)
        assert not thread.is_alive() and time.monotonic() - before < 5
        assert not worker.ready()
        assert repo.get_session(session["upload_id"])["state"] == "FINALIZING"
        expire_jobs(repo)
        replacement = Worker(repo)
        try:
            assert run_job(replacement, "FINALIZE")["state"] == "COMPLETED"
        finally:
            replacement.close()
    finally:
        released.set()
        worker.stopping.set()
        thread.join(5)


def crash_worker(repo, point, *, chunk=None, owner=None):
    script = """
import os, signal, sys
from uuid import UUID
from sqlalchemy import create_engine
from metadata.v2.repository import Repository
from metadata.v2.worker import Worker
def hook(point):
    if point == sys.argv[4]:
        os.kill(os.getpid(), signal.SIGKILL)
engine=create_engine(os.environ['TEST_DATABASE_URL'], connect_args={'connect_timeout':5})
repo=Repository(engine, UUID(sys.argv[1]), UUID(sys.argv[2]), schema=sys.argv[3])
worker=Worker(repo, hook=hook)
if sys.argv[5]:
    worker.retry_for_consumer(UUID(sys.argv[5]), UUID(sys.argv[6]))
else:
    worker.run()
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(repo.namespace_id),
            str(repo.epoch),
            repo.schema,
            point,
            str(chunk or ""),
            str(owner or ""),
        ],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == -signal.SIGKILL, result.stderr


@pytest.mark.parametrize("point", ["claimed", "chunk_verified", "before_publish", "published"])
def test_process_crash_finalize_commit_boundaries(cluster, point):
    repo, _ = cluster
    session = confirmed(repo)
    repo.complete_upload(session["upload_id"])
    crash_worker(repo, point)
    if point == "published":
        assert repo.get_session(session["upload_id"])["state"] == "COMPLETED"
        assert job_state(repo, "FINALIZE") == "DONE"
    else:
        assert repo.get_session(session["upload_id"])["state"] == "FINALIZING"
        expire_jobs(repo)
        worker = Worker(repo)
        try:
            assert run_job(worker, "FINALIZE")["state"] == "COMPLETED"
        finally:
            worker.close()
    assert charged(repo) == 18
    assert sql(repo, "SELECT held_count FROM admission_counters")[0]["held_count"] == 0


@pytest.mark.parametrize("point", ["cleanup_intent", "delete_ack", "release_committed"])
def test_process_crash_cleanup_commit_boundaries(cluster, point):
    repo, _ = cluster
    session = confirmed(repo)
    repo.abort(session["upload_id"])
    crash_worker(repo, point)
    assert job_state(repo, "CLEANUP") == "RUNNING"
    assert charged(repo) == (15 if point == "release_committed" else 18)
    expire_jobs(repo)
    worker = Worker(repo)
    try:
        run_job(worker, "CLEANUP")
        assert job_state(repo, "CLEANUP") == "DONE" and charged(repo) == 0
    finally:
        worker.close()


@pytest.mark.parametrize("point", ["retry_intent", "before_retry", "retry_committed"])
def test_process_crash_retry_handoff_commit_boundaries(cluster, point):
    repo, _ = cluster
    session = upload(repo)
    part = claim(repo, session)
    repo.uncertain(session["upload_id"], 0, part["claim_token"], part["revision"])
    crash_worker(repo, point, chunk=part["chunk_id"], owner=uuid4())
    if point == "retry_committed":
        sql(repo, "UPDATE upload_parts SET claim_expires_at=clock_timestamp()-interval '1 second'")
        repo.sweep_claims()
    expire_jobs(repo)
    worker = Worker(repo)
    try:
        result = worker.retry_for_consumer(part["chunk_id"], uuid4())
        assert result and result["claim_held"]
        assert {d["generation"] for d in result["destinations"]} == {3}
        repo.confirm_part(
            session["upload_id"],
            0,
            result["claim_token"],
            result["revision"],
            store(result, b"abc"),
        )
        assert charged(repo) == 6
    finally:
        worker.close()


def test_partial_generation_intent_recovers_only_after_all_prepare_acks(cluster):
    repo, _ = cluster
    session = upload(repo)
    part = claim(repo, session)
    # Both nodes contain generation 1 but only one receives the committed generation 2 intent.
    for d in part["destinations"]:
        with StorageClient.channel(d) as channel:
            rpc.StorageServiceStub(channel).PrepareAllocation(
                pb.PrepareAllocationRequest(key=key_for(d), generation=1, chunk=spec_for(d)),
                timeout=5,
            )
    repo.uncertain(session["upload_id"], 0, part["claim_token"], part["revision"])
    job = repo.claim_jobs(uuid4(), 1, kinds=("RECONCILE",))[0]
    lease = JobLease(job["job_id"], job["lease_owner"], job["fencing_token"])
    intent = repo.authorize_part_retry(session["upload_id"], 0, part["revision"], uuid4(), lease)
    d = intent["destinations"][0]
    with StorageClient.channel(d) as channel:
        rpc.StorageServiceStub(channel).PrepareAllocation(
            pb.PrepareAllocationRequest(key=key_for(d), generation=2, chunk=spec_for(d)), timeout=5
        )
    expire_jobs(repo)
    worker = Worker(repo)
    try:
        result = worker.retry_for_consumer(part["chunk_id"], uuid4())
        assert result and {d["generation"] for d in result["destinations"]} == {3}
        assert charged(repo) == 6
        assert {
            r["state"] for r in sql(repo, "SELECT state FROM write_attempts WHERE generation<3")
        } == {"CLOSED"}
    finally:
        worker.close()


def test_live_old_writer_must_drain_before_consumer_claim(cluster):
    repo, resources = cluster
    session = upload(repo)
    part = claim(repo, session)
    d = part["destinations"][0]
    ledger = next(item for item, _, _ in resources if item.scope.node_id == d["node_id"])
    release = Event()

    def frames():
        yield pb.StoreChunkFrame(
            header=pb.StoreChunkHeader(key=key_for(d), generation=1, chunk=spec_for(d))
        )
        release.wait(10)
        yield pb.StoreChunkFrame(block=pb.DataBlock(offset=0, data=b"abc"))

    with StorageClient.channel(d) as channel:
        stub = rpc.StorageServiceStub(channel)
        stub.PrepareAllocation(
            pb.PrepareAllocationRequest(key=key_for(d), generation=1, chunk=spec_for(d)), timeout=5
        )
        old_write = stub.StoreChunk.future(frames(), timeout=10)
        worker = Worker(repo)
        try:
            deadline = time.monotonic() + 3
            while ledger.lookup(str(d["allocation_id"]))["state"] != "WRITING":
                assert time.monotonic() < deadline
                time.sleep(0.01)
            repo.uncertain(session["upload_id"], 0, part["claim_token"], part["revision"])
            assert worker.retry_for_consumer(part["chunk_id"], uuid4()) is None
            assert ledger.lookup(str(d["allocation_id"]))["generation"] == 2
            assert sql(repo, "SELECT claim_held FROM upload_parts")[0]["claim_held"] is False
            assert charged(repo) == 6
            release.set()
            with pytest.raises(grpc.RpcError):
                old_write.result(timeout=5)
            due(repo)
            result = worker.retry_for_consumer(part["chunk_id"], uuid4())
            assert result and result["claim_held"]
            repo.confirm_part(
                session["upload_id"],
                0,
                result["claim_token"],
                result["revision"],
                store(result, b"abc"),
            )
            assert charged(repo) == 6
        finally:
            release.set()
            old_write.cancel()
            worker.close()


@pytest.mark.parametrize(
    "fault",
    [
        "scope",
        "generation",
        "spec",
        "actual",
        "unknown_state",
        "offset",
        "large_block",
        "no_footer",
        "duplicate_header",
        "duplicate_footer",
        "footer_hash",
        "failed_eof",
    ],
)
def test_finalize_rejects_malformed_wire_stream_without_changing_prefix(cluster, fault):
    repo, _ = cluster
    session = upload(repo)
    part = claim(repo, session)
    store(part, b"abc")
    d = part["destinations"][0]
    with StorageClient.channel(d) as channel:
        record = rpc.StorageServiceStub(channel).InspectAllocation(
            pb.AllocationRequest(key=key_for(d), generation=1), timeout=5
        )
    if fault == "scope":
        record.key.scope.node_incarnation = str(uuid4())
    elif fault == "generation":
        record.generation += 1
    elif fault == "spec":
        record.chunk.checksum_sha256 = "a" * 64
    elif fault == "actual":
        record.ClearField("actual_checksum_sha256")
    elif fault == "unknown_state":
        record.state = 999

    class Malformed(rpc.StorageServiceServicer):
        def GetChunk(self, request, context):
            yield pb.GetChunkFrame(header=record)
            if fault == "duplicate_header":
                yield pb.GetChunkFrame(header=record)
            yield pb.GetChunkFrame(
                block=pb.DataBlock(
                    offset=1 if fault == "offset" else 0,
                    data=b"x" * 262145 if fault == "large_block" else b"abc",
                )
            )
            if fault == "no_footer":
                return
            footer = pb.GetChunkFrame(
                footer=pb.GetChunkFooter(
                    actual_size_bytes=3,
                    actual_checksum_sha256="a" * 64
                    if fault == "footer_hash"
                    else part["checksum_sha256"],
                )
            )
            yield footer
            if fault == "duplicate_footer":
                yield footer
            if fault == "failed_eof":
                context.abort(grpc.StatusCode.UNAVAILABLE, "EOF lost after footer")

    executor = ThreadPoolExecutor(max_workers=1)
    server = grpc.server(executor)
    rpc.add_StorageServiceServicer_to_server(Malformed(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    repo.uncertain(session["upload_id"], 0, part["claim_token"], part["revision"])
    job = repo.claim_jobs(uuid4(), 1, kinds=("RECONCILE",))[0]
    lease = JobLease(job["job_id"], job["lease_owner"], job["fencing_token"])
    prefix = hashlib.sha256(b"prior chunk")
    try:
        with Work(repo, lease, WorkerConfig(), Event()) as work:
            with pytest.raises((DomainError, grpc.RpcError)):
                StorageClient().read(dict(d, port=port), prefix, work)
        assert prefix.hexdigest() == hashlib.sha256(b"prior chunk").hexdigest()
        assert charged(repo) == 6
    finally:
        server.stop(0).wait()
        executor.shutdown(wait=True)


def test_worker_entrypoint_sigterm_cancels_real_grpc_and_can_restart(cluster):
    repo, _ = cluster
    session = confirmed(repo)
    repo.complete_upload(session["upload_id"])
    endpoints = {
        row["node_id"]: row["port"] for row in sql(repo, "SELECT node_id,port FROM storage_nodes")
    }
    entered, drained = Event(), Event()

    class Blocked(rpc.StorageServiceServicer):
        def GetChunk(self, request, context):
            port = endpoints[request.key.scope.node_id]
            with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                call = rpc.StorageServiceStub(channel).GetChunk(request, timeout=5)
                try:
                    yield next(call)
                    entered.set()
                    while context.is_active():
                        time.sleep(0.01)
                finally:
                    call.cancel()
                    drained.set()

    executor = ThreadPoolExecutor(max_workers=2)
    server = grpc.server(executor)
    rpc.add_StorageServiceServicer_to_server(Blocked(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    sql(repo, "UPDATE storage_nodes SET port=:port", port=port)
    env = dict(
        os.environ,
        V2_DATABASE_URL=os.environ["TEST_DATABASE_URL"],
        V2_NAMESPACE_ID=str(repo.namespace_id),
        V2_RECOVERY_EPOCH=str(repo.epoch),
        V2_DATABASE_SCHEMA=repo.schema,
    )
    child = subprocess.Popen(
        [sys.executable, "-m", "metadata.v2.worker"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert entered.wait(5)
        child.terminate()
        _, errors = child.communicate(timeout=5)
        assert child.returncode == 0, errors
        assert drained.wait(3)
        assert repo.get_session(session["upload_id"])["state"] == "FINALIZING"
        assert job_state(repo, "FINALIZE") == "RUNNING"
        assert charged(repo) == 18
        for node, port in endpoints.items():
            sql(
                repo,
                "UPDATE storage_nodes SET port=:port WHERE node_id=:node",
                port=port,
                node=node,
            )
        expire_jobs(repo)
        replacement = Worker(repo)
        try:
            assert run_job(replacement, "FINALIZE")["state"] == "COMPLETED"
        finally:
            replacement.close()
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate(timeout=5)
        server.stop(0).wait()
        executor.shutdown(wait=True)


def test_exhausted_job_rechecks_lease_after_failure_domain_effects(cluster, monkeypatch):
    repo, _ = cluster
    session = confirmed(repo)
    repo.complete_upload(session["upload_id"])
    job = repo.claim_jobs(uuid4(), 1, kinds=("FINALIZE",))[0]
    lease = JobLease(job["job_id"], job["lease_owner"], job["fencing_token"])
    original = repo.enqueue

    def expired_during_enqueue(db, kind, target, payload=None):
        result = original(db, kind, target, payload)
        # Simulate expiry while the failure transaction is enqueuing cleanup.
        # The final live check must roll back ALL tentative domain effects.
        execute(
            db,
            "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' "
            "WHERE job_id=:id",
            id=lease.job_id,
        )
        return result

    monkeypatch.setattr(repo, "enqueue", expired_during_enqueue)
    with pytest.raises(DomainError, match="STALE_JOB"):
        repo.fail_worker_job(job, lease, error="STORAGE_UNAVAILABLE", max_attempts=1)
    assert repo.get_session(session["upload_id"])["state"] == "FINALIZING"
    assert job_state(repo, "FINALIZE") == "RUNNING"
    assert not sql(repo, "SELECT 1 FROM jobs WHERE kind='CLEANUP'")
    assert sql(repo, "SELECT held_count FROM admission_counters")[0]["held_count"] == 1
    assert charged(repo) == 18
