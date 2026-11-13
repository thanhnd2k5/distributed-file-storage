"""F2 automatic repair execution, streaming copy, recovery and cleanup on real PostgreSQL/gRPC."""

import hashlib
import logging
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from pathlib import Path
from threading import Event, Thread
from uuid import UUID, uuid4

import grpc
import pytest
import v2_storage_pb2 as pb
import v2_storage_pb2_grpc as rpc
from test_v2_c1 import claim, upload
from test_v2_c2 import due, expire_jobs, job_state
from test_v2_e2 import accounting
from test_v2_metadata import foundation as foundation
from test_v2_metadata import sql

from metadata.v2.repository import DomainError, JobLease
from metadata.v2.storage_client import StorageClient, evidence, key_for, spec_for
from metadata.v2.worker import Worker, WorkerConfig
from storage.v2.ledger import Ledger, Scope
from storage.v2.server import create_server


@dataclass
class NodeHandle:
    node_id: str
    incarnation: UUID
    failure_domain: str
    root: Path
    chunk_limit: int
    ledger: Ledger | None
    server: grpc.Server | None
    executor: ThreadPoolExecutor | None
    port: int

    def stop(self):
        if self.server is not None:
            self.server.stop(0).wait()
            self.server = None
        if self.executor is not None:
            self.executor.shutdown(wait=True)
            self.executor = None
        if self.ledger is not None:
            self.ledger.close()
            self.ledger = None

    def restart(self, repo, scope_override=None):
        self.stop()
        scope = scope_override or Scope(
            str(repo.namespace_id), str(repo.epoch), self.node_id, str(self.incarnation)
        )
        self.ledger = Ledger(self.root, scope, provision=False, chunk_limit=self.chunk_limit)
        self.server, self.port, self.executor = create_server(
            self.ledger, failure_domain=self.failure_domain
        )
        self.server.start()
        sql(
            repo,
            "UPDATE storage_nodes SET port=:port WHERE node_id=:node",
            port=self.port,
            node=self.node_id,
        )
        return self.port


class ClusterHarness:
    def __init__(self, repo, root: Path):
        self.repo = repo
        self.root = root
        self.nodes: list[NodeHandle] = []

    def add_node(
        self,
        node_id: str,
        *,
        domain: str | None = None,
        budget: int = 500,
        chunk_limit: int = 3,
        activate: bool = True,
    ) -> NodeHandle:
        inc = uuid4()
        domain = domain or f"domain-{node_id}"
        node_root = self.root / node_id
        ledger = Ledger(
            node_root,
            Scope(str(self.repo.namespace_id), str(self.repo.epoch), node_id, str(inc)),
            provision=True,
            chunk_limit=chunk_limit,
        )
        server, port, executor = create_server(ledger, failure_domain=domain)
        server.start()
        handle = NodeHandle(
            node_id=node_id,
            incarnation=inc,
            failure_domain=domain,
            root=node_root,
            chunk_limit=chunk_limit,
            ledger=ledger,
            server=server,
            executor=executor,
            port=port,
        )
        self.nodes.append(handle)
        self.repo.provision_node(
            node_id, inc, host="127.0.0.1", port=port, failure_domain=domain, budget=budget
        )
        if activate:
            self.repo.activate_node(
                node_id,
                inc,
                observed_namespace=self.repo.namespace_id,
                observed_epoch=self.repo.epoch,
            )
        return handle

    def close(self):
        for handle in self.nodes:
            handle.stop()


@pytest.fixture
def f2_cluster(foundation, tmp_path):
    harness = ClusterHarness(foundation, tmp_path)
    try:
        yield foundation, harness
    finally:
        harness.close()


def until(predicate, seconds=10.0):
    due_at = time.monotonic() + seconds
    while not predicate():
        assert time.monotonic() < due_at, "Timed out waiting for convergence"
        time.sleep(0.01)


def store_blocks(part, data):
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

            def frames(dest=d):
                yield pb.StoreChunkFrame(
                    header=pb.StoreChunkHeader(
                        key=key_for(dest), generation=dest["generation"], chunk=spec_for(dest)
                    )
                )
                for off in range(0, len(data), 262144):
                    yield pb.StoreChunkFrame(
                        block=pb.DataBlock(offset=off, data=data[off : off + 262144])
                    )

            record = stub.StoreChunk(frames(), timeout=10).allocation
            proofs.append(evidence(record, d))
    return proofs


def make_available(repo, data):
    session = upload(repo, data)
    chunk_size = repo.policy.chunk_size_bytes
    for i in range(session["part_count"]):
        block = data[i * chunk_size : (i + 1) * chunk_size]
        part = claim(repo, session, index=i, data=block)
        repo.confirm_part(
            session["upload_id"],
            i,
            part["claim_token"],
            part["revision"],
            store_blocks(part, block),
        )
    repo.complete_upload(session["upload_id"])
    finalizer = Worker(repo)
    try:
        job = repo.claim_jobs(finalizer.owner, 1, kinds=("FINALIZE",), target=session["file_id"])[0]
        result = finalizer.execute(job)
        assert result["state"] == "COMPLETED"
    finally:
        finalizer.close()
    assert (
        sql(repo, "SELECT state FROM files WHERE file_id=:id", id=session["file_id"])[0]["state"]
        == "AVAILABLE"
    )
    return session


def read_verified_bytes(repo, file_id, *, expected_nodes=None):
    lease = repo.acquire_read(file_id)
    try:
        chunks = []
        cursor = -1
        client = StorageClient()
        while True:
            page = repo.read_manifest(
                lease["read_id"], lease["lease_token"], after_index=cursor, limit=4
            )
            for item in page["items"]:
                if expected_nodes is not None:
                    assert {d["node_id"] for d in item["destinations"]} == expected_nodes
                d = item["destinations"][0]
                with client.channel(d) as channel:
                    call = rpc.StorageServiceStub(channel).GetChunk(
                        pb.GetChunkRequest(key=key_for(d), expected_chunk=spec_for(d)), timeout=5
                    )
                    header = next(call)
                    assert evidence(header.header, d).state == "STORED"
                    buf = bytearray()
                    footer = None
                    for frame in call:
                        kind = frame.WhichOneof("payload")
                        if kind == "block":
                            buf.extend(frame.block.data)
                        elif kind == "footer":
                            footer = frame.footer
                        else:
                            pytest.fail(f"Unexpected frame {kind}")
                    assert footer is not None
                    assert len(buf) == item["size_bytes"]
                    assert hashlib.sha256(buf).hexdigest() == item["checksum_sha256"]
                    chunks.append(bytes(buf))
            if not page["has_more"]:
                break
            cursor = page["next_after_index"]
        return b"".join(chunks)
    finally:
        repo.release_read(lease["read_id"], lease["lease_token"])


def run_single(worker, kind, *, target=None):
    jobs = worker.repo.claim_jobs(worker.owner, 1, kinds=(kind,), target=target)
    assert len(jobs) == 1
    return worker.execute(jobs[0])


def crash_worker(repo, point):
    script = """
import os, signal, sys
from uuid import UUID
from sqlalchemy import create_engine
from metadata.v2.repository import Repository
from metadata.v2.worker import Worker, WorkerConfig
def hook(p):
    if p == sys.argv[4]:
        os.kill(os.getpid(), signal.SIGKILL)
engine = create_engine(os.environ["TEST_DATABASE_URL"], connect_args={"connect_timeout": 5})
repo = Repository(engine, UUID(sys.argv[1]), UUID(sys.argv[2]), schema=sys.argv[3])
worker = Worker(
    repo,
    config=WorkerConfig(poll_seconds=0.01, repair_scan_seconds=10.0),
    hook=hook,
)
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
        ],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == -signal.SIGKILL, result.stderr


def test_health_down_triggers_paged_discovery_repair_and_readable_bytes(f2_cluster, caplog):
    repo, cluster = f2_cluster
    n0 = cluster.add_node("node-0", domain="domain-0", budget=500, chunk_limit=3)
    cluster.add_node("node-1", domain="domain-1", budget=500, chunk_limit=3)

    # 10 chunks total (> page_size=8) across two files, plus one unrecoverable chunk later.
    first_data = b"abc" * 9
    second_data = b"xyz"
    f1 = make_available(repo, first_data)
    f2 = make_available(repo, second_data)
    f3 = make_available(repo, b"123")
    f3_chunk = sql(
        repo,
        "SELECT m.chunk_id FROM manifest_parts m "
        "JOIN files f USING(version_id) WHERE f.file_id=:id",
        id=f3["file_id"],
    )[0]["chunk_id"]
    sql(repo, "UPDATE allocations SET replica_health='CORRUPT' WHERE chunk_id=:c", c=f3_chunk)

    n2 = cluster.add_node("node-2", domain="domain-2", budget=500, chunk_limit=3)
    # Force all polls inactive except node-0 when node-0 crashes so health poll hits node-0 fast.
    n0.stop()
    sql(
        repo,
        "UPDATE node_health_polls SET next_poll_at=clock_timestamp() + interval '1 hour' "
        "WHERE node_id!='node-0'",
    )

    caplog.set_level(logging.WARNING, logger="metadata.v2.worker")
    worker = Worker(
        repo, config=WorkerConfig(poll_seconds=0.01, repair_scan_seconds=0.05, page_size=8)
    )
    thread = Thread(target=worker.run, daemon=True)
    thread.start()
    try:
        # Speed up the 3 real HealthCheck failures for node-0 without altering poll logic.
        for expected_failures in (1, 2, 3):
            until(
                lambda ef=expected_failures: (
                    sql(
                        repo,
                        "SELECT consecutive_failures FROM node_health_polls WHERE node_id='node-0'",
                    )[0]["consecutive_failures"]
                    >= ef
                )
            )
            if expected_failures < 3:
                sql(
                    repo,
                    "UPDATE node_health_polls SET next_poll_at=clock_timestamp() "
                    "WHERE node_id='node-0'",
                )

        until(
            lambda: (
                sql(repo, "SELECT health_state FROM storage_nodes WHERE node_id='node-0'")[0][
                    "health_state"
                ]
                == "DOWN"
            )
        )
        until(
            lambda: (
                sql(repo, "SELECT count(*) AS n FROM jobs WHERE kind='REPAIR' AND state='DONE'")[0][
                    "n"
                ]
                == 10
            )
        )
    finally:
        worker.stopping.set()
        thread.join(10)
        assert not thread.is_alive()

    # Zero-source chunk (f3) logged warning and was never enqueued.
    assert "zero eligible repair sources" in caplog.text
    assert sql(repo, "SELECT count(*) AS n FROM jobs WHERE kind='REPAIR'")[0]["n"] == 10
    assert (
        sql(
            repo,
            "SELECT count(*) AS n FROM allocations "
            "WHERE node_id='node-2' AND state='STORED' AND replica_health='VERIFIED'",
        )[0]["n"]
        == 10
    )
    assert len(n2.ledger.inventory(n2.ledger.scope)[0]) == 10

    # Reading exclusively from repaired replicas on node-2 verifies exact bytes & SHA-256.
    sql(repo, "UPDATE storage_nodes SET enabled=false WHERE node_id='node-1'")
    assert read_verified_bytes(repo, f1["file_id"], expected_nodes={"node-2"}) == first_data
    assert read_verified_bytes(repo, f2["file_id"], expected_nodes={"node-2"}) == second_data
    sql(repo, "UPDATE storage_nodes SET enabled=true WHERE node_id='node-1'")

    # node-0(11*3) + node-1(11*3) + node-2(10*3) = 96 charged bytes retained faithfully.
    accounting(repo, sessions=0, claims=0, charged=96)


def test_reclaimed_and_lost_ack_reuse_generation_and_existing_stored_without_recopy(f2_cluster):
    repo, cluster = f2_cluster
    cluster.add_node("node-0", domain="domain-0", budget=500, chunk_limit=3)
    cluster.add_node("node-1", domain="domain-1", budget=500, chunk_limit=3)
    session = make_available(repo, b"abc")
    n2 = cluster.add_node("node-2", domain="domain-2", budget=500, chunk_limit=3)
    sql(repo, "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-0'")
    (deficit,) = repo.repair_scan_page()["deficits"]
    repo.enqueue_repair(deficit["file_id"], deficit["version_id"], deficit["chunk_id"])

    class LostAfterStore(StorageClient):
        lost = False

        def copy(self, source, dest, work):
            proof = super().copy(source, dest, work)
            if not self.lost:
                self.lost = True
                raise DomainError("ACK_LOST")
            return proof

    w1 = Worker(repo, storage=LostAfterStore())
    try:
        job1 = repo.claim_jobs(w1.owner, 1, kinds=("REPAIR",), target=deficit["chunk_id"])[0]
        lease1 = JobLease(job1["job_id"], job1["lease_owner"], job1["fencing_token"])
        assert w1.execute(job1) is None
        assert job_state(repo, "REPAIR") == "RETRY"
    finally:
        w1.close()

    # Physical bytes ALREADY landed STORED on node-2 even though Postgres stayed RESERVED.
    stored_allocs = sql(
        repo,
        "SELECT allocation_id,state,generation FROM allocations WHERE node_id='node-2'",
    )
    assert len(stored_allocs) == 1
    assert stored_allocs[0]["state"] == "RESERVED"
    aid = stored_allocs[0]["allocation_id"]
    assert n2.ledger.lookup(str(aid))["state"] == "STORED"

    # Second attempt reuses existing STORED allocation via PrepareAllocation(gen=2) with 0 copies.
    due(repo)
    copies = []

    class CountingStorage(StorageClient):
        def copy(self, source, dest, work):
            copies.append((source["node_id"], dest["node_id"]))
            return super().copy(source, dest, work)

    w2 = Worker(repo, storage=CountingStorage())
    try:
        result = run_single(w2, "REPAIR", target=deficit["chunk_id"])
        assert result is not None and result.allocation_id == aid and result.generation == 2
        # Expired/stale first lease cannot confirm even after completion.
        with pytest.raises(DomainError, match="STALE_JOB"):
            repo.confirm_repair(deficit["chunk_id"], deficit["version_id"], lease1, result)
    finally:
        w2.close()

    assert copies == []
    assert job_state(repo, "REPAIR") == "DONE"
    assert sql(
        repo,
        "SELECT generation,state FROM write_attempts WHERE allocation_id=:id ORDER BY generation",
        id=aid,
    ) == [{"generation": 1, "state": "CLOSED"}, {"generation": 2, "state": "ACKED"}]
    accounting(repo, sessions=0, claims=0, charged=9)
    sql(repo, "UPDATE storage_nodes SET enabled=false WHERE node_id='node-1'")
    assert read_verified_bytes(repo, session["file_id"], expected_nodes={"node-2"}) == b"abc"


def test_destination_failures_cleanup_inline_and_on_unreachable_restart(f2_cluster):
    repo, cluster = f2_cluster
    cluster.add_node("node-0", domain="domain-0", budget=500, chunk_limit=3)
    cluster.add_node("node-1", domain="domain-1", budget=500, chunk_limit=3)
    s1 = make_available(repo, b"abc")
    s2 = make_available(repo, b"def")
    s3 = make_available(repo, b"ghi")
    n2 = cluster.add_node("node-2", domain="domain-2", budget=500, chunk_limit=3)
    n3 = cluster.add_node("node-3", domain="domain-3", budget=500, chunk_limit=3)
    sql(repo, "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-0'")

    def chunk_of(sess):
        return sql(
            repo,
            "SELECT m.chunk_id,f.version_id FROM manifest_parts m "
            "JOIN files f USING(version_id) WHERE f.file_id=:id",
            id=sess["file_id"],
        )[0]

    # Case A: Reachable destination ENOSPC (RESOURCE_EXHAUSTED on real StorageService StoreChunk)
    # -> abandons node-2 inline (DeleteAllocation -> RELEASED -> charged=0), finishes on node-3!
    p1 = chunk_of(s1)
    repo.enqueue_repair(s1["file_id"], p1["version_id"], p1["chunk_id"])
    orig_writable = n2.ledger.writable
    n2.ledger.writable = lambda size: False
    worker = Worker(repo)
    try:
        res1 = run_single(worker, "REPAIR", target=p1["chunk_id"])
    finally:
        worker.close()
        n2.ledger.writable = orig_writable

    assert res1.node_id == "node-3"
    a_n2 = sql(
        repo,
        "SELECT allocation_id,state,cleanup_complete_at FROM allocations "
        "WHERE node_id='node-2' AND chunk_id=:c",
        c=p1["chunk_id"],
    )[0]
    assert a_n2["state"] == "RELEASED" and a_n2["cleanup_complete_at"] is not None
    assert n2.ledger.lookup(str(a_n2["allocation_id"]))["cleanup_complete"] == 1
    assert (
        sql(repo, "SELECT charged_bytes FROM node_budgets WHERE node_id='node-2'")[0][
            "charged_bytes"
        ]
        == 0
    )
    assert (
        sql(
            repo,
            "SELECT state FROM jobs WHERE kind='CLEANUP' AND target_id=:aid",
            aid=a_n2["allocation_id"],
        )[0]["state"]
        == "DONE"
    )
    assert (
        sql(repo, "SELECT state FROM jobs WHERE kind='REPAIR' AND target_id=:c", c=p1["chunk_id"])[
            0
        ]["state"]
        == "DONE"
    )
    assert len(n3.ledger.inventory(n3.ledger.scope)[0]) == 1

    # Case B: Unreachable node-2 mid-repair -> A1 stays DELETE_PENDING retaining charge while
    # node-2 is down; repair finishes on node-3; restarting node-2 runs CLEANUP -> charge=0!
    p2 = chunk_of(s2)
    repo.enqueue_repair(s2["file_id"], p2["version_id"], p2["chunk_id"])
    stopped_once = False

    def stop_n2_on_intent(point):
        nonlocal stopped_once
        if point == "repair_intent" and not stopped_once:
            stopped_once = True
            n2.stop()

    w_rot = Worker(repo, hook=stop_n2_on_intent)
    try:
        res2 = run_single(w_rot, "REPAIR", target=p2["chunk_id"])
    finally:
        w_rot.close()

    assert res2.node_id == "node-3"
    abandoned_row = sql(
        repo,
        "SELECT allocation_id,state FROM allocations WHERE node_id='node-2' AND chunk_id=:c",
        c=p2["chunk_id"],
    )[0]
    assert abandoned_row["state"] == "DELETE_PENDING"
    assert (
        sql(repo, "SELECT charged_bytes FROM node_budgets WHERE node_id='node-2'")[0][
            "charged_bytes"
        ]
        == 3
    )
    cleanup_job = sql(
        repo,
        "SELECT * FROM jobs WHERE kind='CLEANUP' AND target_id=:aid",
        aid=abandoned_row["allocation_id"],
    )[0]
    assert cleanup_job["state"] in ("READY", "RETRY")

    # Restart node-2 with same volume and run CLEANUP(allocation:aid).
    n2.restart(repo)
    due(repo)
    w_clean = Worker(repo)
    try:
        assert run_single(w_clean, "CLEANUP", target=abandoned_row["allocation_id"]) is True
    finally:
        w_clean.close()

    assert (
        sql(
            repo,
            "SELECT state FROM allocations WHERE allocation_id=:aid",
            aid=abandoned_row["allocation_id"],
        )[0]["state"]
        == "RELEASED"
    )
    assert (
        sql(repo, "SELECT charged_bytes FROM node_budgets WHERE node_id='node-2'")[0][
            "charged_bytes"
        ]
        == 0
    )
    assert (
        sql(repo, "SELECT state FROM jobs WHERE job_id=:id", id=cleanup_job["job_id"])[0]["state"]
        == "DONE"
    )
    assert n2.ledger.lookup(str(abandoned_row["allocation_id"]))["cleanup_complete"] == 1

    # Case C: Reserved target goes DOWN before retry (target is None in reserve_repair)
    # -> abandons node-2 allocation cleanly and allocates node-3.
    p3 = chunk_of(s3)
    repo.enqueue_repair(s3["file_id"], p3["version_id"], p3["chunk_id"])
    job3 = repo.claim_jobs(uuid4(), 1, kinds=("REPAIR",), target=p3["chunk_id"])[0]
    lease3_old = JobLease(job3["job_id"], job3["lease_owner"], job3["fencing_token"])
    dest_old = repo.reserve_repair(p3["chunk_id"], p3["version_id"], lease3_old)
    assert dest_old["node_id"] == "node-2"
    expire_jobs(repo)
    sql(repo, "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-2'")
    w3 = Worker(repo)
    try:
        res3 = run_single(w3, "REPAIR", target=p3["chunk_id"])
        assert res3.node_id == "node-3"
        due(repo)
        assert run_single(w3, "CLEANUP", target=dest_old["allocation_id"]) is True
    finally:
        w3.close()

    assert (
        sql(repo, "SELECT charged_bytes FROM node_budgets WHERE node_id='node-2'")[0][
            "charged_bytes"
        ]
        == 0
    )
    assert (
        sql(repo, "SELECT charged_bytes FROM node_budgets WHERE node_id='node-3'")[0][
            "charged_bytes"
        ]
        == 9
    )
    assert read_verified_bytes(repo, s2["file_id"]) == b"def"
    accounting(repo, sessions=0, claims=0, charged=27)


def test_source_fallback_multiblock_stream_budget_and_zero_sources_to_dead(f2_cluster):
    repo, cluster = f2_cluster
    chunk_len = 262144 + 777
    repo.policy = replace(repo.policy, chunk_size_bytes=512 * 1024, max_parallel_parts=1)
    cluster.add_node("node-0", domain="domain-0", budget=2_000_000, chunk_limit=512 * 1024)
    n1 = cluster.add_node("node-1", domain="domain-1", budget=2_000_000, chunk_limit=512 * 1024)
    data = (b"multi-block-payload-" * ((chunk_len // 20) + 1))[:chunk_len]
    session = make_available(repo, data)
    chunk_id = sql(repo, "SELECT chunk_id FROM manifest_parts")[0]["chunk_id"]
    version_id = session["version_id"]

    # First repair adds verified multi-block replica on node-2 so node-1 & node-2 both have data.
    n2 = cluster.add_node("node-2", domain="domain-2", budget=2_000_000, chunk_limit=512 * 1024)
    sql(repo, "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-0'")
    repo.enqueue_repair(session["file_id"], version_id, chunk_id)
    w_init = Worker(repo)
    try:
        assert run_single(w_init, "REPAIR", target=chunk_id).node_id == "node-2"
    finally:
        w_init.close()

    # Now add node-3, boost session policy to RF=3/domains=3 so node-3 becomes target.
    sql(repo, "UPDATE upload_sessions SET replication_factor=3, min_distinct_domains=3")
    n3 = cluster.add_node("node-3", domain="domain-3", budget=2_000_000, chunk_limit=512 * 1024)

    # Proxy node-1's port to emit a truncated multi-block GetChunk stream (header + both blocks,
    # NO footer -> triggers store_opened=True on StoreChunk) to test hold-back & node-2 fallback!
    orig_n1_port = n1.port

    class TruncatedMultiBlock(rpc.StorageServiceServicer):
        def GetChunk(self, request, context):
            with grpc.insecure_channel(f"127.0.0.1:{orig_n1_port}") as ch:
                for frame in rpc.StorageServiceStub(ch).GetChunk(request, timeout=5):
                    if frame.WhichOneof("payload") == "footer":
                        context.abort(grpc.StatusCode.DATA_LOSS, "truncated before footer")
                    yield frame

    proxy_exec = ThreadPoolExecutor(max_workers=2)
    proxy_server = grpc.server(proxy_exec)
    rpc.add_StorageServiceServicer_to_server(TruncatedMultiBlock(), proxy_server)
    proxy_port = proxy_server.add_insecure_port("127.0.0.1:0")
    proxy_server.start()
    sql(repo, "UPDATE storage_nodes SET port=:port WHERE node_id='node-1'", port=proxy_port)

    try:
        job_id = repo.enqueue_repair(session["file_id"], version_id, chunk_id)
        # 2 * chunk_len prepaid via ReadBudget (~2s at 262921 B/s).
        w_multi = Worker(repo, config=WorkerConfig(read_bytes_per_second=chunk_len))
        started = time.monotonic()
        try:
            proof = run_single(w_multi, "REPAIR", target=chunk_id)
        finally:
            w_multi.close()
        elapsed = time.monotonic() - started
        assert proof.node_id == "node-3"
        assert proof.actual_size == chunk_len
        assert elapsed >= 1.8
        assert (n3.root / "chunks" / str(proof.allocation_id)).read_bytes() == data
    finally:
        sql(repo, "UPDATE storage_nodes SET port=:port WHERE node_id='node-1'", port=orig_n1_port)
        proxy_server.stop(0).wait()
        proxy_exec.shutdown(wait=True)

    # Corrupt all live sources physically -> retries up to max_attempts=2 -> DEAD (file stays
    # AVAILABLE!), _scan_repairs / enqueue_repair does not create duplicate DEAD jobs, and
    # reactivating a healthy node wakes the DEAD job back to RETRY!
    sql(repo, "UPDATE upload_sessions SET replication_factor=4, min_distinct_domains=4")
    n4 = cluster.add_node("node-4", domain="domain-4", budget=2_000_000, chunk_limit=512 * 1024)
    for handle in (n1, n2, n3):
        for chunk_path in (handle.root / "chunks").iterdir():
            raw = bytearray(chunk_path.read_bytes())
            raw[-1] ^= 0xFF
            chunk_path.write_bytes(bytes(raw))

    dead_job_id = repo.enqueue_repair(session["file_id"], version_id, chunk_id)
    assert dead_job_id != job_id
    w_dead = Worker(repo, config=WorkerConfig(max_attempts=2))
    try:
        assert run_single(w_dead, "REPAIR", target=chunk_id) is None
        assert job_state(repo, "REPAIR") == "RETRY"
        due(repo)
        assert run_single(w_dead, "REPAIR", target=chunk_id) is None
    finally:
        w_dead.close()

    assert sql(repo, "SELECT state,last_error FROM jobs WHERE job_id=:id", id=dead_job_id)[0] == {
        "state": "DEAD",
        "last_error": "REPAIR_SOURCE_UNAVAILABLE",
    }
    assert (
        sql(repo, "SELECT state FROM files WHERE file_id=:id", id=session["file_id"])[0]["state"]
        == "AVAILABLE"
    )
    # Dead job deduplication prevents repeated _scan_repairs from spawning backlog jobs.
    assert repo.enqueue_repair(session["file_id"], version_id, chunk_id) is None
    assert sql(repo, "SELECT count(*) AS n FROM jobs WHERE kind='REPAIR'")[0]["n"] == 3
    assert (
        sql(repo, "SELECT count(*) AS n FROM jobs WHERE kind='REPAIR' AND state='DEAD'")[0]["n"]
        == 1
    )
    assert [(r["state"], r["generation"]) for r in n4.ledger.inventory(n4.ledger.scope)[0]] == [
        ("PREPARED", 2)
    ]
    assert not list((n4.root / "chunks").iterdir())

    # Healing n1's physical chunk + activating n5 automatically wakes the DEAD job back to RETRY.
    n1_alloc = sql(
        repo,
        "SELECT allocation_id FROM allocations WHERE node_id='node-1' AND state='STORED'",
    )[0]["allocation_id"]
    (n1.root / "chunks" / str(n1_alloc)).write_bytes(data)
    with n1.ledger.transaction() as db:
        db.execute("UPDATE allocations SET state='STORED' WHERE allocation_id=?", (str(n1_alloc),))
    cluster.add_node("node-5", domain="domain-5", budget=2_000_000, chunk_limit=512 * 1024)
    assert (
        sql(repo, "SELECT state FROM jobs WHERE job_id=:id", id=dead_job_id)[0]["state"] == "RETRY"
    )
    due(repo)
    w_heal = Worker(repo)
    try:
        healed = run_single(w_heal, "REPAIR", target=chunk_id)
        assert (
            healed.node_id == "node-4"
            and healed.actual_size == chunk_len
            and healed.generation == 3
        )
    finally:
        w_heal.close()


@pytest.mark.parametrize("when", ["repair_intent", "before_confirm"])
def test_delete_tombstone_during_repair_releases_all_allocations(f2_cluster, when):
    repo, cluster = f2_cluster
    cluster.add_node("node-0", domain="domain-0", budget=500, chunk_limit=3)
    cluster.add_node("node-1", domain="domain-1", budget=500, chunk_limit=3)
    session = make_available(repo, b"abc")
    n2 = cluster.add_node("node-2", domain="domain-2", budget=500, chunk_limit=3)
    sql(repo, "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-0'")
    (deficit,) = repo.repair_scan_page()["deficits"]
    repo.enqueue_repair(deficit["file_id"], deficit["version_id"], deficit["chunk_id"])

    def delete_on_hook(point):
        if point == when:
            repo.delete_file(session["file_id"])

    worker = Worker(repo, hook=delete_on_hook)
    try:
        assert run_single(worker, "REPAIR", target=deficit["chunk_id"]) is None
        assert job_state(repo, "REPAIR") == "DONE"
        sql(repo, "UPDATE storage_nodes SET health_state='ACTIVE' WHERE node_id='node-0'")
        assert run_single(worker, "CLEANUP", target=session["file_id"]) is None
    finally:
        worker.close()

    assert (
        sql(repo, "SELECT state FROM files WHERE file_id=:id", id=session["file_id"])[0]["state"]
        == "DELETED"
    )
    assert len(n2.ledger.inventory(n2.ledger.scope)[0]) == 1
    assert n2.ledger.inventory(n2.ledger.scope)[0][0]["cleanup_complete"] == 1
    accounting(repo, sessions=0, claims=0, charged=0)


@pytest.mark.parametrize(
    "point",
    [
        "repair_intent",
        "repair_abandoned",
        "repair_cleanup_released",
        "repair_copied",
        "before_confirm",
        "repair_committed",
    ],
)
def test_process_crash_and_graceful_cancel_boundaries(f2_cluster, point):
    repo, cluster = f2_cluster
    n0 = cluster.add_node("node-0", domain="domain-0", budget=500, chunk_limit=3)
    cluster.add_node("node-1", domain="domain-1", budget=500, chunk_limit=3)
    session = make_available(repo, b"abc")
    n2 = cluster.add_node("node-2", domain="domain-2", budget=500, chunk_limit=3)
    n3 = cluster.add_node("node-3", domain="domain-3", budget=500, chunk_limit=3)
    n0.stop()
    sql(repo, "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-0'")
    (deficit,) = repo.repair_scan_page()["deficits"]
    repo.enqueue_repair(deficit["file_id"], deficit["version_id"], deficit["chunk_id"])

    proxy_server = proxy_exec = None
    orig_n2_port = n2.port
    if point in ("repair_abandoned", "repair_cleanup_released"):
        # Point node-2 to a local gRPC server forwarding HealthCheck/DeleteAllocation to n2's
        # real server while deterministically failing PrepareAllocation with FAILED_PRECONDITION.
        class FailingPrepare(rpc.StorageServiceServicer):
            def HealthCheck(self, request, context):
                with grpc.insecure_channel(f"127.0.0.1:{orig_n2_port}") as ch:
                    return rpc.StorageServiceStub(ch).HealthCheck(request, timeout=2)

            def PrepareAllocation(self, request, context):
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "simulated destination fault")

            def DeleteAllocation(self, request, context):
                with grpc.insecure_channel(f"127.0.0.1:{orig_n2_port}") as ch:
                    return rpc.StorageServiceStub(ch).DeleteAllocation(request, timeout=2)

        proxy_exec = ThreadPoolExecutor(max_workers=4)
        proxy_server = grpc.server(proxy_exec)
        rpc.add_StorageServiceServicer_to_server(FailingPrepare(), proxy_server)
        proxy_port = proxy_server.add_insecure_port("127.0.0.1:0")
        proxy_server.start()
        sql(repo, "UPDATE storage_nodes SET port=:port WHERE node_id='node-2'", port=proxy_port)

    try:
        crash_worker(repo, point)
        if point == "repair_committed":
            assert job_state(repo, "REPAIR") == "DONE"
        else:
            expire_jobs(repo)
            due(repo)
            recovery = Worker(
                repo,
                config=WorkerConfig(poll_seconds=0.01, repair_scan_seconds=10.0),
            )
            t = Thread(target=recovery.run, daemon=True)
            t.start()
            try:
                until(
                    lambda: (
                        sql(
                            repo,
                            "SELECT count(*) AS n FROM jobs "
                            "WHERE kind IN ('REPAIR','CLEANUP') AND state!='DONE'",
                        )[0]["n"]
                        == 0
                    )
                )
            finally:
                recovery.stopping.set()
                t.join(10)
                assert not t.is_alive()
    finally:
        sql(repo, "UPDATE storage_nodes SET port=:port WHERE node_id='node-2'", port=orig_n2_port)
        if proxy_server is not None:
            proxy_server.stop(0).wait()
            proxy_exec.shutdown(wait=True)

    assert not repo.repair_scan_page()["deficits"]
    if point in ("repair_abandoned", "repair_cleanup_released"):
        assert (
            sql(repo, "SELECT charged_bytes FROM node_budgets WHERE node_id='node-2'")[0][
                "charged_bytes"
            ]
            == 0
        )
        assert len(n3.ledger.inventory(n3.ledger.scope)[0]) == 1
        sql(repo, "UPDATE storage_nodes SET enabled=false WHERE node_id='node-1'")
        assert read_verified_bytes(repo, session["file_id"], expected_nodes={"node-3"}) == b"abc"
    else:
        assert len(n2.ledger.inventory(n2.ledger.scope)[0]) == 1
        sql(repo, "UPDATE storage_nodes SET enabled=false WHERE node_id='node-1'")
        assert read_verified_bytes(repo, session["file_id"], expected_nodes={"node-2"}) == b"abc"
    accounting(repo, sessions=0, claims=0, charged=9)


def test_graceful_worker_close_cancels_copy_and_recovers(f2_cluster):
    repo, cluster = f2_cluster
    n0 = cluster.add_node("node-0", domain="domain-0", budget=500, chunk_limit=3)
    n1 = cluster.add_node("node-1", domain="domain-1", budget=500, chunk_limit=3)
    session = make_available(repo, b"abc")
    n2 = cluster.add_node("node-2", domain="domain-2", budget=500, chunk_limit=3)
    n0.stop()
    sql(repo, "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-0'")
    (deficit,) = repo.repair_scan_page()["deficits"]
    repo.enqueue_repair(deficit["file_id"], deficit["version_id"], deficit["chunk_id"])

    orig_n1_port = n1.port
    entered, drained = Event(), Event()

    class BlockedGet(rpc.StorageServiceServicer):
        def HealthCheck(self, request, context):
            with grpc.insecure_channel(f"127.0.0.1:{orig_n1_port}") as ch:
                return rpc.StorageServiceStub(ch).HealthCheck(request, timeout=2)

        def GetChunk(self, request, context):
            with grpc.insecure_channel(f"127.0.0.1:{orig_n1_port}") as ch:
                call = rpc.StorageServiceStub(ch).GetChunk(request, timeout=10)
                try:
                    yield next(call)
                    entered.set()
                    while context.is_active():
                        time.sleep(0.01)
                finally:
                    call.cancel()
                    drained.set()

    proxy_exec = ThreadPoolExecutor(max_workers=2)
    proxy_server = grpc.server(proxy_exec)
    rpc.add_StorageServiceServicer_to_server(BlockedGet(), proxy_server)
    proxy_port = proxy_server.add_insecure_port("127.0.0.1:0")
    proxy_server.start()
    sql(repo, "UPDATE storage_nodes SET port=:port WHERE node_id='node-1'", port=proxy_port)

    worker = Worker(
        repo, config=WorkerConfig(poll_seconds=0.01, repair_scan_seconds=10.0, shutdown_seconds=2)
    )
    thread = Thread(target=worker.run, daemon=True)
    thread.start()
    try:
        assert entered.wait(5)
        started = time.monotonic()
        worker.stopping.set()
        thread.join(5)
        assert not thread.is_alive()
        assert time.monotonic() - started < 2
        assert drained.wait(3)
        assert job_state(repo, "REPAIR") == "RUNNING"
        accounting(repo, sessions=0, claims=0, charged=9)
    finally:
        sql(repo, "UPDATE storage_nodes SET port=:port WHERE node_id='node-1'", port=orig_n1_port)
        proxy_server.stop(0).wait()
        proxy_exec.shutdown(wait=True)

    expire_jobs(repo)
    replacement = Worker(repo)
    try:
        proof = run_single(replacement, "REPAIR", target=deficit["chunk_id"])
        assert (
            proof.node_id == "node-2"
            and n2.ledger.lookup(str(proof.allocation_id))["state"] == "STORED"
        )
    finally:
        replacement.close()
    assert read_verified_bytes(repo, session["file_id"]) == b"abc"
    accounting(repo, sessions=0, claims=0, charged=9)
