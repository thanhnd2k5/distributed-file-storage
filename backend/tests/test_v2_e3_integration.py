"""E3 fair quanta, paused leases and shared I/O on real PostgreSQL/gRPC/POSIX."""

import hashlib
import time
from collections import Counter
from contextlib import contextmanager
from dataclasses import replace
from threading import Event, Lock, Thread
from uuid import uuid4

import pytest
from test_v2_c1 import claim, upload
from test_v2_c2 import cluster as cluster
from test_v2_c2 import confirmed, expire_jobs, store
from test_v2_e2 import accounting
from test_v2_metadata import foundation as foundation
from test_v2_metadata import sql

from metadata.v2.storage_client import StorageClient
from metadata.v2.worker import KINDS, Worker, WorkerConfig


def until(predicate, seconds=8):
    due = time.monotonic() + seconds
    while not predicate():
        assert time.monotonic() < due, "Controlled E3 boundary did not progress"
        time.sleep(0.01)


class ObservedWorker(Worker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.finished, self.samples = [], []
        self.mutex = Lock()

    def tick(self):
        super().tick()
        with self.mutex:
            kinds = Counter(self.futures.values())
            resident = Counter(kind for kind, _ in self.resident.values())
            assert len(self.futures) <= self.config.slots
            assert kinds["FINALIZE"] <= self.config.finalize_slots
            assert max(kinds.values(), default=0) <= self.config.slots - 1
            assert max(resident.values(), default=0) <= self.config.jobs_per_kind
            self.samples.append((dict(kinds), dict(resident)))

    def steps(self, job, **kwargs):
        result = yield from super().steps(job, **kwargs)
        with self.mutex:
            self.finished.append(job["job_id"])
        return result


@contextmanager
def running(worker, *release_events):
    errors = []

    def loop():
        try:
            worker.run()
        except BaseException as exc:
            errors.append(exc)

    thread = Thread(target=loop, name="e3-scheduler")
    thread.start()
    try:
        yield
        assert not errors, errors
    finally:
        for event in release_events:
            event.set()
        worker.stopping.set()
        thread.join(8)
        assert not thread.is_alive(), "Worker failed to drain"
        assert not errors, errors
        assert not worker.resident and not worker.futures
        assert all(not queue for queue in worker.queues.values())


def seed(repo, kind, data=b"abc"):
    if kind == "RECONCILE":
        session = upload(repo, data)
        part = claim(repo, session, data=data)
        store(part, data)
        repo.uncertain(session["upload_id"], 0, part["claim_token"], part["revision"])
    else:
        session = confirmed(repo, data)
        if kind == "CLEANUP":
            repo.abort(session["upload_id"])
        else:
            repo.complete_upload(session["upload_id"])
    job = sql(
        repo, "SELECT * FROM jobs WHERE kind=:kind ORDER BY created_at DESC LIMIT 1", kind=kind
    )[0]
    return session, job


def done(repo, job):
    return (
        sql(repo, "SELECT state FROM jobs WHERE job_id=:id", id=job["job_id"])[0]["state"] == "DONE"
    )


@pytest.mark.parametrize("flood", KINDS)
def test_each_kind_progresses_while_an_older_flood_holds_its_io_lane(cluster, flood):
    repo, _ = cluster
    backlog = [seed(repo, flood)[1] for _ in range(4)]
    victims = [seed(repo, kind)[1] for kind in KINDS if kind != flood]
    sql(
        repo,
        "UPDATE jobs SET run_after=clock_timestamp()-interval '1 day' WHERE kind=:kind",
        kind=flood,
    )
    kinds = {job["job_id"]: job["kind"] for job in backlog + victims}
    entered, release = Event(), Event()

    class Flood(StorageClient):
        def boundary(self, work):
            if kinds[work.lease.job_id] == flood:
                entered.set()
                while not release.wait(0.01):
                    work.check()

        def unary(self, method, d, work):
            self.boundary(work)
            return super().unary(method, d, work)

        def read(self, d, digest, work):
            self.boundary(work)
            return super().read(d, digest, work)

    worker = ObservedWorker(repo, storage=Flood(), config=WorkerConfig(poll_seconds=0.01))
    with running(worker, release):
        assert entered.wait(5)
        until(lambda: all(done(repo, job) for job in victims))
        # Backlog remains ready throughout the victims' complete, real I/O flows.
        assert all(not done(repo, job) for job in backlog)
        release.set()
        until(lambda: all(done(repo, job) for job in backlog))
    assert worker.samples
    for session in sql(repo, "SELECT state,active_claims FROM upload_sessions"):
        assert session["active_claims"] == 0
    remaining = sql(repo, "SELECT count(*) AS n FROM allocations WHERE state!='RELEASED'")[0]["n"]
    active = sql(repo, "SELECT count(*) AS n FROM upload_sessions WHERE state='OPEN'")[0]["n"]
    accounting(repo, sessions=active, claims=0, charged=3 * remaining)


@pytest.mark.parametrize("kind", ["CLEANUP", "FINALIZE"])
def test_large_job_yields_to_another_file_at_the_io_boundary(cluster, kind):
    repo, _ = cluster
    large, large_job = seed(repo, kind, b"abc" * 12)
    small, small_job = seed(repo, kind, b"abc")
    sql(
        repo,
        "UPDATE jobs SET run_after=clock_timestamp()-interval '1 day' WHERE job_id=:id",
        id=large_job["job_id"],
    )
    entered, release = Event(), Event()
    order = []

    class Boundary(StorageClient):
        def record(self, work):
            if not order and work.lease.job_id == large_job["job_id"]:
                entered.set()
                while not release.wait(0.01):
                    work.check()
            order.append(work.lease.job_id)

        def unary(self, method, d, work):
            self.record(work)
            return super().unary(method, d, work)

        def read(self, d, digest, work):
            self.record(work)
            return super().read(d, digest, work)

    worker = ObservedWorker(
        repo, storage=Boundary(), config=WorkerConfig(poll_seconds=0.01, renew_seconds=0.05)
    )
    with running(worker, release):
        assert entered.wait(5)
        until(lambda: len(worker.resident) == 2)
        # Both RUNNING jobs renew even though one continuation owns no thread.
        sql(repo, "UPDATE jobs SET lease_expires_at=clock_timestamp()+interval '2 seconds'")
        until(
            lambda: all(
                row["live"]
                for row in sql(
                    repo,
                    "SELECT lease_expires_at>clock_timestamp()+interval '30 seconds' AS live "
                    "FROM jobs WHERE state='RUNNING'",
                )
            )
        )
        release.set()
        until(lambda: done(repo, large_job) and done(repo, small_job))
    assert order[0] == large_job["job_id"]
    assert order.index(small_job["job_id"]) <= 2
    assert worker.finished.index(small_job["job_id"]) < worker.finished.index(large_job["job_id"])
    assert all(row["attempts"] == 1 for row in sql(repo, "SELECT attempts FROM jobs"))
    if kind == "FINALIZE":
        assert (
            sql(repo, "SELECT actual_sha256 FROM files WHERE file_id=:id", id=large["file_id"])[0][
                "actual_sha256"
            ]
            == hashlib.sha256(b"abc" * 12).hexdigest()
        )
        assert repo.get_session(small["upload_id"])["state"] == "COMPLETED"
    accounting(repo, sessions=0, claims=0, charged=78 if kind == "FINALIZE" else 0)


def test_concurrent_finalizers_share_one_worker_read_rate(cluster):
    repo, resources = cluster
    size = 32768
    repo.policy = replace(repo.policy, chunk_size_bytes=size)
    for ledger, _, _ in resources:
        ledger.chunk_limit = size
    # Increase only the isolated fixture's limits, never shared storage data.
    sql(repo, "UPDATE node_budgets SET allocatable_bytes=1048576")
    data = b"a" * size + b"b" * size
    sessions = []
    for _ in range(2):
        session = upload(repo, data)
        for index in range(2):
            block = data[index * size : (index + 1) * size]
            part = claim(repo, session, index=index, data=block)
            repo.confirm_part(
                session["upload_id"],
                index,
                part["claim_token"],
                part["revision"],
                store(part, block),
            )
        repo.complete_upload(session["upload_id"])
        sessions.append(session)
    worker = ObservedWorker(
        repo,
        config=WorkerConfig(
            slots=3, finalize_slots=2, read_bytes_per_second=65536, poll_seconds=0.005
        ),
    )
    started = time.monotonic()
    with running(worker):
        until(
            lambda: all(repo.get_session(s["upload_id"])["state"] == "COMPLETED" for s in sessions)
        )
    elapsed = time.monotonic() - started
    assert elapsed >= 2.0  # 128 KiB total, one 64 KiB/s budget, including both jobs.
    assert any(kinds.get("FINALIZE") == 2 for kinds, _ in worker.samples)
    assert {row["actual_sha256"] for row in sql(repo, "SELECT actual_sha256 FROM files")} == {
        hashlib.sha256(data).hexdigest()
    }
    accounting(repo, sessions=0, claims=0, charged=4 * len(data))


def test_reclaimed_paused_continuation_cannot_publish_with_old_token(cluster):
    repo, _ = cluster
    _, first = seed(repo, "FINALIZE", b"abc" * 3)
    second_session, second = seed(repo, "FINALIZE")
    sql(
        repo,
        "UPDATE jobs SET run_after=clock_timestamp()-interval '1 day' WHERE job_id=:id",
        id=first["job_id"],
    )
    entered, release = Event(), Event()

    class Blocked(StorageClient):
        def read(self, d, digest, work):
            if work.lease.job_id == first["job_id"]:
                entered.set()
                while not release.wait(0.01):
                    work.check()
            return super().read(d, digest, work)

    worker = ObservedWorker(
        repo, storage=Blocked(), config=WorkerConfig(poll_seconds=0.01, renew_seconds=10)
    )
    with running(worker, release):
        assert entered.wait(5)
        until(lambda: len(worker.resident) == 2)
        sql(
            repo,
            "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' "
            "WHERE job_id=:id",
            id=second["job_id"],
        )
        current = repo.claim_jobs(
            uuid4(), 1, kinds=("FINALIZE",), target=second_session["file_id"]
        )[0]
        assert current["fencing_token"] == 2
        release.set()
        until(lambda: second["job_id"] in worker.finished)
        assert repo.get_session(second_session["upload_id"])["state"] == "FINALIZING"
        assert (
            sql(repo, "SELECT lease_owner FROM jobs WHERE job_id=:id", id=second["job_id"])[0][
                "lease_owner"
            ]
            == current["lease_owner"]
        )
    replacement = Worker(repo)
    replacement.owner = current["lease_owner"]
    try:
        assert replacement.execute(current)["state"] == "COMPLETED"
    finally:
        replacement.close()
    assert repo.get_session(second_session["upload_id"])["state"] == "COMPLETED"


@pytest.mark.parametrize("gate", ["paused", "read_budget"])
def test_paused_job_keeps_original_deadline_and_heartbeat_is_drained(cluster, monkeypatch, gate):
    repo, _ = cluster
    session, _ = seed(repo, "FINALIZE")
    opened = []
    original = StorageClient.channel

    def channel(d):
        opened.append(d["allocation_id"])
        return original(d)

    monkeypatch.setattr(StorageClient, "channel", staticmethod(channel))
    worker = Worker(
        repo,
        config=WorkerConfig(
            job_seconds=0.1,
            renew_seconds=0.02,
            max_attempts=1,
            read_bytes_per_second=1 if gate == "read_budget" else 65536,
        ),
    )
    job = repo.claim_jobs(worker.owner, 1, kinds=("FINALIZE",))[0]
    steps = worker.steps(job)
    assert worker.advance(steps) == (False, None)  # Primed, no RPC yet.
    if gate == "paused":
        time.sleep(0.15)
    try:
        assert worker.advance(steps) == (True, None)
        assert not opened  # Expired/read-budget-starved Work never opens GetChunk.
        assert repo.get_session(session["upload_id"])["state"] == "FAILED"
        assert (
            sql(repo, "SELECT state FROM jobs WHERE job_id=:id", id=job["job_id"])[0]["state"]
            == "DEAD"
        )
        accounting(repo, sessions=0, claims=0, charged=6)
    finally:
        steps.close()
        worker.close()


def test_resident_exclusion_does_not_reclaim_a_local_expired_iterator(cluster):
    repo, _ = cluster
    seed(repo, "FINALIZE")
    first = repo.claim_jobs(uuid4(), 1, kinds=("FINALIZE",))[0]
    expire_jobs(repo)
    assert repo.claim_jobs(uuid4(), 1, kinds=("FINALIZE",), exclude=[first["job_id"]]) == []
    second = repo.claim_jobs(uuid4(), 1, kinds=("FINALIZE",))[0]
    assert second["fencing_token"] == first["fencing_token"] + 1
    with pytest.raises(ValueError, match="24 resident"):
        repo.claim_jobs(uuid4(), exclude=[uuid4() for _ in range(25)])


def test_dedicated_retry_consumer_drains_on_close(cluster):
    repo, _ = cluster
    session = upload(repo)
    part = claim(repo, session)
    repo.uncertain(session["upload_id"], 0, part["claim_token"], part["revision"])
    entered = Event()

    class Blocked(StorageClient):
        def unary(self, method, d, work):
            entered.set()
            while True:
                work.check()
                time.sleep(0.01)

    worker = Worker(repo, storage=Blocked(), config=WorkerConfig(shutdown_seconds=2))
    results = []
    thread = Thread(
        target=lambda: results.append(worker.retry_for_consumer(part["chunk_id"], uuid4()))
    )
    thread.start()
    try:
        assert entered.wait(5)
        started = time.monotonic()
        worker.close()
        thread.join(1)
        assert not thread.is_alive() and results == [None]
        assert time.monotonic() - started < 2
        assert worker.consumer_done.is_set()
        accounting(repo, sessions=1, claims=0, charged=6)
        assert sql(repo, "SELECT state FROM jobs")[0]["state"] == "RUNNING"
    finally:
        worker.stopping.set()
        thread.join(5)
        worker.close()
