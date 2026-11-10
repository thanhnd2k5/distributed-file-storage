"""Pure E3 read-accounting/configuration checks; no service dependencies."""

from concurrent.futures import Future, ThreadPoolExecutor
from threading import Event
from uuid import uuid4

import pytest

from metadata.v2.repository import DomainError, JobLease
from metadata.v2.worker import ReadBudget, Work, Worker, WorkerConfig


@pytest.mark.parametrize("value", [0, -1, 7, 9, True, 1.5])
def test_resident_jobs_have_a_finite_per_kind_cap(value):
    with pytest.raises(ValueError):
        WorkerConfig(jobs_per_kind=value)


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), True])
def test_repair_scan_interval_must_be_finite_and_positive(value):
    with pytest.raises(ValueError):
        WorkerConfig(repair_scan_seconds=value)


@pytest.mark.parametrize("size", [0, -1, 262145, True, 1.5])
def test_read_reservations_require_bounded_wire_blocks(size):
    with pytest.raises(ValueError):
        ReadBudget(1024).reserve(size)


def test_read_budget_serializes_threads_and_does_not_bank_idle_credit(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("metadata.v2.worker.time.monotonic", lambda: now[0])
    budget = ReadBudget(1024)
    with ThreadPoolExecutor(8) as pool:
        deadlines = list(pool.map(lambda _: budget.reserve(256), range(8)))
    assert sorted(deadlines) == [100 + (index + 1) / 4 for index in range(8)]
    now[0] = 200
    assert budget.reserve(512) == 200.5


def test_work_shares_debt_even_after_cancellation(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("metadata.v2.worker.time.monotonic", lambda: now[0])
    budget = ReadBudget(1024)
    config = WorkerConfig()
    lease = JobLease(uuid4(), uuid4(), 1)
    first = Work(None, lease, config, Event(), read_budget=budget)
    second = Work(None, lease, config, Event(), read_budget=budget)

    class CancelOnWait:
        def wait(self, timeout):
            first.cancelled.set()

    # Cancel while acquiring read credit. Reserved bytes remain charged;
    # another job must wait behind that same debt instead of bypassing the cap.
    monkeypatch.setattr(first.cancelled, "wait", CancelOnWait().wait)
    with pytest.raises(DomainError, match="WORK_CANCELLED"):
        first.consume(512)
    assert second.read_budget.reserve(512) == 101


@pytest.mark.parametrize("stop", ["cancel", "shutdown", "deadline"])
def test_pacing_stops_before_another_read_reservation(monkeypatch, stop):
    now = [100.0]
    monkeypatch.setattr("metadata.v2.worker.time.monotonic", lambda: now[0])
    budget, stopping = ReadBudget(1), Event()
    work = Work(None, JobLease(uuid4(), uuid4(), 1), WorkerConfig(), stopping, read_budget=budget)
    if stop == "cancel":
        work.cancel()
    elif stop == "shutdown":
        stopping.set()
    else:
        now[0] += 3601
    with pytest.raises(DomainError):
        work.consume(1)
    assert budget.due == 0


def test_close_drains_a_cancelled_unnotified_quantum_without_waiting_for_timeout():
    worker = Worker(None, config=WorkerConfig(shutdown_seconds=1))
    closed, job_id = Event(), uuid4()

    def paused():
        try:
            yield
        finally:
            closed.set()

    steps = paused()
    next(steps)
    future = Future()
    assert future.cancel() and future.done()
    worker.futures[future] = "FINALIZE"
    worker.executing[future] = job_id
    worker.resident[job_id] = "FINALIZE", steps
    worker.close()
    assert closed.is_set()
    assert not worker.resident and not worker.futures
