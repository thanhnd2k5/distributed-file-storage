"""Admission policy: two-sided progress, bounded total and real concurrency."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from metadata.v2.admission import AdmissionBudget, progress_request
from metadata.v2.runtime import Limits

ID = "c045cf32-31d0-4660-85dc-d756f1d2f852"


@pytest.mark.parametrize(
    "method,path",
    [
        ("POST", f"/api/v2/uploads/{ID}/complete"),
        ("POST", f"/api/v2/uploads/{ID}/abort"),
        ("DELETE", f"/api/v2/files/{ID}"),
        ("POST", f"/internal/v2/uploads/{ID}/parts/0/renew"),
        ("POST", f"/internal/v2/uploads/{ID}/parts/25/confirm"),
        ("POST", f"/internal/v2/uploads/{ID}/parts/999/uncertain"),
        ("POST", f"/internal/v2/downloads/{ID}/renew"),
        ("POST", f"/internal/v2/downloads/{ID}/release"),
        ("GET", f"/internal/v2/downloads/{ID}/manifest?after_index=-1&limit=1"),
    ],
)
def test_classify_existing_progress(method, path):
    assert progress_request(method, path)


@pytest.mark.parametrize(
    "method,path",
    [
        ("POST", "/api/v2/uploads"),
        ("POST", "/internal/v2/downloads"),
        ("POST", f"/internal/v2/uploads/{ID}/parts/0/claim"),
        ("PUT", f"/api/v2/uploads/{ID}/parts/0"),
        ("GET", f"/api/v2/files/{ID}/download"),
        ("HEAD", f"/api/v2/files/{ID}/download"),
        ("GET", f"/api/v2/uploads/{ID}"),
        ("GET", "/api/v2/files"),
        ("GET", "/api/v2/config?progress=true"),
        ("GET", f"/internal/v2/downloads/{ID}/renew"),
        ("POST", f"/internal/v2/downloads/{ID.upper()}/renew"),
        ("POST", "/internal/v2/downloads/not-an-id/renew"),
        ("POST", f"/internal/v2/uploads/{ID}/parts/-1/renew"),
        ("POST", f"/internal/v2/uploads/{ID}/parts/01/renew"),
        ("POST", f"/internal/v2/downloads/{ID}/renew/extra"),
        ("POST", f"/api/v2/uploads/{ID}/abort/"),
        ("DELETE", f"/api/v2/files/{ID}/other"),
        ("HEAD", f"/internal/v2/downloads/{ID}/manifest"),
    ],
)
def test_classify_new_work_and_lookalikes(method, path):
    assert not progress_request(method, path)


@pytest.mark.parametrize("flood", [False, True])
def test_budget_keeps_both_classes_alive_under_flood(flood):
    budget = AdmissionBudget(8, 2)
    assert all(budget.acquire(flood) for _ in range(6))
    assert not budget.acquire(flood) and budget.active == 6
    assert all(budget.acquire(not flood) for _ in range(2))
    assert budget.active == 8
    assert not budget.acquire(flood) and not budget.acquire(not flood)
    for _ in range(6):
        budget.release(flood)
    assert all(budget.acquire(not flood) for _ in range(4))
    assert not budget.acquire(not flood)  # Now the other group owns shared capacity.
    assert all(budget.acquire(flood) for _ in range(2))
    for _ in range(6):
        budget.release(not flood)
    for _ in range(2):
        budget.release(flood)
    assert budget.active == 0
    with pytest.raises(RuntimeError):
        budget.release(flood)
    assert budget.active == 0


def test_budget_caps_simultaneous_threads_and_reclaims_all_grants():
    budget = AdmissionBudget(8, 2)
    start, attempted = Barrier(16), Barrier(16)

    def race(index):
        progress = index % 2 == 0
        start.wait(3)
        granted = budget.acquire(progress)
        attempted.wait(3)
        if granted:
            budget.release(progress)
        return progress, granted

    with ThreadPoolExecutor(16) as pool:
        results = list(pool.map(race, range(16)))
    assert sum(granted for _, granted in results) == 8
    for group in (False, True):
        assert 2 <= sum(granted for progress, granted in results if progress == group) <= 6
    assert budget.active == 0


@pytest.mark.parametrize(
    "limit,reserve", [(0, 0), (-1, 0), (True, 0), (4.0, 1), (4, -1), (4, 3), (4, True), (4, 0.5)]
)
def test_budget_rejects_invalid_configuration(limit, reserve):
    with pytest.raises(ValueError):
        AdmissionBudget(limit, reserve)


def test_budget_small_cap_and_disabled_reservation():
    for limit in (1, 2, 3):
        budget = AdmissionBudget(limit)
        assert budget.reserve == 0
        assert all(budget.acquire() for _ in range(limit))
        assert not budget.acquire(True)
        for _ in range(limit):
            budget.release()
    budget = AdmissionBudget(4, 0)
    assert all(budget.acquire(True) for _ in range(4))
    assert not budget.acquire()


def test_limits_configures_separate_http_and_control_reservations(monkeypatch):
    monkeypatch.setenv("V2_REQUEST_RESERVE", "4")
    monkeypatch.setenv("V2_CONTROL_RESERVE", "6")
    limits = Limits.from_env()
    assert (limits.request_reserve, limits.control_reserve) == (4, 6)
    for values in (
        dict(request_reserve=17),
        dict(control_reserve=17),
        dict(request_reserve=-1),
        dict(control_reserve=0.5),
    ):
        with pytest.raises(ValueError):
            Limits(**values)
