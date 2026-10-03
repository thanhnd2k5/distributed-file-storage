from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone

import grpc
import pytest
import storage_pb2

from metadata.config import NodeConfig
from metadata.health import HealthDetector, HealthSnapshot

UTC_START = datetime(2026, 10, 3, 4, 0, tzinfo=UTC)


class Clock:
    def __init__(self):
        self.monotonic = 0.0
        self.utc = UTC_START

    def monotonic_now(self):
        return self.monotonic

    def utc_now(self):
        return self.utc


@pytest.fixture
def detector():
    clock = Clock()
    node = NodeConfig(node_id="node-1", host="storage", port=50051, failure_domain="dev_host")
    return (
        HealthDetector(node, 10, monotonic_clock=clock.monotonic_now, utc_clock=clock.utc_now),
        clock,
    )


def response(**overrides):
    return storage_pb2.HealthCheckResponse(
        **(
            dict(
                node_id="node-1",
                failure_domain="dev_host",
                storage_writable=True,
                capacity_bytes=1000,
                available_bytes=900,
                used_bytes=10,
            )
            | overrides
        )
    )


def history(snapshot):
    return (
        snapshot.last_success_at,
        snapshot.capacity_bytes,
        snapshot.available_bytes,
        snapshot.used_bytes,
        snapshot.last_success_monotonic,
    )


def test_initial_state_stays_down_without_success(detector):
    health, clock = detector
    initial = HealthSnapshot()
    assert initial.status == "DOWN" and not initial.hard_down
    assert history(initial) == (None, None, None, None, None)
    for age in (0, 3, 10, 100):
        clock.monotonic = age
        failed = health.failure(initial, grpc.StatusCode.UNAVAILABLE)
        assert failed.status == "DOWN" and failed.last_error == "UNAVAILABLE"
        assert history(failed) == history(initial)


def test_valid_success_updates_metrics_and_both_clocks_without_mutating_input(detector):
    health, clock = detector
    initial = HealthSnapshot()
    active = health.success(initial, response())
    assert active.status == "ACTIVE" and active.last_error is None and not active.hard_down
    assert history(active) == (UTC_START, 1000, 900, 10, 0)
    clock.monotonic = 3
    clock.utc += timedelta(seconds=3)
    updated = health.success(active, response(available_bytes=800, used_bytes=20))
    assert history(updated) == (clock.utc, 1000, 800, 20, 3)
    assert history(active) == (UTC_START, 1000, 900, 10, 0)
    assert initial == HealthSnapshot()
    with pytest.raises(FrozenInstanceError):
        updated.status = "DOWN"


@pytest.mark.parametrize("code", [grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE])
@pytest.mark.parametrize(
    "age, expected", [(3, "SUSPECTED"), (9.999, "SUSPECTED"), (10, "DOWN"), (10.001, "DOWN")]
)
def test_failure_threshold_uses_last_success_including_monotonic_zero(
    detector, age, expected, code
):
    health, clock = detector
    active = health.success(HealthSnapshot(), response())
    clock.monotonic = age
    failed = health.failure(active, code)
    assert failed.status == expected and failed.last_error == code.name
    assert history(failed) == history(active)


def test_repeated_failures_do_not_reset_age_and_success_restarts_the_window(detector):
    health, clock = detector
    snapshot = health.success(HealthSnapshot(), response())
    for age, expected in ((3, "SUSPECTED"), (6, "SUSPECTED"), (10, "DOWN"), (13, "DOWN")):
        clock.monotonic = age
        snapshot = health.failure(snapshot, grpc.StatusCode.UNAVAILABLE)
        assert snapshot.status == expected and snapshot.last_success_monotonic == 0
    clock.utc += timedelta(seconds=13)
    snapshot = health.success(snapshot, response())
    assert snapshot.status == "ACTIVE" and snapshot.last_error is None
    assert snapshot.last_success_monotonic == 13
    clock.monotonic = 16
    assert health.failure(snapshot, grpc.StatusCode.UNAVAILABLE).status == "SUSPECTED"
    clock.monotonic = 23
    assert health.failure(snapshot, grpc.StatusCode.UNAVAILABLE).status == "DOWN"


@pytest.mark.parametrize("previous_success", [False, True])
@pytest.mark.parametrize(
    "overrides, reason",
    [
        ({"node_id": "wrong-node"}, "IDENTITY_MISMATCH"),
        ({"failure_domain": "wrong-host"}, "IDENTITY_MISMATCH"),
        ({"node_id": "wrong-node", "storage_writable": False}, "IDENTITY_MISMATCH"),
        ({"storage_writable": False}, "STORAGE_NOT_WRITABLE"),
    ],
)
def test_blocking_response_preserves_history_and_remains_down_until_success(
    detector, previous_success, overrides, reason
):
    health, clock = detector
    previous = HealthSnapshot()
    if previous_success:
        previous = health.success(previous, response())
    clock.monotonic = 1
    clock.utc += timedelta(seconds=1)
    blocked = health.success(previous, response(capacity_bytes=9999, **overrides))
    assert blocked.status == "DOWN" and blocked.last_error == reason and blocked.hard_down
    assert history(blocked) == history(previous)
    clock.monotonic = 2
    failed = health.failure(blocked, grpc.StatusCode.DEADLINE_EXCEEDED)
    assert failed.status == "DOWN" and failed.last_error == reason and failed.hard_down
    assert history(failed) == history(previous)
    recovered = health.success(failed, response(used_bytes=30))
    assert recovered.status == "ACTIVE" and recovered.last_error is None and not recovered.hard_down
    assert history(recovered) == (clock.utc, 1000, 900, 30, 2)


@pytest.mark.parametrize("observation", ["success", "failure"])
def test_disabled_ignores_observations_and_reenable_needs_new_success(detector, observation):
    health, clock = detector
    active = health.success(HealthSnapshot(), response())
    clock.monotonic = 1
    if observation == "success":
        disabled = health.success(active, response(used_bytes=99), enabled=False)
    else:
        disabled = health.failure(active, grpc.StatusCode.UNAVAILABLE, enabled=False)
    assert disabled.status == "DOWN" and disabled.last_success_monotonic is None
    assert history(disabled)[:4] == history(active)[:4]
    assert health.failure(disabled, grpc.StatusCode.UNAVAILABLE).status == "DOWN"
    recovered = health.success(disabled, response())
    assert recovered.status == "ACTIVE" and recovered.last_success_monotonic == 1


@pytest.mark.parametrize("wall_jump", [timedelta(days=365), timedelta(days=-365)])
def test_wall_clock_jumps_do_not_change_age_or_failure_state(detector, wall_jump):
    health, clock = detector
    active = health.success(HealthSnapshot(), response())
    clock.utc += wall_jump
    clock.monotonic = 3
    failed = health.failure(active, grpc.StatusCode.UNAVAILABLE)
    assert failed.status == "SUSPECTED" and failed.last_success_at == UTC_START
    clock.monotonic = 10
    assert health.failure(failed, grpc.StatusCode.UNAVAILABLE).status == "DOWN"
    recovered = health.success(failed, response())
    assert recovered.status == "ACTIVE" and recovered.last_success_at == clock.utc


def test_restarted_process_uses_history_only_and_waits_for_new_success(detector):
    health, clock = detector
    old_process = health.success(HealthSnapshot(), response())
    restarted = HealthSnapshot(
        last_success_at=old_process.last_success_at,
        capacity_bytes=old_process.capacity_bytes,
        available_bytes=old_process.available_bytes,
        used_bytes=old_process.used_bytes,
    )
    # Persisted UTC history is recent, but no success belongs to this process.
    clock.monotonic = 1
    failed = health.failure(restarted, grpc.StatusCode.UNAVAILABLE)
    assert failed.status == "DOWN" and failed.last_success_monotonic is None
    assert history(failed)[:4] == history(old_process)[:4]
    assert health.success(failed, response()).status == "ACTIVE"


def test_custom_down_after_uses_configuration_value(detector):
    health, clock = detector
    health.down_after_seconds = 7
    active = health.success(HealthSnapshot(), response())
    clock.monotonic = 6.999
    assert health.failure(active, grpc.StatusCode.UNAVAILABLE).status == "SUSPECTED"
    clock.monotonic = 7
    assert health.failure(active, grpc.StatusCode.UNAVAILABLE).status == "DOWN"


def test_valid_zero_metrics_are_not_unknown_nulls(detector):
    health, _ = detector
    active = health.success(
        HealthSnapshot(), response(capacity_bytes=0, available_bytes=0, used_bytes=0)
    )
    assert active.status == "ACTIVE"
    assert (active.capacity_bytes, active.available_bytes, active.used_bytes) == (0, 0, 0)


def test_success_normalizes_display_timestamp_to_utc(detector):
    health, clock = detector
    clock.utc = UTC_START.astimezone(timezone(timedelta(hours=7)))
    active = health.success(HealthSnapshot(), response())
    assert active.last_success_at == UTC_START and active.last_success_at.tzinfo == UTC


def test_naive_display_clock_is_rejected_without_changing_snapshot(detector):
    health, clock = detector
    previous = HealthSnapshot()
    clock.utc = UTC_START.replace(tzinfo=None)
    with pytest.raises(ValueError, match="timezone-aware"):
        health.success(previous, response())
    assert previous == HealthSnapshot()


@pytest.mark.parametrize("threshold", [0, -1, float("inf"), float("nan")])
def test_invalid_threshold_is_rejected(threshold):
    node = NodeConfig(node_id="node-1", host="storage", port=50051, failure_domain="dev_host")
    with pytest.raises(ValueError, match="finite and positive"):
        HealthDetector(node, threshold)
