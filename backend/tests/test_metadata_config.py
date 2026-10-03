from threading import TIMEOUT_MAX, Event

import pytest
from pydantic import ValidationError

from metadata.config import MetadataSettings
from metadata.worker import MetadataWorker


def settings(**overrides):
    return MetadataSettings(
        _env_file=None,
        database_url="postgresql+psycopg://test:test@127.0.0.1:1/test",
        storage_nodes_json=[
            {"node_id": "node-1", "host": "127.0.0.1", "port": 1, "failure_domain": "test"}
        ],
        replication_factor=1,
        **overrides,
    )


@pytest.mark.parametrize(
    "field",
    [
        "health_interval_seconds",
        "health_rpc_timeout_seconds",
        "node_down_after_seconds",
        "chunk_rpc_timeout_seconds",
        "cleanup_interval_seconds",
        "repair_time_budget_seconds",
    ],
)
@pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan"), 0, -1])
def test_duration_rejects_nonfinite_and_nonpositive_values(field, value):
    with pytest.raises(ValidationError) as failure:
        settings(**{field: value})
    assert any(error["loc"] == (field,) for error in failure.value.errors())


def test_infinite_interval_from_environment_is_rejected(monkeypatch):
    monkeypatch.setenv("HEALTH_INTERVAL_SECONDS", "inf")
    with pytest.raises(ValidationError, match="finite"):
        settings()


def test_defaults_and_custom_finite_health_timings():
    default = settings()
    assert (
        default.health_interval_seconds,
        default.health_rpc_timeout_seconds,
        default.node_down_after_seconds,
    ) == (3, 1, 10)
    custom = settings(
        health_interval_seconds=0.05, health_rpc_timeout_seconds=0.2, node_down_after_seconds=0.6
    )
    assert custom.node_down_after_seconds == 0.6
    with pytest.raises(ValidationError, match="must exceed"):
        settings(health_rpc_timeout_seconds=10)


def test_large_finite_interval_does_not_overflow_platform_wait(monkeypatch):
    worker = MetadataWorker(None, settings(health_interval_seconds=TIMEOUT_MAX * 2))
    waiting = Event()
    timeouts = []
    original_wait = worker._wake.wait

    def observed_wait(timeout):
        timeouts.append(timeout)
        waiting.set()
        return original_wait(timeout)

    # Exercise the real scheduler/OS wait, without making DB or RPC calls.
    monkeypatch.setattr(worker, "_poll", lambda config: None)
    monkeypatch.setattr(worker._wake, "wait", observed_wait)
    try:
        assert not worker.running
        worker.start()
        assert waiting.wait(2) and worker.running
        assert timeouts and all(0 <= timeout <= TIMEOUT_MAX for timeout in timeouts)
    finally:
        worker.stop()
    assert not worker.running
