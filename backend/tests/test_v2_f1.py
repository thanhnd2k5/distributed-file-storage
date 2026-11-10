from threading import Event, Thread

from metadata.v2.health import HealthObservation, next_health
from metadata.v2.worker import Worker, WorkerConfig


def test_health_hysteresis_requires_consecutive_results():
    reachable = HealthObservation("reachable", available_bytes=0, writable=False)
    unreachable = HealthObservation("unreachable")
    state = ("ACTIVE", 0, 0)
    state = next_health(*state, unreachable)
    assert state == ("SUSPECTED", 1, 0)
    state = next_health(*state, reachable)
    assert state == ("SUSPECTED", 0, 1)
    state = next_health(*state, unreachable)
    assert state == ("SUSPECTED", 1, 0)
    state = next_health(*state, reachable)
    assert next_health(*state, reachable) == ("ACTIVE", 0, 0)


def test_down_requires_rejoin_even_after_repeated_healthy_observations():
    state = ("ACTIVE", 0, 0)
    for _ in range(3):
        state = next_health(*state, HealthObservation("unreachable"))
    assert state == ("DOWN", 3, 0)
    for _ in range(3):
        state = next_health(*state, HealthObservation("reachable"))
    assert state == ("DOWN", 0, 0)
    assert next_health(*state, HealthObservation("scope_mismatch")) == ("QUARANTINED", 0, 0)


def test_slow_health_poll_allows_job_ticks_and_drains_on_shutdown():
    entered = Event()
    jobs_progressed = Event()
    claims = []
    applied = []

    class Repo:
        def ready(self):
            return True

        def claim_health_poll(self):
            claims.append("node-0")
            return {"node_id": "node-0"}

        def apply_health_poll(self, ticket, observation):
            applied.append(observation)

    class SlowStorage:
        def health(self, node, stopping):
            entered.set()
            stopping.wait(3)
            return HealthObservation("unreachable")

    class ObservedWorker(Worker):
        def tick(self):
            if entered.is_set():
                jobs_progressed.set()

    worker = ObservedWorker(
        Repo(), storage=SlowStorage(), config=WorkerConfig(poll_seconds=0.01, shutdown_seconds=1)
    )
    thread = Thread(target=worker.run)
    thread.start()
    try:
        assert jobs_progressed.wait(2)
        assert claims == ["node-0"]
    finally:
        worker.stopping.set()
        thread.join(2)
    assert not thread.is_alive()
    assert not applied
