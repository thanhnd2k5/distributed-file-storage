import uuid
from contextlib import ExitStack, contextmanager
from threading import Event, Lock, current_thread
from time import monotonic, sleep

import grpc
import pytest
import storage_pb2
from fastapi.testclient import TestClient
from sqlalchemy import event, update

from metadata.bootstrap import initialize_metadata
from metadata.config import MetadataSettings, NodeConfig
from metadata.main import create_app
from metadata.models import Chunk, ChunkReplica, File, StorageNode
from metadata.worker import MetadataWorker
from storage.config import StorageSettings
from storage.server import create_server
from storage.service import StorageService


def wait_until(predicate, timeout=5):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate():
            return
        sleep(0.01)
    raise AssertionError("Timed out waiting for the expected worker observation")


class ProbeService(StorageService):
    def __init__(self, settings):
        super().__init__(settings)
        self.mode = "good"
        self.entered = Event()
        self.release = Event()
        self.lock = Lock()
        self.calls = 0
        self.on_health = lambda: None

    def HealthCheck(self, request, context):
        with self.lock:
            self.calls += 1
        mode = self.mode
        self.entered.set()
        self.on_health()
        if mode in {"blocked", "timeout"}:
            while context.is_active() and not self.release.wait(0.01):
                pass
        result = super().HealthCheck(request, context)
        if mode == "identity":
            result.node_id = "wrong-node"
        elif mode == "domain":
            result.failure_domain = "wrong-domain"
        elif mode == "unwritable":
            result.storage_writable = False
        return result


@pytest.fixture
def nodes(tmp_path):
    with ExitStack() as resources:

        def create(node_id="node-1", mode="good"):
            storage_settings = StorageSettings(
                _env_file=None,
                node_id=node_id,
                failure_domain="test",
                data_dir=tmp_path / node_id,
            )
            service = ProbeService(storage_settings)
            service.mode = mode
            server = create_server(storage_settings, service=service, bind_address="127.0.0.1:0")
            server.start()
            resources.callback(lambda: server.stop(0).wait())
            resources.callback(service.release.set)
            channel = resources.enter_context(grpc.insecure_channel(f"127.0.0.1:{server.port}"))
            grpc.channel_ready_future(channel).result(timeout=5)
            return (
                NodeConfig(
                    node_id=node_id, host="127.0.0.1", port=server.port, failure_domain="test"
                ),
                service,
            )

        yield create


def settings_for(database, configs, **overrides):
    return MetadataSettings(
        **(
            dict(
                _env_file=None,
                database_url=database[1],
                storage_nodes_json=configs,
                replication_factor=1,
                health_interval_seconds=0.05,
                health_rpc_timeout_seconds=0.2,
                node_down_after_seconds=0.6,
            )
            | overrides
        )
    )


def read_node(sessions, node_id="node-1"):
    with sessions() as session:
        return session.get(StorageNode, node_id)


@contextmanager
def running_worker(sessions, settings):
    initialize_metadata(sessions, settings)
    worker = MetadataWorker(sessions, settings)
    try:
        worker.start()
        yield worker
    finally:
        worker.stop()


def test_real_health_persists_metrics_without_transaction_during_rpc(database, nodes):
    sessions, _ = database
    config, service = nodes(mode="blocked")
    checkouts_during_rpc = []
    checked = Event()

    def check_pool():
        checkouts_during_rpc.append(sessions.kw["bind"].pool.checkedout())
        checked.set()

    service.on_health = check_pool
    with running_worker(
        sessions, settings_for(database, [config], health_interval_seconds=5)
    ) as worker:
        assert checked.wait(2)
        service.release.set()
        wait_until(lambda: read_node(sessions).status == "ACTIVE")
        node = read_node(sessions)
        assert node.last_success_at is not None and node.last_error is None
        assert node.capacity_bytes > 0 and node.available_bytes >= 0 and node.used_bytes == 0
        wait_until(lambda: "node-1" in worker.snapshots)
        assert worker.snapshots["node-1"].last_success_monotonic is not None
    assert checkouts_during_rpc and all(count == 0 for count in checkouts_during_rpc)


def test_real_timeout_suspected_down_and_recovery_keep_history(database, nodes):
    sessions, _ = database
    config, service = nodes()
    with running_worker(sessions, settings_for(database, [config])):
        wait_until(lambda: read_node(sessions).status == "ACTIVE")
        service.mode = "timeout"
        wait_until(lambda: read_node(sessions).status == "SUSPECTED")
        before = read_node(sessions)
        wait_until(lambda: read_node(sessions).status == "DOWN")
        down = read_node(sessions)
        assert down.last_error == "DEADLINE_EXCEEDED"
        assert down.last_success_at == before.last_success_at
        assert down.capacity_bytes == before.capacity_bytes
        service.mode = "good"
        wait_until(lambda: read_node(sessions).status == "ACTIVE")
        recovered = read_node(sessions)
        assert recovered.last_error is None and recovered.last_success_at > before.last_success_at


@pytest.mark.parametrize(
    "mode, error",
    [
        ("identity", "IDENTITY_MISMATCH"),
        ("domain", "IDENTITY_MISMATCH"),
        ("unwritable", "STORAGE_NOT_WRITABLE"),
    ],
)
def test_real_invalid_health_stays_down_through_timeout_then_recovers(database, nodes, mode, error):
    sessions, _ = database
    config, service = nodes(mode=mode)
    with running_worker(sessions, settings_for(database, [config])) as worker:
        wait_until(lambda: read_node(sessions).last_error == error)
        rejected = read_node(sessions)
        assert rejected.status == "DOWN" and rejected.last_success_at is None
        assert rejected.capacity_bytes is None
        service.mode = "timeout"
        wait_until(lambda: service.calls >= 3)
        assert read_node(sessions).last_error == error
        assert worker.snapshots["node-1"].hard_down
        service.mode = "good"
        wait_until(lambda: read_node(sessions).status == "ACTIVE")
        assert read_node(sessions).last_error is None


def test_slow_node_does_not_block_other_nodes_or_live_and_never_overlaps_client_calls(
    database, nodes, monkeypatch
):
    sessions, _ = database
    slow, slow_service = nodes("node-1", "timeout")
    fast, fast_service = nodes("node-2")
    third, third_service = nodes("node-3")
    settings = settings_for(
        database, [slow, fast, third], health_rpc_timeout_seconds=1, node_down_after_seconds=2
    )
    monkeypatch.setattr("metadata.main.create_database", lambda _: (sessions.kw["bind"], sessions))
    app = create_app(settings)
    with TestClient(app) as client:
        assert slow_service.entered.wait(2)
        wait_until(lambda: fast_service.calls >= 3 and third_service.calls >= 3, timeout=0.8)
        assert slow_service.calls == 1
        assert read_node(sessions, "node-2").status == "ACTIVE"
        assert read_node(sessions, "node-3").status == "ACTIVE"
        assert client.get("/api/v1/health/live").json() == {"status": "LIVE"}
        assert client.get("/api/v1/health/ready").status_code == 200
        worker = app.state.health_worker
        # Track actual client-side calls, not the server's single Health executor.
        original = worker._stubs["node-1"].HealthCheck
        counts = {"active": 0, "max": 0, "calls": 0}
        lock = Lock()

        def counted(*args, **kwargs):
            with lock:
                counts["active"] += 1
                counts["calls"] += 1
                counts["max"] = max(counts["max"], counts["active"])
            try:
                return original(*args, **kwargs)
            finally:
                with lock:
                    counts["active"] -= 1

        monkeypatch.setattr(worker._stubs["node-1"], "HealthCheck", counted)
        wait_until(lambda: counts["calls"] >= 2)
    assert counts["max"] == 1 and counts["active"] == 0


def test_disabled_node_is_not_polled_and_reenable_requires_new_health(database, nodes):
    sessions, _ = database
    config, service = nodes()
    settings = settings_for(database, [config])
    initialize_metadata(sessions, settings)
    with sessions.begin() as session:
        session.get(StorageNode, "node-1").enabled = False
    worker = MetadataWorker(sessions, settings)
    try:
        worker.start()
        wait_until(lambda: "node-1" in worker.snapshots)
        assert service.calls == 0 and read_node(sessions).status == "DOWN"
        with sessions.begin() as session:
            session.get(StorageNode, "node-1").enabled = True
        wait_until(lambda: read_node(sessions).status == "ACTIVE")
        assert service.calls > 0
    finally:
        worker.stop()


@pytest.mark.parametrize("registry_change", ["disabled", "endpoint"])
def test_late_registry_change_discards_inflight_health(
    database, nodes, monkeypatch, registry_change
):
    sessions, _ = database
    config, service = nodes(mode="blocked")
    settings = settings_for(
        database, [config], health_rpc_timeout_seconds=2, node_down_after_seconds=3
    )
    initialize_metadata(sessions, settings)
    worker = MetadataWorker(sessions, settings)
    persisted = Event()
    original = worker._persist

    def observed_persist(*args):
        original(*args)
        persisted.set()

    monkeypatch.setattr(worker, "_persist", observed_persist)
    try:
        worker.start()
        assert service.entered.wait(2)
        changes = {"enabled": False} if registry_change == "disabled" else {"host": "new-endpoint"}
        with sessions.begin() as session:
            session.execute(update(StorageNode).values(**changes, status="DOWN"))
        service.release.set()
        assert persisted.wait(3)
        node = read_node(sessions)
        assert (
            node.status == "DOWN" and node.last_success_at is None and node.capacity_bytes is None
        )
        assert service.calls == 1
        if registry_change == "disabled":
            assert worker.snapshots["node-1"].last_success_monotonic is None
        else:
            assert "node-1" not in worker.snapshots
    finally:
        worker.stop()


def test_write_failure_rolls_back_and_does_not_publish_candidate_then_recovers(
    database, nodes, caplog
):
    sessions, _ = database
    config, _ = nodes()
    fail = Event()
    fail.set()
    attempted = Event()

    def reject_write(session, context):
        if fail.is_set() and any(isinstance(row, StorageNode) for row in session.dirty):
            attempted.set()
            raise RuntimeError("private database diagnostics must not be logged")

    event.listen(sessions.class_, "after_flush", reject_write)
    try:
        with running_worker(sessions, settings_for(database, [config])) as worker:
            assert attempted.wait(3)
            wait_until(lambda: "Health poll failed" in caplog.text)
            assert (
                read_node(sessions).status == "DOWN" and read_node(sessions).last_success_at is None
            )
            assert worker.snapshots == {}
            assert "private database diagnostics" not in caplog.text
            assert caplog.text.count("Health poll failed") == 1
            fail.clear()
            wait_until(lambda: read_node(sessions).status == "ACTIVE")
            wait_until(lambda: "node-1" in worker.snapshots)
            assert worker.snapshots["node-1"].status == "ACTIVE"
    finally:
        event.remove(sessions.class_, "after_flush", reject_write)


def test_db_read_outage_keeps_live_and_ready_recovers_without_restart(database, nodes, monkeypatch):
    sessions, _ = database
    config, _ = nodes()
    engine = sessions.kw["bind"]
    outage = Event()
    worker_failed = Event()

    def fail_query(connection, cursor, statement, parameters, context, executemany):
        if outage.is_set():
            if current_thread().name.startswith("metadata-health"):
                worker_failed.set()
            raise RuntimeError("injected database outage")

    event.listen(engine, "before_cursor_execute", fail_query)
    monkeypatch.setattr("metadata.main.create_database", lambda _: (engine, sessions))
    app = create_app(settings_for(database, [config]))
    try:
        with TestClient(app) as client:
            wait_until(lambda: read_node(sessions).status == "ACTIVE")
            outage.set()
            assert client.get("/api/v1/health/live").status_code == 200
            ready = client.get("/api/v1/health/ready")
            assert (
                ready.status_code == 503 and ready.json()["error"]["code"] == "METADATA_UNAVAILABLE"
            )
            assert worker_failed.wait(3)
            outage.clear()
            previous = read_node(sessions).last_success_at
            wait_until(lambda: read_node(sessions).last_success_at > previous)
            assert client.get("/api/v1/health/ready").status_code == 200
    finally:
        outage.clear()
        event.remove(engine, "before_cursor_execute", fail_query)


def test_health_ignores_operation_lock_and_preserves_replica_rows(database, nodes, monkeypatch):
    sessions, _ = database
    config, service = nodes(mode="blocked")
    settings = settings_for(
        database, [config], health_rpc_timeout_seconds=2, node_down_after_seconds=3
    )
    initialize_metadata(sessions, settings)
    file_id, chunk_id = uuid.uuid4(), uuid.uuid4()
    with sessions.begin() as session:
        session.add(
            File(
                id=file_id,
                original_name="fixture",
                content_type="application/octet-stream",
                size_bytes=1,
                chunk_size_bytes=2097152,
                total_chunks=1,
                replication_factor=1,
                status="AVAILABLE",
            )
        )
        session.flush()
        session.add(
            Chunk(
                id=chunk_id, file_id=file_id, chunk_index=0, size_bytes=1, checksum_sha256="a" * 64
            )
        )
        session.flush()
        session.add(
            ChunkReplica(
                chunk_id=chunk_id, node_id="node-1", status="PENDING", cleanup_pending=False
            )
        )
    monkeypatch.setattr("metadata.main.create_database", lambda _: (sessions.kw["bind"], sessions))
    app = create_app(settings)
    with TestClient(app):
        assert service.entered.wait(2)
        with app.state.operation_lock:
            service.release.set()
            wait_until(lambda: read_node(sessions).status == "ACTIVE")
        with sessions() as session:
            replica = session.get(ChunkReplica, (chunk_id, "node-1"))
            assert replica.status == "PENDING" and not replica.cleanup_pending
            assert session.get(File, file_id).status == "AVAILABLE"


def test_shutdown_drains_rpc_closes_channels_and_is_idempotent(database, nodes):
    sessions, _ = database
    config, service = nodes(mode="timeout")
    settings = settings_for(database, [config], health_rpc_timeout_seconds=0.3)
    initialize_metadata(sessions, settings)
    worker = MetadataWorker(sessions, settings)
    worker.start()
    try:
        assert service.entered.wait(2)
    finally:
        started = monotonic()
        worker.stop()
    assert monotonic() - started < 2
    assert worker._closed.is_set() and not worker._thread.is_alive()
    assert all(not thread.is_alive() for thread in worker._executor._threads)
    with pytest.raises(ValueError, match="closed channel"):
        worker._stubs["node-1"].HealthCheck(storage_pb2.HealthCheckRequest(), timeout=0.1)
    worker.stop()


def test_all_storage_down_still_ready_after_successful_initialization(database, nodes, monkeypatch):
    sessions, _ = database
    config, _ = nodes(mode="unwritable")
    monkeypatch.setattr("metadata.main.create_database", lambda _: (sessions.kw["bind"], sessions))
    app = create_app(settings_for(database, [config]))
    with TestClient(app) as client:
        wait_until(lambda: read_node(sessions).last_error == "STORAGE_NOT_WRITABLE")
        assert read_node(sessions).status == "DOWN"
        assert client.get("/api/v1/health/ready").json() == {"status": "READY"}


def test_lifespans_start_fresh_worker_and_stop_before_engine_disposal(database, nodes, monkeypatch):
    sessions, _ = database
    config, _ = nodes()
    engine = sessions.kw["bind"]
    monkeypatch.setattr("metadata.main.create_database", lambda _: (engine, sessions))
    workers = []
    for _ in range(2):
        app = create_app(settings_for(database, [config]))
        assert app.state.health_worker is None
        disposals = []

        def on_dispose(engine):
            worker = app.state.health_worker
            assert worker._closed.is_set() and not worker._thread.is_alive()
            assert all(not thread.is_alive() for thread in worker._executor._threads)
            disposals.append(current_thread().name)

        event.listen(engine, "engine_disposed", on_dispose)
        try:
            with TestClient(app):
                workers.append(app.state.health_worker)
                wait_until(lambda: read_node(sessions).status == "ACTIVE")
            assert not app.state.initialized and len(disposals) == 1
        finally:
            event.remove(engine, "engine_disposed", on_dispose)
    assert workers[0] is not workers[1]


def test_partial_worker_start_failure_cleans_resources_and_keeps_not_ready(
    database, nodes, monkeypatch
):
    sessions, _ = database
    first, _ = nodes("node-1")
    second, _ = nodes("node-2")
    monkeypatch.setattr("metadata.main.create_database", lambda _: (sessions.kw["bind"], sessions))
    original = grpc.insecure_channel
    calls = []

    def fail_second_channel(*args, **kwargs):
        calls.append(args[0])
        if len(calls) == 2:
            raise RuntimeError("injected channel construction failure")
        return original(*args, **kwargs)

    monkeypatch.setattr("metadata.worker.grpc.insecure_channel", fail_second_channel)
    app = create_app(settings_for(database, [first, second]))
    with TestClient(app) as client:
        assert not app.state.initialized and app.state.health_worker._closed.is_set()
        assert client.get("/api/v1/health/live").status_code == 200
        assert client.get("/api/v1/health/ready").status_code == 503
        assert app.state.health_worker._executor is None
    assert len(calls) == 2


def test_dead_scheduler_keeps_live_and_snapshots_but_not_ready(
    database, nodes, monkeypatch, caplog
):
    sessions, _ = database
    config, _ = nodes()
    release = Event()

    def crash_scheduler(self):
        release.wait(5)
        raise RuntimeError("private scheduler diagnostics")

    monkeypatch.setattr(MetadataWorker, "_schedule", crash_scheduler)
    monkeypatch.setattr("metadata.main.create_database", lambda _: (sessions.kw["bind"], sessions))
    app = create_app(settings_for(database, [config]))
    try:
        with TestClient(app) as client:
            assert app.state.health_worker.running
            assert client.get("/api/v1/health/ready").status_code == 200
            release.set()
            wait_until(lambda: not app.state.health_worker.running)
            response = client.get("/api/v1/health/ready")
            assert response.status_code == 503
            assert response.json()["error"]["code"] == "METADATA_UNAVAILABLE"
            assert client.get("/api/v1/health/live").json() == {"status": "LIVE"}
            assert client.get("/api/v1/nodes").status_code == 200
            assert client.get("/api/v1/cluster").status_code == 200
    finally:
        release.set()
    assert app.state.health_worker._closed.is_set()
    assert "Health scheduler stopped (RuntimeError)" in caplog.text
    assert "private scheduler diagnostics" not in caplog.text


def test_stopped_worker_is_not_ready_but_still_live(database, nodes, monkeypatch):
    sessions, _ = database
    config, _ = nodes()
    monkeypatch.setattr("metadata.main.create_database", lambda _: (sessions.kw["bind"], sessions))
    app = create_app(settings_for(database, [config]))
    with TestClient(app) as client:
        assert client.get("/api/v1/health/ready").status_code == 200
        app.state.health_worker.stop()
        assert not app.state.health_worker.running
        assert client.get("/api/v1/health/ready").status_code == 503
        assert client.get("/api/v1/health/live").status_code == 200
