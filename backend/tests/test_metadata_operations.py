import hashlib
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event, Lock, current_thread
from time import monotonic, sleep
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from metadata.config import MetadataSettings, NodeConfig
from metadata.main import create_app
from metadata.models import StorageNode
from metadata.operations import OperationError, data_operation
from metadata.storage_client import StorageClient
from storage.config import StorageSettings
from storage.server import create_server

CHUNK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
DATA = b"operation-payload"
HASH = hashlib.sha256(DATA).hexdigest()


def wait_until(predicate, timeout=3):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate():
            return
        sleep(0.01)
    raise AssertionError("Timed out waiting for operation/lifespan observation")


@pytest.fixture
def app_parts(database, tmp_path, monkeypatch):
    sessions, url = database
    server = create_server(
        StorageSettings(
            _env_file=None,
            node_id="node-1",
            failure_domain="test",
            data_dir=tmp_path / "node-1",
        ),
        bind_address="127.0.0.1:0",
    )
    server.start()
    config = NodeConfig(node_id="node-1", host="127.0.0.1", port=server.port, failure_domain="test")
    settings = MetadataSettings(
        _env_file=None,
        database_url=url,
        storage_nodes_json=[config],
        replication_factor=1,
        health_interval_seconds=0.05,
        health_rpc_timeout_seconds=0.2,
        chunk_rpc_timeout_seconds=5,
    )
    monkeypatch.setattr("metadata.main.create_database", lambda _: (sessions.kw["bind"], sessions))
    try:
        yield settings, server.service, sessions
    finally:
        server.stop(0).wait()


@contextmanager
def standalone_operation(settings, sessions):
    client = StorageClient(settings)
    client.start()
    state = SimpleNamespace(
        initialized=True,
        operation_lock=Lock(),
        storage_client=client,
        session_factory=sessions,
        health_worker=SimpleNamespace(running=True),
    )
    try:
        yield state
    finally:
        client.close()


def test_short_distinct_sessions_commit_before_rpc_and_reject_rpc_inside_transaction(app_parts):
    settings, service, sessions = app_parts
    observed = []
    active_session = []
    original_put = service.store.put

    def observe_rpc(*args):
        observed.append(
            (sessions.kw["bind"].pool.checkedout(), active_session[-1].in_transaction())
        )
        return original_put(*args)

    service.store.put = observe_rpc
    with standalone_operation(settings, sessions) as state:
        with data_operation(state) as operation:
            with operation.transaction() as first:
                active_session.append(first)
                assert first.scalar(text("SHOW statement_timeout")) == "5s"
                assert first.scalar(text("SELECT 1")) == 1
                with pytest.raises(RuntimeError, match="before calling Storage"):
                    operation.store_chunk("node-1", CHUNK_ID, DATA, HASH)
                with pytest.raises(RuntimeError, match="Nested"):
                    with operation.transaction():
                        pytest.fail("Nested transaction accepted")
            assert not first.in_transaction()
            ack = operation.store_chunk("node-1", CHUNK_ID, DATA, HASH)
            assert ack.size_bytes == len(DATA)
            with operation.transaction() as second:
                assert second is not first and second.scalar(text("SELECT 2")) == 2
            assert operation.get_chunk("node-1", CHUNK_ID, len(DATA), HASH) == DATA
        assert not state.operation_lock.locked()
    assert observed == [(0, False)]


def test_database_failure_rolls_back_and_releases_operation_lock(app_parts):
    settings, _, sessions = app_parts
    with standalone_operation(settings, sessions) as state:
        with pytest.raises(DBAPIError):
            with data_operation(state) as operation:
                with operation.transaction() as session:
                    session.execute(text("SELECT 1/0"))
        assert not state.operation_lock.locked()
        with data_operation(state) as operation:
            with operation.transaction() as session:
                assert session.scalar(text("SELECT 1")) == 1


def test_delete_operation_committed_scope_thread_and_lifetime_guards(app_parts):
    settings, _, sessions = app_parts
    with standalone_operation(settings, sessions) as state:
        with data_operation(state) as operation:
            with operation.transaction():
                with pytest.raises(RuntimeError, match="before calling Storage"):
                    operation.delete_chunk("node-1", CHUNK_ID)
            with ThreadPoolExecutor(max_workers=1) as executor:
                with pytest.raises(RuntimeError, match="owning thread"):
                    executor.submit(operation.delete_chunk, "node-1", CHUNK_ID).result(timeout=2)
            operation.store_chunk("node-1", CHUNK_ID, DATA, HASH)
            assert operation.delete_chunk("node-1", CHUNK_ID).existed
        with pytest.raises(RuntimeError, match="owning thread"):
            operation.delete_chunk("node-1", CHUNK_ID)
        assert not state.operation_lock.locked()


def test_operation_cannot_cross_threads_or_escape_scope(app_parts):
    settings, _, sessions = app_parts
    with standalone_operation(settings, sessions) as state:
        with data_operation(state) as operation:
            with ThreadPoolExecutor(max_workers=1) as executor:
                call = executor.submit(operation.get_chunk, "node-1", CHUNK_ID, len(DATA), HASH)
                with pytest.raises(RuntimeError, match="owning thread"):
                    call.result(timeout=2)
        with pytest.raises(RuntimeError, match="owning thread"):
            operation.get_chunk("node-1", CHUNK_ID, len(DATA), HASH)
        assert not state.operation_lock.locked()


@pytest.mark.parametrize("initialized, has_client", [(False, False), (True, False), (True, True)])
def test_admission_rejects_uninitialized_missing_or_stopped_client(initialized, has_client):
    state = SimpleNamespace(
        initialized=initialized,
        health_worker=SimpleNamespace(running=True),
        operation_lock=Lock(),
        session_factory=None,
        storage_client=SimpleNamespace(running=False) if has_client else None,
    )
    with pytest.raises(OperationError) as caught:
        with data_operation(state):
            pytest.fail("Unavailable operation admitted")
    assert (caught.value.status_code, caught.value.code) == (503, "METADATA_UNAVAILABLE")
    assert not state.operation_lock.locked()


@pytest.mark.parametrize("worker", [None, SimpleNamespace(running=False)])
def test_admission_rejects_missing_or_stopped_worker_even_when_busy(worker):
    state = SimpleNamespace(
        initialized=True,
        health_worker=worker,
        storage_client=SimpleNamespace(running=True),
        operation_lock=Lock(),
    )
    state.operation_lock.acquire()
    try:
        with pytest.raises(OperationError) as caught:
            with data_operation(state):
                pytest.fail("Dead scheduler admitted work")
        assert (caught.value.status_code, caught.value.code) == (503, "METADATA_UNAVAILABLE")
        assert state.operation_lock.locked()  # Admission did not release somebody else's lock.
    finally:
        state.operation_lock.release()


def test_admission_rechecks_worker_after_acquiring_lock():
    worker = SimpleNamespace(running=True)
    lock = Lock()

    class StoppingLock:
        def acquire(self, *, blocking):
            acquired = lock.acquire(blocking=blocking)
            worker.running = False
            return acquired

        def release(self):
            lock.release()

    state = SimpleNamespace(
        initialized=True,
        health_worker=worker,
        storage_client=SimpleNamespace(running=True),
        operation_lock=StoppingLock(),
    )
    with pytest.raises(OperationError) as caught:
        with data_operation(state):
            pytest.fail("Scheduler stopped during admission")
    assert caught.value.status_code == 503 and not lock.locked()


def test_http_busy_envelope_health_and_reads_continue_during_real_data_rpc(app_parts, monkeypatch):
    settings, service, sessions = app_parts
    app = create_app(settings)
    entered, release = Event(), Event()
    original_put = service.store.put

    def block_put(*args):
        entered.set()
        assert release.wait(3)
        return original_put(*args)

    monkeypatch.setattr(service.store, "put", block_put)
    transfer_threads = []

    @app.post("/__test/transfer")
    def transfer():
        transfer_threads.append(current_thread().name)
        with data_operation(app.state) as operation:
            with operation.transaction() as session:
                node_id = session.get(StorageNode, "node-1").node_id
            ack = operation.store_chunk(node_id, CHUNK_ID, DATA, HASH)
            with operation.transaction() as session:
                assert session.scalar(text("SELECT 1")) == 1
            return {"size_bytes": ack.size_bytes}

    with TestClient(app) as http, ThreadPoolExecutor(max_workers=1) as executor:
        wait_until(lambda: http.get("/api/v1/nodes").json()["items"][0]["status"] == "ACTIVE")
        before = http.get("/api/v1/nodes").json()["items"][0]["last_success_at"]
        first = executor.submit(http.post, "/__test/transfer")
        try:
            assert entered.wait(2)
            busy = http.post("/__test/transfer")
            assert busy.status_code == 409 and busy.json()["error"]["code"] == "OPERATION_BUSY"
            assert busy.json()["error"]["details"] == {}
            assert http.get("/api/v1/cluster").json()["operation_busy"]
            assert http.get("/api/v1/health/live").status_code == 200
            assert http.get("/api/v1/health/ready").status_code == 200
            wait_until(
                lambda: http.get("/api/v1/nodes").json()["items"][0]["last_success_at"] != before
            )
        finally:
            release.set()
        assert first.result(timeout=3).json() == {"size_bytes": len(DATA)}
        assert not app.state.operation_lock.locked()
    assert all("AnyIO" in name for name in transfer_threads)


def test_lifespan_waits_for_operation_final_db_work_before_closing_clients_engine(
    app_parts, monkeypatch
):
    settings, service, sessions = app_parts
    app = create_app(settings)
    entered, release = Event(), Event()
    original_put = service.store.put
    observations = []

    def block_put(*args):
        entered.set()
        assert release.wait(3)
        return original_put(*args)

    monkeypatch.setattr(service.store, "put", block_put)

    def transfer():
        with data_operation(app.state) as operation:
            ack = operation.store_chunk("node-1", CHUNK_ID, DATA, HASH)
            with operation.transaction() as session:
                assert session.scalar(text("SELECT 42")) == 42
            observations.append("operation finished")
            return ack

    def on_dispose(engine):
        assert observations == ["operation finished"]
        assert not app.state.initialized and not app.state.operation_lock.locked()
        assert app.state.storage_client._closed and app.state.storage_client._active_calls == 0
        assert not app.state.health_worker.running and app.state.health_worker._closed.is_set()
        observations.append("engine disposed")

    engine = sessions.kw["bind"]
    event.listen(engine, "engine_disposed", on_dispose)
    context = TestClient(app)
    context.__enter__()
    shutdown = None
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            active = executor.submit(transfer)
            assert entered.wait(2)
            shutdown = executor.submit(context.__exit__, None, None, None)
            try:
                wait_until(lambda: not app.state.initialized)
                assert not shutdown.done() and app.state.storage_client.running
                with pytest.raises(OperationError) as caught:
                    with data_operation(app.state):
                        pytest.fail("Shutdown must stop admitting work")
                assert caught.value.code == "METADATA_UNAVAILABLE"
            finally:
                release.set()
            assert active.result(timeout=3).size_bytes == len(DATA)
            shutdown.result(timeout=3)
    finally:
        release.set()
        if shutdown is None:
            context.__exit__(None, None, None)
        event.remove(engine, "engine_disposed", on_dispose)
    assert observations == ["operation finished", "engine disposed"]


def test_fresh_data_client_each_lifespan_and_stopped_client_gates_ready(app_parts):
    settings, _, _ = app_parts
    app = create_app(settings)
    clients = []
    assert app.state.storage_client is None
    for _ in range(2):
        with TestClient(app) as http:
            client = app.state.storage_client
            clients.append(client)
            assert client.running and http.get("/api/v1/health/ready").status_code == 200
            client.close()
            assert http.get("/api/v1/health/ready").status_code == 503
            assert http.get("/api/v1/health/live").status_code == 200
            assert http.get("/api/v1/nodes").status_code == 200
    assert clients[0] is not clients[1] and all(not client.running for client in clients)


def test_data_client_start_failure_stops_health_and_preserves_live(app_parts, monkeypatch):
    settings, _, _ = app_parts

    def fail_start(self):
        raise RuntimeError("injected data client failure")

    monkeypatch.setattr("metadata.main.StorageClient.start", fail_start)
    app = create_app(settings)

    @app.post("/__test/transfer")
    def transfer():
        with data_operation(app.state):
            pytest.fail("Failed startup admitted work")

    with TestClient(app) as http:
        assert not app.state.initialized and not app.state.storage_client.running
        assert not app.state.health_worker.running
        assert app.state.health_worker._closed.is_set()
        assert http.get("/api/v1/health/live").status_code == 200
        assert http.get("/api/v1/health/ready").status_code == 503
        assert http.post("/__test/transfer").json()["error"]["code"] == "METADATA_UNAVAILABLE"
    assert not app.state.operation_lock.locked()
