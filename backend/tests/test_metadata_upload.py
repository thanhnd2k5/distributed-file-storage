import asyncio
import hashlib
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from time import monotonic, sleep
from uuid import UUID

import grpc
import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, select, update
from sqlalchemy.exc import SQLAlchemyError

from metadata.bootstrap import initialize_metadata
from metadata.config import MetadataSettings, NodeConfig
from metadata.main import create_app
from metadata.models import Chunk, ChunkReplica, File, StorageNode
from metadata.operations import DataOperation
from metadata.routes import files as file_routes
from storage.config import StorageSettings
from storage.server import create_server
from storage.service import StorageService

C = 256 * 1024
MAX = 2 * C + 7


def wait_until(predicate, timeout=4):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate():
            return
        sleep(0.01)
    raise AssertionError("Timed out waiting for upload observation")


class ObservedStorage(StorageService):
    """Inject faults around the real handler registered with the production server."""

    def __init__(self, settings):
        super().__init__(settings)
        self.store_handler = super().StoreChunk
        self.get_handler = super().GetChunk

    def StoreChunk(self, request, context):
        return self.store_handler(request, context)

    def GetChunk(self, request, context):
        return self.get_handler(request, context)


@pytest.fixture
def cluster(database, tmp_path, monkeypatch):
    sessions, url = database
    servers = []
    configs = []
    for index in range(1, 4):
        node_id = f"node-{index}"
        settings = StorageSettings(
            _env_file=None,
            node_id=node_id,
            failure_domain=node_id,
            data_dir=tmp_path / node_id,
            chunk_size_bytes=4 * 1024 * 1024,
        )
        server = create_server(
            settings, service=ObservedStorage(settings), bind_address="127.0.0.1:0"
        )
        server.start()
        servers.append(server)
        configs.append(
            NodeConfig(
                node_id=node_id,
                host="127.0.0.1",
                port=server.port,
                failure_domain=node_id,
            )
        )
    settings = MetadataSettings(
        _env_file=None,
        database_url=url,
        storage_nodes_json=configs,
        chunk_size_bytes=C,
        max_file_size_bytes=MAX,
        health_interval_seconds=0.05,
        health_rpc_timeout_seconds=0.3,
        chunk_rpc_timeout_seconds=2,
    )
    monkeypatch.setattr("metadata.main.create_database", lambda _: (sessions.kw["bind"], sessions))
    try:
        yield settings, sessions, servers
    finally:
        for server in servers:
            server.stop(0).wait()


def start_http(cluster):
    settings, _, _ = cluster
    app = create_app(settings)
    http = TestClient(app)
    http.__enter__()
    try:
        wait_until(lambda: http.get("/api/v1/cluster").json()["nodes"]["active"] == 3)
    except BaseException:
        http.__exit__(None, None, None)
        raise
    return app, http


def rows(sessions):
    with sessions() as session:
        return (
            session.scalars(select(File)).all(),
            session.scalars(select(Chunk).order_by(Chunk.chunk_index)).all(),
            session.scalars(select(ChunkReplica)).all(),
        )


@pytest.mark.parametrize("size", [0, 1, C, C + 5, 2 * C, MAX])
def test_http_upload_size_hash_rf_location_and_actual_storage(cluster, size):
    _, sessions, servers = cluster
    app, http = start_http(cluster)
    data = (b"abcd" * ((size + 3) // 4))[:size]
    try:
        result = http.post(
            "/api/v1/files", files={"file": ("../báo cáo.bin", data, "application/test")}
        )
        assert result.status_code == 201, result.text
        body = result.json()
        file_id = UUID(body["file_id"])
        assert result.headers["location"] == f"/api/v1/files/{file_id}"
        assert body == {
            "file_id": str(file_id),
            "original_name": "báo cáo.bin",
            "content_type": "application/test",
            "size_bytes": size,
            "chunk_size_bytes": C,
            "total_chunks": (size + C - 1) // C,
            "replication_factor": 2,
            "checksum_sha256": hashlib.sha256(data).hexdigest(),
            "status": "AVAILABLE",
            "created_at": body["created_at"],
        }
        assert body["created_at"].endswith("Z")
        files, chunks, replicas = rows(sessions)
        assert len(files) == 1 and files[0].status == "AVAILABLE"
        assert len(chunks) == body["total_chunks"]
        assert len(replicas) == 2 * len(chunks)
        for chunk in chunks:
            payload = data[chunk.chunk_index * C : (chunk.chunk_index + 1) * C]
            copies = [r for r in replicas if r.chunk_id == chunk.id]
            assert len({r.node_id for r in copies}) == 2
            for replica in copies:
                assert replica.status == "VERIFIED" and not replica.cleanup_pending
                assert replica.last_verified_at is not None and replica.last_error is None
                store = servers[int(replica.node_id[-1]) - 1].service.store
                assert store.chunk_path(str(chunk.id)).read_bytes() == payload
        assert not app.state.operation_lock.locked()
    finally:
        http.__exit__(None, None, None)


def test_default_64_mib_limit_roundtrip(cluster):
    settings, sessions, _ = cluster
    settings.chunk_size_bytes = 4 * 1024 * 1024
    settings.max_file_size_bytes = 64 * 1024 * 1024
    _, http = start_http(cluster)
    data = b"m" * settings.max_file_size_bytes
    try:
        response = http.post("/api/v1/files", files={"file": ("max.bin", data)})
        assert response.status_code == 201, response.text
        assert response.json()["total_chunks"] == 16
        assert response.json()["checksum_sha256"] == hashlib.sha256(data).hexdigest()
        assert len(rows(sessions)[2]) == 32
    finally:
        http.__exit__(None, None, None)


@pytest.mark.parametrize(
    "kwargs,status,code",
    [
        ({"json": {"file": "bytes"}}, 400, "INVALID_REQUEST"),
        ({"files": {"wrong": ("a", b"x")}}, 400, "INVALID_REQUEST"),
        ({"files": [("file", ("a", b"x")), ("file", ("b", b"y"))]}, 400, "INVALID_REQUEST"),
        ({"files": {"file": ("a", b"x")}, "data": {"rf": "1"}}, 400, "INVALID_REQUEST"),
        ({"files": {"file": ("..", b"x")}}, 400, "INVALID_REQUEST"),
        ({"files": {"file": ("a" * 256, b"x")}}, 400, "INVALID_REQUEST"),
        ({"files": {"file": ("large", b"x" * (MAX + 1))}}, 413, "FILE_TOO_LARGE"),
        (
            {"content": b"bad", "headers": {"content-type": "multipart/form-data"}},
            400,
            "INVALID_REQUEST",
        ),
        ({"files": {"file": (None, "text field")}}, 400, "INVALID_REQUEST"),
    ],
)
def test_invalid_http_input_creates_no_metadata_or_chunks(cluster, kwargs, status, code):
    _, sessions, servers = cluster
    app, http = start_http(cluster)
    try:
        response = http.post("/api/v1/files", **kwargs)
        assert response.status_code == status, response.text
        assert response.json()["error"]["code"] == code
        assert response.json()["error"]["details"] == {}
        assert rows(sessions) == ([], [], [])
        assert all(server.service.store.used_bytes == 0 for server in servers)
        assert not app.state.operation_lock.locked()
    finally:
        http.__exit__(None, None, None)


def test_empty_needs_no_active_nodes_and_repeated_post_creates_new_uuid(cluster):
    _, sessions, _ = cluster
    app, http = start_http(cluster)
    try:
        app.state.health_worker.stop()
        with sessions.begin() as session:
            session.execute(update(StorageNode).values(status="DOWN"))
        nonempty = http.post("/api/v1/files", files={"file": ("x", b"x")})
        assert nonempty.status_code == 503
        assert nonempty.json()["error"]["code"] == "INSUFFICIENT_NODES"
        assert rows(sessions) == ([], [], [])
        first = http.post("/api/v1/files", files={"file": ("x", b"")})
        second = http.post("/api/v1/files", files={"file": ("x", b"")})
        assert first.status_code == second.status_code == 201
        assert first.json()["file_id"] != second.json()["file_id"]
        assert all(f.status == "AVAILABLE" for f in rows(sessions)[0])
    finally:
        http.__exit__(None, None, None)


def test_pending_committed_before_every_store_with_no_coordinator_transaction(cluster, monkeypatch):
    _, sessions, servers = cluster
    app, http = start_http(cluster)
    observed = []
    try:
        app.state.health_worker.stop()
        for server in servers:
            original = server.service.store_handler

            def observe(
                request, context, original=original, node_id=server.service.settings.node_id
            ):
                assert sessions.kw["bind"].pool.checkedout() == 0
                with sessions() as session:
                    replica = session.get(ChunkReplica, (UUID(request.chunk_id), node_id))
                    chunk = session.get(Chunk, replica.chunk_id)
                    file = session.get(File, chunk.file_id)
                    assert replica.status == "PENDING" and file.status == "UPLOADING"
                observed.append((request.chunk_id, node_id))
                return original(request, context)

            monkeypatch.setattr(server.service, "store_handler", observe)
        response = http.post("/api/v1/files", files={"file": ("multi", b"x" * (C + 1))})
        assert response.status_code == 201, response.text
        assert len(observed) == 4 and len(set(observed)) == 4
    finally:
        http.__exit__(None, None, None)


@pytest.mark.parametrize("mode", ["full", "invalid_ack", "timeout_after_store"])
def test_fallback_preserves_pending_and_excludes_failed_node_for_later_chunks(
    cluster, monkeypatch, mode
):
    settings, sessions, servers = cluster
    settings.chunk_rpc_timeout_seconds = 0.15
    app, http = start_http(cluster)
    calls = []
    original = servers[0].service.store_handler

    def fail(request, context):
        calls.append(request.chunk_id)
        if mode == "full":
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "test full")
        ack = original(request, context)
        if mode == "invalid_ack":
            ack.size_bytes += 1
            return ack
        while context.is_active():
            sleep(0.005)
        return ack

    monkeypatch.setattr(servers[0].service, "store_handler", fail)
    try:
        response = http.post("/api/v1/files", files={"file": ("multi", b"x" * (C + 1))})
        assert response.status_code == 201, response.text
        files, chunks, replicas = rows(sessions)
        assert files[0].status == "AVAILABLE" and len(chunks) == 2
        pending = [r for r in replicas if r.status == "PENDING"]
        assert len(pending) == 1 and pending[0].node_id == "node-1"
        assert pending[0].last_error is not None
        assert all(not r.cleanup_pending for r in replicas)
        assert len(calls) == (2 if mode == "timeout_after_store" else 1)
        assert len(set(calls)) == 1
        with sessions() as session:
            assert session.get(StorageNode, "node-1").status == "ACTIVE"
    finally:
        http.__exit__(None, None, None)


def test_ack_loss_retry_uses_same_mapping_and_storage_id(cluster, monkeypatch):
    _, sessions, servers = cluster
    app, http = start_http(cluster)
    original = servers[0].service.store_handler
    calls, duplicates = [], []

    def lose_first_ack(request, context):
        calls.append(request.chunk_id)
        ack = original(request, context)
        duplicates.append(ack.already_existed)
        if len(calls) == 1:
            context.abort(grpc.StatusCode.UNAVAILABLE, "injected ack loss")
        return ack

    monkeypatch.setattr(servers[0].service, "store_handler", lose_first_ack)
    try:
        response = http.post("/api/v1/files", files={"file": ("x", b"payload")})
        assert response.status_code == 201, response.text
        assert len(calls) == 2 and calls[0] == calls[1]
        assert duplicates == [False, True]
        assert len(rows(sessions)[2]) == 2
    finally:
        http.__exit__(None, None, None)


def test_insufficient_rf_marks_all_attempts_for_cleanup_and_releases_lock(cluster, monkeypatch):
    _, sessions, servers = cluster
    app, http = start_http(cluster)

    def fail(request, context):
        context.abort(grpc.StatusCode.INTERNAL, "not returned to HTTP")

    for server in servers[1:]:
        monkeypatch.setattr(server.service, "store_handler", fail)
    try:
        response = http.post("/api/v1/files", files={"file": ("x", b"payload")})
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "UPLOAD_REPLICATION_FAILED"
        files, _, replicas = rows(sessions)
        assert response.json()["error"]["details"] == {"file_id": str(files[0].id)}
        assert files[0].status == "FAILED" and files[0].error_code == "UPLOAD_REPLICATION_FAILED"
        assert len(replicas) == 3 and all(r.cleanup_pending for r in replicas)
        assert sorted(r.status for r in replicas) == ["PENDING", "PENDING", "VERIFIED"]
        assert servers[0].service.store.used_bytes == len(b"payload")
        assert not app.state.operation_lock.locked()
        assert http.post("/api/v1/files", files={"file": ("empty", b"")}).status_code == 201
    finally:
        http.__exit__(None, None, None)


def test_last_chunk_ack_gates_available_busy_and_health_continue(cluster, monkeypatch):
    _, sessions, servers = cluster
    app, http = start_http(cluster)
    entered, release = Event(), Event()
    # Placement of chunk 2 is node-3 then node-1/2; block every candidate for its final ack.
    originals = {s.service.settings.node_id: s.service.store_handler for s in servers}
    seen_tail = []

    def block_final(request, context, node_id):
        if len(request.data) == 1:
            seen_tail.append(node_id)
            if len(seen_tail) == 2:
                entered.set()
                assert release.wait(3)
        return originals[node_id](request, context)

    for server in servers:
        node_id = server.service.settings.node_id
        monkeypatch.setattr(
            server.service, "store_handler", lambda r, c, n=node_id: block_final(r, c, n)
        )
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(
                http.post, "/api/v1/files", files={"file": ("x", b"x" * (C + 1))}
            )
            try:
                assert entered.wait(2)
                assert not pending.done() and rows(sessions)[0][0].status == "UPLOADING"
                busy = http.post("/api/v1/files", files={"file": ("other", b"y")})
                assert busy.status_code == 409 and busy.json()["error"]["code"] == "OPERATION_BUSY"
                before = http.get("/api/v1/nodes").json()["items"][0]["last_success_at"]
                assert http.get("/api/v1/cluster").json()["operation_busy"]
                assert http.get("/api/v1/health/ready").status_code == 200
                wait_until(
                    lambda: (
                        http.get("/api/v1/nodes").json()["items"][0]["last_success_at"] != before
                    )
                )
            finally:
                release.set()
            assert pending.result(timeout=3).status_code == 201
            assert rows(sessions)[0][0].status == "AVAILABLE"
    finally:
        release.set()
        http.__exit__(None, None, None)


def test_database_failure_after_store_keeps_committed_attempt_and_recovery(cluster, monkeypatch):
    settings, sessions, _ = cluster
    app, http = start_http(cluster)
    original = DataOperation.store_chunk
    engine = sessions.kw["bind"]
    armed = Event()

    def store_then_break_db(self, *args, **kwargs):
        ack = original(self, *args, **kwargs)
        armed.set()
        return ack

    def break_db(connection, cursor, statement, parameters, context, executemany):
        if armed.is_set():
            raise SQLAlchemyError("injected DB outage after durable Store")

    monkeypatch.setattr(DataOperation, "store_chunk", store_then_break_db)
    app.state.health_worker.stop()
    event.listen(engine, "before_cursor_execute", break_db)
    try:
        response = http.post("/api/v1/files", files={"file": ("x", b"payload")})
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "METADATA_UNAVAILABLE"
        assert not app.state.operation_lock.locked()
    finally:
        event.remove(engine, "before_cursor_execute", break_db)
        http.__exit__(None, None, None)
    files, _, replicas = rows(sessions)
    assert files[0].status == "UPLOADING" and len(replicas) == 1
    assert replicas[0].status == "PENDING"
    initialize_metadata(sessions, settings)
    files, _, replicas = rows(sessions)
    assert files[0].status == "FAILED" and files[0].error_code == "UPLOAD_INTERRUPTED"
    assert replicas[0].cleanup_pending


async def asgi_upload(app, *, entered=None, cancel_task=False, fail_send=False):
    request = httpx.Request("POST", "http://test/api/v1/files", files={"file": ("x", b"payload")})
    body = request.read()
    incoming = asyncio.Queue()
    incoming.put_nowait({"type": "http.request", "body": body, "more_body": False})
    sent = []
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/api/v1/files",
        "raw_path": b"/api/v1/files",
        "query_string": b"",
        "root_path": "",
        "headers": [(key.lower(), value) for key, value in request.headers.raw],
        "client": ("127.0.0.1", 1234),
        "server": ("test", 80),
    }

    async def send(message):
        sent.append(message)
        if fail_send:
            assert message["type"] == "http.response.start" and message["status"] == 201
            raise RuntimeError("injected lost HTTP response")

    task = asyncio.create_task(app(scope, incoming.get, send))
    if entered is not None:
        assert await asyncio.to_thread(entered.wait, 2)
        if cancel_task:
            task.cancel()
        else:
            incoming.put_nowait({"type": "http.disconnect"})
    try:
        await asyncio.wait_for(task, 4)
    except asyncio.CancelledError:
        assert cancel_task
    except RuntimeError:
        assert fail_send
    return sent


@pytest.mark.parametrize("cancel_task", [False, True])
def test_http_disconnect_and_coroutine_cancel_wait_for_failure_persistence(
    cluster, monkeypatch, cancel_task
):
    _, sessions, servers = cluster
    app, http = start_http(cluster)
    entered = Event()
    original = servers[0].service.store_handler
    observed_spools = []
    original_transfer = file_routes.upload_file

    def track_spool(state, parts, cancel):
        parts = tuple(parts)
        observed_spools.append(parts[0][1].file)
        return original_transfer(state, parts, cancel)

    def store_and_lose_ack(request, context):
        ack = original(request, context)
        entered.set()
        while context.is_active():
            sleep(0.005)
        return ack

    monkeypatch.setattr("metadata.routes.files.upload_file", track_spool)
    monkeypatch.setattr(servers[0].service, "store_handler", store_and_lose_ack)
    try:
        http.portal.call(lambda: asgi_upload(app, entered=entered, cancel_task=cancel_task))
        files, _, replicas = rows(sessions)
        assert files[0].status == "FAILED" and files[0].error_code == "UPLOAD_INTERRUPTED"
        assert len(replicas) == 1 and replicas[0].cleanup_pending
        assert replicas[0].status == "PENDING"
        assert servers[0].service.store.used_bytes == len(b"payload")
        assert observed_spools[0].closed
        assert not app.state.operation_lock.locked()
        assert app.state.storage_client._active_calls == 0
    finally:
        http.__exit__(None, None, None)


def test_lost_http_response_after_commit_does_not_fail_available(cluster):
    _, sessions, _ = cluster
    app, http = start_http(cluster)
    try:
        http.portal.call(lambda: asgi_upload(app, fail_send=True))
        files, _, replicas = rows(sessions)
        assert files[0].status == "AVAILABLE" and files[0].error_code is None
        assert len(replicas) == 2 and all(not r.cleanup_pending for r in replicas)
        assert not app.state.operation_lock.locked()
    finally:
        http.__exit__(None, None, None)


@pytest.mark.parametrize("failure", ["truncated", "parse_disconnect", "spool_write"])
def test_parse_failures_close_unfinished_spools_without_metadata(cluster, monkeypatch, failure):
    from tempfile import SpooledTemporaryFile

    _, sessions, _ = cluster
    app, http = start_http(cluster)
    spools = []

    def spool(*args, **kwargs):
        file = SpooledTemporaryFile(*args, **kwargs)
        spools.append(file)
        if failure == "spool_write":

            def fail_write(data):
                raise OSError("test disk full")

            file.write = fail_write
        return file

    monkeypatch.setattr("starlette.formparsers.SpooledTemporaryFile", spool)
    body = b'--b\r\nContent-Disposition: form-data; name="file"; filename="x"\r\n\r\npayload'
    try:
        if failure == "parse_disconnect":

            async def partial_request():
                incoming = asyncio.Queue()
                incoming.put_nowait({"type": "http.request", "body": body, "more_body": True})
                incoming.put_nowait({"type": "http.disconnect"})
                sent = []

                async def send(message):
                    sent.append(message)

                scope = {
                    "type": "http",
                    "asgi": {"version": "3.0"},
                    "http_version": "1.1",
                    "method": "POST",
                    "scheme": "http",
                    "path": "/api/v1/files",
                    "query_string": b"",
                    "headers": [(b"content-type", b"multipart/form-data; boundary=b")],
                }
                await app(scope, incoming.get, send)
                assert sent[0]["status"] == 400

            http.portal.call(partial_request)
        else:
            response = http.post(
                "/api/v1/files",
                content=body,
                headers={"content-type": "multipart/form-data; boundary=b"},
            )
            assert response.status_code == (503 if failure == "spool_write" else 400)
        assert spools and all(file.closed for file in spools)
        assert rows(sessions) == ([], [], [])
        assert not app.state.operation_lock.locked()
    finally:
        http.__exit__(None, None, None)


def test_actual_file_size_ignores_http_content_length(cluster):
    _, sessions, _ = cluster
    _, http = start_http(cluster)
    try:
        response = http.post(
            "/api/v1/files",
            files={"file": ("x", b"x" * (MAX + 1))},
            headers={"content-length": "1"},
        )
        assert response.status_code == 413 and response.json()["error"]["code"] == "FILE_TOO_LARGE"
        assert rows(sessions) == ([], [], [])
    finally:
        http.__exit__(None, None, None)


def test_pending_commit_failure_prevents_store_and_fails_file(cluster):
    _, sessions, servers = cluster
    app, http = start_http(cluster)
    engine = sessions.kw["bind"]
    injected = False

    def break_pending(connection, cursor, statement, parameters, context, executemany):
        nonlocal injected
        if not injected and "INSERT INTO" in statement and "chunk_replicas" in statement:
            injected = True
            raise SQLAlchemyError("injected attempt commit failure")

    event.listen(engine, "before_cursor_execute", break_pending)
    try:
        response = http.post("/api/v1/files", files={"file": ("x", b"payload")})
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "METADATA_UNAVAILABLE"
        files, chunks, replicas = rows(sessions)
        assert files[0].status == "FAILED" and len(chunks) == 1 and replicas == []
        assert all(server.service.store.used_bytes == 0 for server in servers)
        assert not app.state.operation_lock.locked()
    finally:
        event.remove(engine, "before_cursor_execute", break_pending)
        http.__exit__(None, None, None)


def test_final_commit_ack_loss_does_not_change_available_to_failed(cluster):
    _, sessions, _ = cluster
    app, http = start_http(cluster)
    injected = False

    def lose_ack(session):
        nonlocal injected
        if session.bind is sessions.kw["bind"] and any(
            isinstance(row, File) and row.status == "AVAILABLE"
            for row in session.identity_map.values()
        ):
            injected = True
            raise SQLAlchemyError("injected acknowledgement loss after DB commit")

    event.listen(sessions.class_, "after_commit", lose_ack)
    try:
        response = http.post("/api/v1/files", files={"file": ("x", b"payload")})
        assert injected and response.status_code == 503
        assert response.json()["error"]["code"] == "METADATA_UNAVAILABLE"
        files, _, replicas = rows(sessions)
        assert files[0].status == "AVAILABLE" and files[0].error_code is None
        assert response.json()["error"]["details"]["file_id"] == str(files[0].id)
        assert all(not row.cleanup_pending for row in replicas)
        assert not app.state.operation_lock.locked()
    finally:
        event.remove(sessions.class_, "after_commit", lose_ack)
        http.__exit__(None, None, None)


def test_commit_uses_file_snapshots_and_acknowledgements_despite_health_change(
    cluster, monkeypatch
):
    settings, sessions, _ = cluster
    app, http = start_http(cluster)
    app.state.health_worker.stop()
    original = DataOperation.store_chunk
    calls = 0

    def acknowledge_then_change_snapshot(self, *args, **kwargs):
        nonlocal calls
        ack = original(self, *args, **kwargs)
        calls += 1
        if calls == 2:
            settings.replication_factor = 3
            settings.chunk_size_bytes = 4 * 1024 * 1024
            with self.transaction() as session:
                session.execute(update(StorageNode).values(status="DOWN"))
        return ack

    monkeypatch.setattr(DataOperation, "store_chunk", acknowledge_then_change_snapshot)
    try:
        response = http.post("/api/v1/files", files={"file": ("x", b"payload")})
        assert response.status_code == 201, response.text
        assert response.json()["replication_factor"] == 2
        assert response.json()["chunk_size_bytes"] == C
        assert rows(sessions)[0][0].status == "AVAILABLE"
        assert http.get("/api/v1/cluster").json()["nodes"]["down"] == 3
    finally:
        http.__exit__(None, None, None)


@pytest.mark.parametrize("mutation", ["checksum", "replica"])
def test_final_commit_rejects_inconsistent_persisted_chunks_or_rf(cluster, monkeypatch, mutation):
    _, sessions, _ = cluster
    app, http = start_http(cluster)
    original = DataOperation.store_chunk
    calls = 0

    def acknowledge_then_mutate(self, node_id, chunk_id, *args, **kwargs):
        nonlocal calls
        ack = original(self, node_id, chunk_id, *args, **kwargs)
        calls += 1
        if calls == 2:
            with self.transaction() as session:
                if mutation == "checksum":
                    session.get(Chunk, UUID(chunk_id)).checksum_sha256 = "0" * 64
                else:
                    session.execute(
                        update(ChunkReplica)
                        .where(ChunkReplica.chunk_id == UUID(chunk_id))
                        .values(status="PENDING")
                    )
        return ack

    monkeypatch.setattr(DataOperation, "store_chunk", acknowledge_then_mutate)
    try:
        response = http.post("/api/v1/files", files={"file": ("x", b"payload")})
        assert response.status_code == 500 and response.json()["error"]["code"] == "INTERNAL_ERROR"
        files, _, replicas = rows(sessions)
        assert files[0].status == "FAILED" and all(row.cleanup_pending for row in replicas)
        assert not app.state.operation_lock.locked()
    finally:
        http.__exit__(None, None, None)


def test_database_failure_before_file_creation_has_no_file_id(cluster):
    _, sessions, servers = cluster
    app, http = start_http(cluster)
    app.state.health_worker.stop()
    engine = sessions.kw["bind"]

    def unavailable(connection, cursor, statement, parameters, context, executemany):
        raise SQLAlchemyError("injected database unavailable at upload admission")

    event.listen(engine, "before_cursor_execute", unavailable)
    try:
        response = http.post("/api/v1/files", files={"file": ("x", b"payload")})
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "METADATA_UNAVAILABLE"
        assert response.json()["error"]["details"] == {}
        assert not app.state.operation_lock.locked()
    finally:
        event.remove(engine, "before_cursor_execute", unavailable)
        http.__exit__(None, None, None)
    assert rows(sessions) == ([], [], [])
    assert all(server.service.store.used_bytes == 0 for server in servers)
