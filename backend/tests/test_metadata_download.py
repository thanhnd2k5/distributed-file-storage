import asyncio
import errno
import hashlib
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from time import sleep
from urllib.parse import quote
from uuid import UUID, uuid4

import grpc
import pytest
from sqlalchemy import delete, event, select
from sqlalchemy.exc import SQLAlchemyError
from test_metadata_upload import C, start_http, wait_until
from test_metadata_upload import cluster as production_cluster

from metadata import download as downloads
from metadata.download_temp import DownloadTemp, initialize_download_temp
from metadata.models import Chunk, ChunkReplica, File, StorageNode
from metadata.operations import DataOperation

cluster = production_cluster
PREFIX = "/api/v1/files"


@pytest.fixture
def api(cluster, tmp_path):
    settings, sessions, servers = cluster
    settings.download_temp_dir = tmp_path / "downloads"
    app, client = start_http(cluster)
    try:
        yield client, app, sessions, servers, settings
    finally:
        client.__exit__(None, None, None)


def upload(api, data=b"payload", name="báo cáo '?.bin"):
    response = api[0].post(PREFIX, files={"file": (name, data, "application/test")})
    assert response.status_code == 201, response.text
    return UUID(response.json()["file_id"])


def chunks_for(api, file_id):
    with api[2]() as session:
        return session.scalars(
            select(Chunk).where(Chunk.file_id == file_id).order_by(Chunk.chunk_index)
        ).all()


def copies_for(api, chunk_id):
    with api[2]() as session:
        return session.scalars(
            select(ChunkReplica)
            .where(ChunkReplica.chunk_id == chunk_id)
            .order_by(ChunkReplica.node_id)
        ).all()


def get(api, file_id):
    return api[0].get(f"{PREFIX}/{file_id}/download")


def assert_clean(api):
    assert list(api[4].download_temp_dir.iterdir()) == []
    assert not api[1].state.operation_lock.locked()
    assert api[1].state.storage_client._active_calls == 0


@pytest.mark.parametrize("size", [0, 1, C, C + 5, 2 * C, 2 * C + 7])
def test_download_verifies_body_headers_unicode_and_observations(api, size):
    data = (b"abcd" * ((size + 3) // 4))[:size]
    file_id = upload(api, data, "../báo cáo '?.bin")
    response = get(api, file_id)
    assert response.status_code == 200, response.text
    assert response.content == data
    assert response.headers["content-type"] == "application/octet-stream"
    assert response.headers["content-length"] == str(size)
    assert response.headers["x-file-checksum-sha256"] == hashlib.sha256(data).hexdigest()
    assert response.headers["content-disposition"] == "attachment; filename*=UTF-8''" + quote(
        "báo cáo '?.bin", safe=""
    )
    assert "../" not in response.headers["content-disposition"]
    with api[2]() as session:
        assert session.get(File, file_id).status == "AVAILABLE"
        for chunk in chunks_for(api, file_id):
            assert all(r.last_verified_at is not None for r in copies_for(api, chunk.id))
    assert_clean(api)


def test_legacy_file_chunk_and_size_snapshot_survives_smaller_current_settings(api):
    settings = api[4]
    settings.chunk_size_bytes = 4 * 1024 * 1024
    settings.max_file_size_bytes = 8 * 1024 * 1024
    data = b"z" * (4 * 1024 * 1024 + 1)
    file_id = upload(api, data)
    settings.chunk_size_bytes = C
    settings.max_file_size_bytes = C
    response = get(api, file_id)
    assert response.status_code == 200 and response.content == data
    assert_clean(api)


@pytest.mark.parametrize(
    "kind,code,status",
    [
        ("UPLOADING", "FILE_NOT_READY", 409),
        ("FAILED", "FILE_NOT_READY", 409),
        ("DELETING", "FILE_DELETING", 409),
        ("DELETED", "FILE_NOT_FOUND", 404),
        ("missing", "FILE_NOT_FOUND", 404),
        ("invalid", "VALIDATION_ERROR", 422),
    ],
)
def test_not_downloadable_status_uuid_and_missing(api, kind, code, status):
    file_id = uuid4() if kind in {"missing", "invalid"} else upload(api)
    if kind not in {"missing", "invalid"}:
        with api[2].begin() as session:
            session.get(File, file_id).status = kind
    response = get(api, "bad-uuid" if kind == "invalid" else file_id)
    assert response.status_code == status and response.json()["error"]["code"] == code
    assert_clean(api)


@pytest.mark.parametrize(
    "mode", ["missing", "data_loss", "bytes", "hash", "id", "size", "timeout", "unavailable"]
)
def test_fallback_records_get_outcome_without_changing_file_or_node_health(api, monkeypatch, mode):
    file_id = upload(api)
    chunk = chunks_for(api, file_id)[0]
    first = api[3][0].service
    original = first.get_handler
    calls = []
    if mode == "missing":
        first.store.chunk_path(str(chunk.id)).unlink()

    def fault(request, context):
        calls.append(request.chunk_id)
        if mode == "data_loss":
            context.abort(grpc.StatusCode.DATA_LOSS, "injected unreadable data")
        if mode == "unavailable":
            context.abort(grpc.StatusCode.UNAVAILABLE, "injected unavailable")
        response = original(request, context)
        if mode == "bytes":
            response.data = b"x" * len(response.data)
        elif mode == "hash":
            response.checksum_sha256 = "0" * 64
        elif mode == "id":
            response.chunk_id = str(uuid4())
        elif mode == "size":
            response.data += b"x"
        elif mode == "timeout":
            while context.is_active():
                sleep(0.005)
        return response

    monkeypatch.setattr(first, "get_handler", fault)
    api[1].state.storage_client._timeout = 0.1
    response = get(api, file_id)
    assert response.status_code == 200 and response.content == b"payload"
    rows = copies_for(api, chunk.id)
    assert rows[0].status == (
        "MISSING"
        if mode == "missing"
        else "VERIFIED"
        if mode in {"timeout", "unavailable"}
        else "CORRUPTED"
    )
    assert rows[0].last_error is not None and not rows[0].cleanup_pending
    assert rows[1].status == "VERIFIED" and rows[1].last_error is None
    assert len(calls) == (2 if mode in {"timeout", "unavailable"} else 1)
    with api[2]() as session:
        assert session.get(StorageNode, "node-1").status == "ACTIVE"
        assert session.get(File, file_id).status == "AVAILABLE"
    assert_clean(api)


@pytest.mark.parametrize("old_status", ["PENDING", "MISSING", "CORRUPTED"])
def test_nonverified_mapping_is_probed_and_promoted(api, old_status):
    file_id = upload(api)
    chunk = chunks_for(api, file_id)[0]
    with api[2].begin() as session:
        session.get(ChunkReplica, (chunk.id, "node-1")).status = old_status
        session.get(StorageNode, "node-2").enabled = False
    assert get(api, file_id).content == b"payload"
    replica = copies_for(api, chunk.id)[0]
    assert replica.status == "VERIFIED" and replica.last_verified_at is not None
    assert replica.last_error is None and not replica.cleanup_pending
    assert_clean(api)


@pytest.mark.parametrize("excluded", ["disabled", "unconfigured", "deleted", "cleanup"])
def test_excluded_replica_is_never_called(api, monkeypatch, excluded):
    file_id = upload(api)
    chunk = chunks_for(api, file_id)[0]
    with api[2].begin() as session:
        if excluded == "disabled":
            session.get(StorageNode, "node-1").enabled = False
        elif excluded == "deleted":
            session.get(ChunkReplica, (chunk.id, "node-1")).status = "DELETED"
        elif excluded == "cleanup":
            session.get(ChunkReplica, (chunk.id, "node-1")).cleanup_pending = True
    if excluded == "unconfigured":
        api[4].storage_nodes_json = [
            node for node in api[4].storage_nodes_json if node.node_id != "node-1"
        ]

    def forbidden(*args):
        pytest.fail("Excluded source received Get")

    monkeypatch.setattr(api[3][0].service, "get_handler", forbidden)
    response = get(api, file_id)
    assert response.status_code == 200 and response.content == b"payload"
    assert_clean(api)


def test_suspected_before_down_and_down_is_usable_fallback(api, monkeypatch):
    file_id = upload(api)
    api[1].state.health_worker.stop()
    with api[2].begin() as session:
        session.get(StorageNode, "node-1").status = "DOWN"
        session.get(StorageNode, "node-2").status = "SUSPECTED"
    order = []
    for number in (0, 1):
        service = api[3][number].service
        original = service.get_handler

        def observe(request, context, number=number, original=original):
            order.append(number + 1)
            if number == 1:
                context.abort(grpc.StatusCode.NOT_FOUND, "injected missing")
            return original(request, context)

        monkeypatch.setattr(service, "get_handler", observe)
    assert get(api, file_id).content == b"payload"
    assert order == [2, 1]
    assert_clean(api)


def test_last_chunk_unavailable_returns_json_before_any_binary_response(api):
    file_id = upload(api, b"x" * (2 * C + 7))
    chunks = chunks_for(api, file_id)
    for replica in copies_for(api, chunks[-1].id):
        api[3][int(replica.node_id[-1]) - 1].service.store.chunk_path(str(chunks[-1].id)).unlink()
    response = get(api, file_id)
    assert response.status_code == 503 and response.headers["content-type"] == "application/json"
    assert response.json()["error"]["code"] == "CHUNK_UNAVAILABLE"
    assert response.json()["error"]["details"] == {"file_id": str(file_id), "chunk_index": 2}
    assert all(r.status == "MISSING" for r in copies_for(api, chunks[-1].id))
    with api[2]() as session:
        assert session.get(File, file_id).status == "AVAILABLE"
    assert_clean(api)


@pytest.mark.parametrize(
    "mutation", ["file_hash", "missing_chunk", "index", "size", "total", "invalid_hash"]
)
def test_metadata_or_full_file_integrity_failure_has_no_partial_200(api, monkeypatch, mutation):
    file_id = upload(api, b"x" * (C + 5))
    chunks = chunks_for(api, file_id)
    with api[2].begin() as session:
        if mutation == "file_hash":
            session.get(File, file_id).checksum_sha256 = "0" * 64
        elif mutation == "missing_chunk":
            session.execute(delete(ChunkReplica).where(ChunkReplica.chunk_id == chunks[-1].id))
            session.delete(session.get(Chunk, chunks[-1].id))
        elif mutation == "index":
            session.get(Chunk, chunks[-1].id).chunk_index = 2
        elif mutation == "size":
            session.get(Chunk, chunks[-1].id).size_bytes += 1
        elif mutation == "total":
            session.get(File, file_id).total_chunks += 1
        else:
            session.get(File, file_id).checksum_sha256 = "X" * 64
    if mutation != "file_hash":

        def forbidden(*args, **kwargs):
            pytest.fail("Invalid metadata reached data RPC")

        monkeypatch.setattr(DataOperation, "get_chunk", forbidden)
    response = get(api, file_id)
    assert (
        response.status_code == 503 and response.json()["error"]["code"] == "INTEGRITY_CHECK_FAILED"
    )
    assert_clean(api)


class FileProxy:
    def __init__(self, file, mode):
        self.inner, self.mode = file, mode
        self.read_sizes = []

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def write(self, data):
        if self.mode == "write":
            raise OSError(errno.ENOSPC, "injected disk full")
        if self.mode == "short_write":
            return self.inner.write(data[:-1])
        return self.inner.write(data)

    def flush(self):
        if self.mode == "flush":
            raise OSError("injected flush error")
        return self.inner.flush()

    def read(self, size):
        self.read_sizes.append(size)
        if self.mode == "read":
            raise OSError("injected read error")
        return self.inner.read(size)


def track_temps(monkeypatch, mode="normal"):
    temps = []

    def create(directory):
        temp = DownloadTemp(directory)
        temp.file = FileProxy(temp.file, mode)
        temps.append(temp)
        return temp

    monkeypatch.setattr(downloads, "DownloadTemp", create)
    return temps


@pytest.mark.parametrize("mode", ["create", "write", "short_write", "flush", "read"])
def test_temp_disk_faults_cleanup_and_return_503(api, monkeypatch, mode):
    file_id = upload(api)
    if mode == "create":

        def fail_create(directory):
            raise OSError(errno.ENOSPC, "injected full disk")

        monkeypatch.setattr(downloads, "DownloadTemp", fail_create)
    else:
        track_temps(monkeypatch, mode)
    response = get(api, file_id)
    assert (
        response.status_code == 503
        and response.json()["error"]["code"] == "TEMP_STORAGE_UNAVAILABLE"
    )
    assert_clean(api)


@pytest.mark.parametrize("after_get", [False, True])
def test_database_failure_does_not_fail_file_or_leak_tmp(api, monkeypatch, after_get):
    file_id = upload(api)
    api[1].state.health_worker.stop()
    engine = api[2].kw["bind"]
    armed = Event()
    if not after_get:
        armed.set()
    else:
        original = DataOperation.get_chunk

        def get_then_break(self, *args, **kwargs):
            result = original(self, *args, **kwargs)
            armed.set()
            return result

        monkeypatch.setattr(DataOperation, "get_chunk", get_then_break)

    def unavailable(*args):
        if armed.is_set():
            raise SQLAlchemyError("private DB diagnostics")

    event.listen(engine, "before_cursor_execute", unavailable)
    try:
        response = get(api, file_id)
        assert (
            response.status_code == 503
            and response.json()["error"]["code"] == "METADATA_UNAVAILABLE"
        )
        assert "private" not in response.text
    finally:
        event.remove(engine, "before_cursor_execute", unavailable)
    with api[2]() as session:
        assert session.get(File, file_id).status == "AVAILABLE"
    assert_clean(api)


def test_get_rpc_has_no_open_db_transaction_and_busy_http_keeps_health_reads(api, monkeypatch):
    file_id = upload(api)
    api[1].state.storage_client._timeout = 4
    entered, release = Event(), Event()
    service = api[3][0].service
    original = service.get_handler

    def block(request, context):
        entered.set()
        assert release.wait(3)
        return original(request, context)

    monkeypatch.setattr(service, "get_handler", block)
    rpc_checks = []
    original_client_get = DataOperation.get_chunk

    def check_scope(self, *args, **kwargs):
        assert not self._in_transaction
        rpc_checks.append(True)
        return original_client_get(self, *args, **kwargs)

    monkeypatch.setattr(DataOperation, "get_chunk", check_scope)
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(get, api, file_id)
        try:
            assert entered.wait(2)
            assert get(api, file_id).status_code == 409
            assert api[0].post(PREFIX, files={"file": ("other", b"x")}).status_code == 409
            assert api[0].get(f"{PREFIX}/{file_id}").status_code == 200
            before = api[0].get("/api/v1/nodes").json()["items"][0]["last_success_at"]
            assert api[0].get("/api/v1/health/ready").status_code == 200
            assert api[0].get("/api/v1/cluster").json()["operation_busy"]
            wait_until(
                lambda: api[0].get("/api/v1/nodes").json()["items"][0]["last_success_at"] != before
            )
        finally:
            release.set()
        assert pending.result(timeout=4).content == b"payload"
    assert rpc_checks
    assert_clean(api)


async def asgi_download(api, file_id, *, mode="success", entered=None, spec="2.4", on_headers=None):
    app = api[1]
    incoming = asyncio.Queue()
    incoming.put_nowait({"type": "http.request", "body": b"", "more_body": False})
    sent = []
    body_sent = Event()
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": spec},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": f"{PREFIX}/{file_id}/download",
        "query_string": b"",
        "headers": [],
        "client": ("127.0.0.1", 1),
        "server": ("test", 80),
    }

    async def send(message):
        sent.append(message)
        if message["type"] == "http.response.start" and message["status"] == 200:
            assert not app.state.operation_lock.locked()
            if on_headers is not None:
                await asyncio.to_thread(on_headers)
            if mode == "send_headers_error":
                raise OSError("injected closed socket")
        if message["type"] == "http.response.body" and message.get("body"):
            body_sent.set()
            if mode == "send_body_error":
                raise OSError("injected mid-body socket failure")
            if mode in {"send_disconnect", "send_cancel"}:
                await asyncio.Event().wait()

    task = asyncio.create_task(app(scope, incoming.get, send))
    if entered is not None:
        assert await asyncio.to_thread(entered.wait, 2)
        if mode == "prepare_cancel":
            task.cancel()
        else:
            incoming.put_nowait({"type": "http.disconnect"})
    if mode in {"send_disconnect", "send_cancel"}:
        assert await asyncio.to_thread(body_sent.wait, 3)
        if mode == "send_cancel":
            task.cancel()
        else:
            incoming.put_nowait({"type": "http.disconnect"})
    try:
        await asyncio.wait_for(task, 5)
    except asyncio.CancelledError:
        assert mode.endswith("cancel")
        if mode == "read_cancel":
            raise
    except (OSError, ExceptionGroup):
        assert mode in {"send_headers_error", "send_body_error"}
    return sent


@pytest.mark.parametrize(
    "mode", ["success", "send_headers_error", "send_body_error", "send_disconnect", "send_cancel"]
)
@pytest.mark.parametrize("spec", ["2.0", "2.4"])
def test_response_finalizer_bounded_disk_reads_covers_asgi_outcomes(api, monkeypatch, mode, spec):
    data = b"d" * (C + 7)
    file_id = upload(api, data)
    temps = track_temps(monkeypatch)
    messages = api[0].portal.call(lambda: asgi_download(api, file_id, mode=mode, spec=spec))
    assert messages[0]["type"] == "http.response.start" and messages[0]["status"] == 200
    assert temps[0].file.closed and not temps[0].path.exists()
    assert all(0 < size <= C for size in temps[0].file.read_sizes)
    if mode == "success":
        assert b"".join(m.get("body", b"") for m in messages) == data
    assert_clean(api)


@pytest.mark.parametrize("mode", ["prepare_disconnect", "prepare_cancel"])
def test_cancellation_during_get_drains_and_keeps_file_available(api, monkeypatch, mode):
    file_id = upload(api)
    entered = Event()
    service = api[3][0].service
    original = service.get_handler

    def block(request, context):
        response = original(request, context)
        entered.set()
        while context.is_active():
            sleep(0.005)
        return response

    monkeypatch.setattr(service, "get_handler", block)
    temps = track_temps(monkeypatch)
    messages = api[0].portal.call(lambda: asgi_download(api, file_id, entered=entered, mode=mode))
    assert all(m.get("status") != 200 for m in messages)
    assert temps[0].file.closed
    with api[2]() as session:
        assert session.get(File, file_id).status == "AVAILABLE"
    assert_clean(api)


def test_startup_janitor_only_unlinks_exact_regular_managed_names(tmp_path):
    managed = tmp_path / f".download-{uuid4().hex}.tmp"
    managed.write_bytes(b"stale partial download")
    foreign = tmp_path / "notes.txt"
    foreign.write_bytes(b"keep")
    wrong = tmp_path / ".download-invalid.tmp"
    wrong.write_bytes(b"keep")
    nested = tmp_path / f".download-{uuid4().hex}.tmp"
    nested.mkdir()
    (nested / "keep").write_bytes(b"keep")
    linked = tmp_path / f".download-{uuid4().hex}.tmp"
    linked.symlink_to(foreign)
    initialize_download_temp(tmp_path)
    assert not managed.exists()
    assert foreign.read_bytes() == wrong.read_bytes() == b"keep"
    assert linked.is_symlink() and (nested / "keep").read_bytes() == b"keep"


def test_bad_download_directory_gates_startup_and_preserves_live(cluster, tmp_path):
    from fastapi.testclient import TestClient

    from metadata.main import create_app

    settings = cluster[0]
    blocked = tmp_path / "not-a-directory"
    blocked.write_bytes(b"keep")
    settings.download_temp_dir = blocked
    with TestClient(create_app(settings)) as client:
        assert client.get("/api/v1/health/live").status_code == 200
        assert client.get("/api/v1/health/ready").status_code == 503
        assert client.get(f"{PREFIX}/{uuid4()}/download").status_code == 503
    assert blocked.read_bytes() == b"keep"


def test_stream_owns_disk_snapshot_and_allows_new_upload_after_prepare(api):
    data = b"s" * (C + 7)
    file_id = upload(api, data)
    chunks = chunks_for(api, file_id)

    def after_prepare():
        for chunk in chunks:
            for replica in copies_for(api, chunk.id):
                api[3][int(replica.node_id[-1]) - 1].service.store.chunk_path(
                    str(chunk.id)
                ).unlink()
        with api[2].begin() as session:
            session.get(File, file_id).status = "DELETING"
        # The old response is still sending. A fresh data operation is admitted.
        response = api[0].post(PREFIX, files={"file": ("new-empty", b"")})
        assert response.status_code == 201, response.text

    messages = api[0].portal.call(lambda: asgi_download(api, file_id, on_headers=after_prepare))
    assert b"".join(m.get("body", b"") for m in messages) == data
    assert_clean(api)


def test_cancel_during_threaded_response_read_drains_before_closing_temp(api, monkeypatch):
    file_id = upload(api)
    temps = track_temps(monkeypatch)
    entered, release = Event(), Event()
    original_read = FileProxy.read

    def block_response_read(self, size):
        if size == 64 * 1024:
            entered.set()
            assert release.wait(3)
        return original_read(self, size)

    monkeypatch.setattr(FileProxy, "read", block_response_read)

    async def cancel_reader():
        task = asyncio.create_task(asgi_download(api, file_id, mode="read_cancel"))
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        # Allow cancellation to reach the response's shielded/draining await.
        await asyncio.sleep(0.05)
        try:
            assert not task.done()
            assert not temps[0].file.closed and temps[0].path.exists()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    api[0].portal.call(cancel_reader)
    assert temps[0].file.closed
    assert_clean(api)


def test_real_corrupted_storage_bytes_fallback_records_corrupted(api):
    file_id = upload(api)
    chunk = chunks_for(api, file_id)[0]
    api[3][0].service.store.chunk_path(str(chunk.id)).write_bytes(b"broken")
    response = get(api, file_id)
    assert response.status_code == 200 and response.content == b"payload"
    assert copies_for(api, chunk.id)[0].status == "CORRUPTED"
    assert_clean(api)


def test_legacy_metadata_filename_is_normalized_before_http_header(api):
    file_id = upload(api)
    with api[2].begin() as session:
        session.get(File, file_id).original_name = "../folder\\安全\r\n?.txt"
    response = get(api, file_id)
    assert response.status_code == 200
    assert response.headers["content-disposition"] == "attachment; filename*=UTF-8''" + quote(
        "安全?.txt", safe=""
    )
    assert_clean(api)


def test_app_startup_cleans_stale_managed_download_and_keeps_foreign_file(cluster, tmp_path):
    settings = cluster[0]
    directory = tmp_path / "downloads"
    directory.mkdir()
    stale = directory / f".download-{uuid4().hex}.tmp"
    stale.write_bytes(b"partial snapshot")
    foreign = directory / "keep.txt"
    foreign.write_bytes(b"keep")
    settings.download_temp_dir = directory
    _, client = start_http(cluster)
    try:
        assert not stale.exists() and foreign.read_bytes() == b"keep"
        assert client.get("/api/v1/health/ready").status_code == 200
    finally:
        client.__exit__(None, None, None)
