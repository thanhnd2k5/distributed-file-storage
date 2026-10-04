import asyncio
from contextlib import contextmanager
from threading import Event
from uuid import UUID, uuid4

import grpc
import pytest
from health_control import pause_background
from sqlalchemy import select, update
from sqlalchemy.exc import SQLAlchemyError
from test_metadata_upload import C, start_http
from test_metadata_upload import cluster as production_cluster

from metadata.cleanup import CleanupPass
from metadata.delete import delete_file
from metadata.models import Chunk, ChunkReplica, File, StorageNode
from metadata.operations import DataOperation, data_operation
from metadata.storage_client import StorageRpcError

cluster = production_cluster
PREFIX = "/api/v1/files"


@pytest.fixture
def api(cluster, tmp_path):
    settings, sessions, servers = cluster
    settings.cleanup_interval_seconds = 3600
    settings.download_temp_dir = tmp_path / "downloads"
    settings.max_file_size_bytes = 10 * C
    app, http = start_http(cluster)
    try:
        yield http, app, sessions, servers, settings
    finally:
        http.__exit__(None, None, None)


def upload(api, data=b"delete payload"):
    response = api[0].post(PREFIX, files={"file": ("fixture.bin", data)})
    assert response.status_code == 201, response.text
    return UUID(response.json()["file_id"])


def snapshot(api, file_id):
    with api[2]() as session:
        return (
            session.get(File, file_id),
            session.scalars(select(Chunk).where(Chunk.file_id == file_id)).all(),
            session.scalars(
                select(ChunkReplica)
                .join(Chunk)
                .where(Chunk.file_id == file_id)
                .order_by(ChunkReplica.chunk_id, ChunkReplica.node_id)
            ).all(),
        )


def run_cleanup(api, scan=None):
    with data_operation(api[1].state) as operation:
        (scan or CleanupPass()).run(operation, api[4], Event())


@pytest.mark.parametrize("size", [0, 1, C + 1])
def test_delete_commits_before_rpc_removes_bytes_preserves_history_and_is_idempotent(
    api, monkeypatch, size
):
    http, app, _, servers, _ = api
    file_id = upload(api, b"d" * size)
    original = DataOperation.delete_chunk
    observations = []

    def observe(operation, node, chunk_id, **kwargs):
        file, _, replicas = snapshot(api, file_id)
        assert file.status == "DELETING" and file.deleted_at is not None
        assert all(r.cleanup_pending or r.status == "DELETED" for r in replicas)
        assert not operation._in_transaction
        assert not http.get(PREFIX).json()["items"]
        assert http.get(f"{PREFIX}/{file_id}/download").status_code == 409  # lock is held
        observations.append(node)
        return original(operation, node, chunk_id, **kwargs)

    monkeypatch.setattr(DataOperation, "delete_chunk", observe)
    response = http.delete(f"{PREFIX}/{file_id}")
    assert response.status_code == 200, response.text
    assert response.json() == {
        "file_id": str(file_id),
        "status": "DELETED",
        "cleanup_pending_replicas": 0,
    }
    file, chunks, replicas = snapshot(api, file_id)
    assert file.status == "DELETED" and len(observations) == len(replicas)
    assert len(chunks) == (size + C - 1) // C
    assert all(r.status == "DELETED" and not r.cleanup_pending for r in replicas)
    assert all(server.service.store.used_bytes == 0 for server in servers)
    assert http.delete(f"{PREFIX}/{file_id}").json() == response.json()
    for suffix in ("", "/chunks", "/download"):
        assert http.get(f"{PREFIX}/{file_id}{suffix}").status_code == 404
    assert not http.get(f"{PREFIX}?include_inactive=true").json()["items"]
    assert not app.state.operation_lock.locked()


@pytest.mark.parametrize("status", ["DOWN", "disabled", "endpoint"])
def test_delete_offline_disabled_changed_endpoint_remains_pending_and_blocks_reads(api, status):
    http, _, sessions, servers, _ = api
    file_id = upload(api)
    _, _, replicas = snapshot(api, file_id)
    node_id = replicas[0].node_id
    if status != "DOWN":
        with sessions.begin() as session:
            node = session.get(StorageNode, node_id)
            if status == "disabled":
                node.enabled = False
            else:
                node.port += 1
    else:
        # Hold health publishing while asserting the DOWN snapshot.
        pause_background(api[1].state.health_worker)
        with sessions.begin() as session:
            session.get(StorageNode, node_id).status = "DOWN"
    response = http.delete(f"{PREFIX}/{file_id}")
    assert response.status_code == 202 and response.json()["cleanup_pending_replicas"] == 1
    assert http.delete(f"{PREFIX}/{file_id}").json() == response.json()
    assert http.get(f"{PREFIX}/{file_id}").json()["status"] == "DELETING"
    assert http.get(f"{PREFIX}/{file_id}/download").json()["error"]["code"] == "FILE_DELETING"
    assert not http.get(PREFIX).json()["items"]
    assert len(http.get(f"{PREFIX}?include_inactive=true").json()["items"]) == 1
    assert servers[int(node_id[-1]) - 1].service.store.used_bytes > 0


@pytest.mark.parametrize(
    "state,code",
    [("UPLOADING", "FILE_NOT_READY"), ("missing", "FILE_NOT_FOUND"), ("busy", "OPERATION_BUSY")],
)
def test_delete_rejects_invalid_state_missing_and_busy(api, state, code):
    file_id = upload(api)
    if state == "missing":
        file_id = uuid4()
    elif state == "UPLOADING":
        with api[2].begin() as session:
            session.get(File, file_id).status = state
    else:
        api[1].state.operation_lock.acquire()
    try:
        response = api[0].delete(f"{PREFIX}/{file_id}")
        assert response.status_code == (404 if state == "missing" else 409)
        assert response.json()["error"]["code"] == code
    finally:
        if state == "busy":
            api[1].state.operation_lock.release()
    assert api[0].delete(f"{PREFIX}/bad-id").status_code == 422


def test_delete_failed_pending_attempts_and_zero_chunk_failed(api):
    file_id = upload(api)
    with api[2].begin() as session:
        file = session.get(File, file_id)
        file.status, file.error_code = "FAILED", "UPLOAD_INTERRUPTED"
        session.execute(update(ChunkReplica).values(status="PENDING", cleanup_pending=True))
    assert api[0].delete(f"{PREFIX}/{file_id}").status_code == 200
    assert snapshot(api, file_id)[0].error_code == "UPLOAD_INTERRUPTED"
    empty_id = upload(api, b"")
    with api[2].begin() as session:
        session.get(File, empty_id).status = "FAILED"
    assert api[0].delete(f"{PREFIX}/{empty_id}").status_code == 200


@pytest.mark.parametrize("reason", ["INVALID_DELETE_ACK", "INTERNAL", "DEADLINE_EXCEEDED"])
def test_delete_fault_keeps_pending_and_mapping_until_confirmed(api, monkeypatch, reason):
    file_id = upload(api)
    original = DataOperation.delete_chunk

    def fail(operation, node_id, chunk_id, **kwargs):
        if reason == "DEADLINE_EXCEEDED":
            original(operation, node_id, chunk_id, **kwargs)  # bytes deleted, ack lost
        raise StorageRpcError("DeleteChunk", node_id, None, 2, reason=reason)

    monkeypatch.setattr(DataOperation, "delete_chunk", fail)
    response = api[0].delete(f"{PREFIX}/{file_id}")
    assert response.status_code == 202 and response.json()["cleanup_pending_replicas"] == 2
    _, _, replicas = snapshot(api, file_id)
    assert all(
        r.cleanup_pending and r.status == "VERIFIED" and r.last_error == reason for r in replicas
    )
    monkeypatch.setattr(DataOperation, "delete_chunk", original)
    run_cleanup(api)
    assert api[0].delete(f"{PREFIX}/{file_id}").status_code == 200


@pytest.mark.parametrize("after_rpc", [False, True])
def test_delete_database_failure_before_or_after_rpc_is_recoverable(api, monkeypatch, after_rpc):
    file_id = upload(api)
    original = DataOperation.transaction
    calls = []
    rpc = DataOperation.delete_chunk

    def record(operation, *args, **kwargs):
        ack = rpc(operation, *args, **kwargs)
        calls.append(ack)
        return ack

    @contextmanager
    def fail(operation):
        if not after_rpc or calls:
            raise SQLAlchemyError("private DB diagnostics")
        with original(operation) as session:
            yield session

    monkeypatch.setattr(DataOperation, "delete_chunk", record)
    monkeypatch.setattr(DataOperation, "transaction", fail)
    response = api[0].delete(f"{PREFIX}/{file_id}")
    assert (
        response.status_code == 503 and response.json()["error"]["code"] == "METADATA_UNAVAILABLE"
    )
    file, _, replicas = snapshot(api, file_id)
    assert file.status == ("DELETING" if after_rpc else "AVAILABLE")
    assert bool(calls) == after_rpc and all(r.cleanup_pending == after_rpc for r in replicas)
    assert not api[1].state.operation_lock.locked()
    monkeypatch.setattr(DataOperation, "transaction", original)
    if after_rpc:
        run_cleanup(api)
    assert api[0].delete(f"{PREFIX}/{file_id}").status_code == 200


def test_delete_bounded_eight_replicas_per_pass(api):
    file_id = upload(api, b"b" * (5 * C))
    response = api[0].delete(f"{PREFIX}/{file_id}")
    assert response.status_code == 202 and response.json()["cleanup_pending_replicas"] == 2
    assert sum(r.status == "DELETED" for r in snapshot(api, file_id)[2]) == 8
    run_cleanup(api)
    assert api[0].delete(f"{PREFIX}/{file_id}").status_code == 200


def test_delete_cancel_after_tombstone_keeps_durable_pending(api, monkeypatch):
    file_id = upload(api)
    cancel = Event()
    original = DataOperation.delete_chunk

    def cancel_after_first(operation, *args, **kwargs):
        ack = original(operation, *args, **kwargs)
        cancel.set()
        return ack

    monkeypatch.setattr(DataOperation, "delete_chunk", cancel_after_first)
    result = delete_file(api[1].state, file_id, cancel)
    assert result.status == "DELETING" and result.cleanup_pending_replicas == 1
    assert not api[1].state.operation_lock.locked()
    monkeypatch.setattr(DataOperation, "delete_chunk", original)
    run_cleanup(api)
    assert snapshot(api, file_id)[0].status == "DELETED"


@pytest.mark.parametrize("mode", ["disconnect", "cancel", "repeated_cancel"])
def test_delete_asgi_cancellation_drains_thread_and_preserves_tombstone(api, monkeypatch, mode):
    file_id = upload(api)
    original = DataOperation.delete_chunk
    entered, cancelled, release = Event(), Event(), Event()

    def block(operation, node_id, chunk_id, *, cancel):
        assert snapshot(api, file_id)[0].status == "DELETING"
        entered.set()
        assert cancel.wait(3)
        cancelled.set()
        assert release.wait(3)
        raise StorageRpcError("DeleteChunk", node_id, grpc.StatusCode.CANCELLED, 1)

    monkeypatch.setattr(DataOperation, "delete_chunk", block)

    async def invoke():
        incoming = asyncio.Queue()
        incoming.put_nowait({"type": "http.request", "body": b"", "more_body": False})
        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.4"},
            "http_version": "1.1",
            "method": "DELETE",
            "scheme": "http",
            "path": f"{PREFIX}/{file_id}",
            "query_string": b"",
            "headers": [],
            "client": ("127.0.0.1", 1),
            "server": ("test", 80),
        }
        sent = []

        async def send(message):
            sent.append(message)

        task = asyncio.create_task(api[1](scope, incoming.get, send))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            if mode == "disconnect":
                incoming.put_nowait({"type": "http.disconnect"})
            else:
                task.cancel()
            assert await asyncio.to_thread(cancelled.wait, 2)
            assert not task.done() and api[1].state.operation_lock.locked()
            if mode == "repeated_cancel":
                task.cancel()
                await asyncio.sleep(0)
                assert not task.done()
            release.set()
            try:
                await asyncio.wait_for(task, 3)
            except asyncio.CancelledError:
                assert mode != "disconnect"
        finally:
            release.set()

    api[0].portal.call(invoke)
    file, _, replicas = snapshot(api, file_id)
    assert file.status == "DELETING" and all(r.cleanup_pending for r in replicas)
    assert not api[1].state.operation_lock.locked()
    assert api[1].state.storage_client._active_calls == 0
    monkeypatch.setattr(DataOperation, "delete_chunk", original)
    run_cleanup(api)
    assert snapshot(api, file_id)[0].status == "DELETED"
