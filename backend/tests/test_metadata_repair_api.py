import asyncio
from contextlib import contextmanager
from threading import Event
from uuid import UUID, uuid4

import grpc
import pytest
from health_control import pause_background
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from test_metadata_delete import PREFIX, snapshot, upload
from test_metadata_delete import api as production_api
from test_metadata_delete import cluster as production_cluster
from test_metadata_upload import C

from metadata import repair as repairs
from metadata.models import ChunkReplica, File, StorageNode
from metadata.operations import DataOperation
from metadata.storage_client import StorageRpcError

api = production_api
cluster = production_cluster
ENDPOINT = "/api/v1/admin/repair"


def repair(api, **kwargs):
    return api[0].post(ENDPOINT, json=kwargs)


@pytest.mark.parametrize(
    "body",
    [
        {"max_chunks": 0},
        {"max_chunks": 9},
        {"max_chunks": True},
        {"max_chunks": "2"},
        {"file_id": "wrong"},
        {"node_id": "?"},
        {"node_id": "absent"},
        {"after": {"file_id": "bad", "chunk_index": 0}},
        {"after": {"file_id": str(uuid4()), "chunk_index": -1}},
        {"extra": "forbidden"},
    ],
)
def test_repair_validation_envelope(api, body):
    response = api[0].post(ENDPOINT, json=body)
    assert response.status_code == 422 and response.json()["error"]["code"] == "VALIDATION_ERROR"
    assert not api[1].state.operation_lock.locked()


@pytest.mark.parametrize("status", ["missing", "UPLOADING", "FAILED", "DELETING", "DELETED"])
def test_repair_explicit_scope_status_and_missing(api, status):
    file_id = upload(api)
    if status == "missing":
        file_id = uuid4()
    else:
        with api[2].begin() as session:
            session.get(File, file_id).status = status
    response = repair(api, file_id=str(file_id))
    assert response.status_code == (404 if status == "missing" else 409)
    assert all(
        server.service.store.used_bytes > 0 if i < 2 else True for i, server in enumerate(api[3])
    )


def test_repair_scope_empty_and_zero_byte_file(api):
    response = repair(api)
    assert response.status_code == 200
    expected = {
        "checked_chunks": 0,
        "repaired_replicas": 0,
        "remaining_chunks": 0,
        "next_after": None,
        "results": [],
    }
    assert response.json() == expected
    file_id = upload(api, b"")
    assert repair(api, file_id=str(file_id)).json() == expected


def test_repair_cursor_orders_uuid_then_index_and_filters(api):
    first, second = upload(api, b"a" * (3 * C)), upload(api, b"b" * (2 * C))
    expected = [
        (str(file_id), index)
        for file_id in sorted([first, second])
        for index in range(3 if file_id == first else 2)
    ]
    got, cursor = [], None
    for page in range(3):
        response = repair(api, max_chunks=2, after=cursor)
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["checked_chunks"] == len(body["results"])
        got.extend((r["file_id"], r["chunk_index"]) for r in body["results"])
        assert body["remaining_chunks"] == 5 - len(got) and body["repaired_replicas"] == 0
        cursor = body["next_after"]
    assert got == expected and cursor is None
    known = snapshot(api, first)[2][0].node_id
    body = repair(api, file_id=str(first), node_id=known).json()
    with api[2]() as session:
        ids = set(
            session.scalars(select(ChunkReplica.chunk_id).where(ChunkReplica.node_id == known))
        )
    assert {UUID(r["chunk_id"]) for r in body["results"]} == {
        c.id for c in snapshot(api, first)[1] if c.id in ids
    }
    assert all(r["file_id"] == str(first) for r in body["results"])


def test_repair_cursor_deleted_file_and_nonexistent_cursor_row(api):
    first, second = upload(api, b"a" * (2 * C)), upload(api)
    low, high = sorted([first, second])
    page = repair(api, max_chunks=1).json()
    assert page["next_after"]["file_id"] == str(low)
    assert api[0].delete(f"{PREFIX}/{low}").status_code == 200
    page = repair(api, after=page["next_after"]).json()
    assert all(r["file_id"] == str(high) for r in page["results"])
    assert page["remaining_chunks"] == 0 and page["next_after"] is None
    body = repair(api, after={"file_id": str(UUID(int=0)), "chunk_index": 123}).json()
    assert body["checked_chunks"] == len(snapshot(api, high)[1])


def test_repair_scoped_node_probes_old_volume_and_can_copy_to_other_node(api):
    file_id = upload(api)
    pause_background(api[1].state.health_worker)
    replica = snapshot(api, file_id)[2][0]
    api[3][int(replica.node_id[-1]) - 1].service.store.delete(str(replica.chunk_id))
    body = repair(api, file_id=str(file_id), node_id=replica.node_id).json()
    assert body["checked_chunks"] == 1 and body["repaired_replicas"] == 1
    assert body["results"][0]["outcome"] == "REPAIRED"
    assert body["results"][0]["live_replica_count"] == 2
    assert api[0].get(f"{PREFIX}/{file_id}/download").content == b"delete payload"


def test_repair_disabled_node_filter_still_selects_known_chunks(api):
    file_id = upload(api)
    pause_background(api[1].state.health_worker)
    replica = snapshot(api, file_id)[2][0]
    with api[2].begin() as session:
        session.get(StorageNode, replica.node_id).enabled = False
    body = repair(api, node_id=replica.node_id).json()
    assert body["checked_chunks"] == 1 and body["repaired_replicas"] == 1
    assert len(snapshot(api, file_id)[2]) == 3


@pytest.mark.parametrize("zero", [False, True])
def test_repair_budget_stops_before_new_chunk_and_preserves_cursor(api, monkeypatch, zero):
    file_id = upload(api, b"budget" * C)  # Six chunks.
    clock = [0.0]
    monkeypatch.setattr(repairs, "monotonic", lambda: clock[0])
    original = repairs.repair_chunk

    def advance(*args):
        result = original(*args)
        clock[0] += api[4].repair_time_budget_seconds
        return result

    monkeypatch.setattr(repairs, "repair_chunk", advance)
    scan = api[1].state.health_worker.cleanup_pass
    original_cleanup = scan.run

    def cleanup(*args, **kwargs):
        original_cleanup(*args, **kwargs)
        if zero:
            clock[0] += api[4].repair_time_budget_seconds

    monkeypatch.setattr(scan, "run", cleanup)
    after = {"file_id": str(file_id), "chunk_index": 0}
    body = repair(api, file_id=str(file_id), after=after).json()
    assert body["checked_chunks"] == (0 if zero else 1)
    assert body["remaining_chunks"] == (5 if zero else 4)
    assert body["next_after"] == {"file_id": str(file_id), "chunk_index": 0 if zero else 1}
    if zero:
        body = repair(api, file_id=str(file_id)).json()
        assert (
            body["checked_chunks"] == 0
            and body["remaining_chunks"] == 6
            and body["next_after"] is None
        )


def test_repair_error_result_advances_cursor_and_remaining_is_unscanned(api, monkeypatch):
    file_id = upload(api, b"e" * (3 * C))
    original = repairs._repair_chunk

    def error(operation, settings, chunk, *args):
        if chunk.index == 0:
            raise RuntimeError("private server diagnostics")
        return original(operation, settings, chunk, *args)

    monkeypatch.setattr(repairs, "_repair_chunk", error)
    response = repair(api, file_id=str(file_id), max_chunks=1)
    assert response.status_code == 200
    body = response.json()
    assert body["results"][0]["outcome"] == "ERROR" and body["remaining_chunks"] == 2
    assert body["next_after"]["chunk_index"] == 0 and "private" not in response.text
    body = repair(api, file_id=str(file_id), after=body["next_after"]).json()
    assert [r["chunk_index"] for r in body["results"]] == [1, 2]
    assert body["next_after"] is None


def test_repair_cleanup_has_priority_without_nested_lock_or_touching_available(api, monkeypatch):
    inactive, available = upload(api), upload(api)
    with api[2].begin() as session:
        session.get(File, inactive).status = "FAILED"
        for replica in snapshot(api, inactive)[2]:
            session.get(ChunkReplica, (replica.chunk_id, replica.node_id)).cleanup_pending = True
    original = repairs.repair_chunk

    def observe(*args):
        file, _, replicas = snapshot(api, inactive)
        assert file.status == "FAILED" and all(r.status == "DELETED" for r in replicas)
        assert all(not r.cleanup_pending for r in snapshot(api, available)[2])
        return original(*args)

    monkeypatch.setattr(repairs, "repair_chunk", observe)
    body = repair(api).json()
    assert body["checked_chunks"] == 1 and body["results"][0]["file_id"] == str(available)


def test_repair_busy_readiness_and_configured_limit(api):
    upload(api)
    api[4].repair_max_chunks = 2
    assert repair(api, max_chunks=3).status_code == 422
    api[1].state.operation_lock.acquire()
    try:
        response = repair(api, max_chunks=1)
        assert response.status_code == 409 and response.json()["error"]["code"] == "OPERATION_BUSY"
        assert api[0].get(PREFIX).status_code == 200
        assert api[0].get("/api/v1/health/ready").status_code == 200
    finally:
        api[1].state.operation_lock.release()
    api[1].state.initialized = False
    assert repair(api, max_chunks=1).status_code == 503
    api[1].state.initialized = True


def test_repair_database_outage_is_503_not_chunk_error_summary(api, monkeypatch):
    file_id = upload(api)

    @contextmanager
    def fail(operation):
        raise SQLAlchemyError("private database diagnostics")
        yield

    monkeypatch.setattr(DataOperation, "transaction", fail)
    response = repair(api, file_id=str(file_id))
    assert (
        response.status_code == 503 and response.json()["error"]["code"] == "METADATA_UNAVAILABLE"
    )
    assert "private" not in response.text and not api[1].state.operation_lock.locked()


def test_repair_partial_ack_without_full_rf_is_no_destination_with_exact_count(api):
    file_id = upload(api)
    pause_background(api[1].state.health_worker)
    old = snapshot(api, file_id)[2][0]
    with api[2].begin() as session:
        session.get(File, file_id).replication_factor = 3
        session.get(StorageNode, old.node_id).status = "DOWN"
    response = repair(api, file_id=str(file_id))
    assert response.status_code == 200
    body = response.json()
    assert body["repaired_replicas"] == 1 and body["checked_chunks"] == 1
    assert body["results"][0]["outcome"] == "NO_DESTINATION"
    assert body["results"][0]["live_replica_count"] == 2


def test_repair_unexpected_error_after_partial_ack_keeps_confirmed_count(api, monkeypatch, caplog):
    file_id = upload(api)
    pause_background(api[1].state.health_worker)
    old = snapshot(api, file_id)[2][0]
    api[3][int(old.node_id[-1]) - 1].service.store.delete(str(old.chunk_id))
    with api[2].begin() as session:
        session.get(File, file_id).replication_factor = 3
    original = DataOperation.store_chunk
    acks = []

    def fail_second(operation, *args, **kwargs):
        if acks:
            raise RuntimeError("injected failure after first confirmed replica")
        ack = original(operation, *args, **kwargs)
        acks.append(ack)
        return ack

    monkeypatch.setattr(DataOperation, "store_chunk", fail_second)
    response = repair(api, file_id=str(file_id))
    assert response.status_code == 200
    body = response.json()
    assert len(acks) == 1 and body["repaired_replicas"] == 1
    assert body["results"][0]["outcome"] == "ERROR"
    assert body["results"][0]["live_replica_count"] == 2
    assert f"file_id={file_id}" in caplog.text
    assert f"chunk_id={old.chunk_id}" in caplog.text
    assert "node_id=node-" in caplog.text and "phase=store (RuntimeError)" in caplog.text
    assert "injected failure after first confirmed replica" not in caplog.text


def test_repair_unavailable_is_200_summary_and_default_limit_is_eight(api):
    file_id = upload(api, b"p" * (9 * C))
    pause_background(api[1].state.health_worker)
    for replica in snapshot(api, file_id)[2]:
        api[3][int(replica.node_id[-1]) - 1].service.store.delete(str(replica.chunk_id))
    response = repair(api, file_id=str(file_id))
    assert response.status_code == 200
    body = response.json()
    assert body["checked_chunks"] == 8 and body["remaining_chunks"] == 1
    assert all(r["outcome"] == "UNAVAILABLE" for r in body["results"])
    assert body["next_after"] == {"file_id": str(file_id), "chunk_index": 7}
    body = repair(api, file_id=str(file_id), after=body["next_after"]).json()
    assert body["checked_chunks"] == 1 and body["remaining_chunks"] == 0


@pytest.mark.parametrize("mode", ["disconnect", "cancel"])
def test_repair_asgi_cancel_drains_and_preserves_pending_attempt(api, monkeypatch, mode):
    file_id = upload(api)
    pause_background(api[1].state.health_worker)
    old = snapshot(api, file_id)[2][0]
    with api[2].begin() as session:
        session.get(StorageNode, old.node_id).status = "DOWN"
    entered = Event()

    def block(operation, node_id, chunk_id, data, checksum, *, cancel):
        with api[2]() as session:
            assert session.get(ChunkReplica, (UUID(chunk_id), node_id)).status == "PENDING"
        entered.set()
        assert cancel.wait(3)
        raise StorageRpcError("StoreChunk", node_id, grpc.StatusCode.CANCELLED, 1)

    monkeypatch.setattr(DataOperation, "store_chunk", block)

    async def invoke():
        import json

        incoming = asyncio.Queue()
        incoming.put_nowait(
            {
                "type": "http.request",
                "body": json.dumps({"file_id": str(file_id)}).encode(),
                "more_body": False,
            }
        )
        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.4"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": ENDPOINT,
            "query_string": b"",
            "headers": [(b"content-type", b"application/json")],
            "client": ("127.0.0.1", 1),
            "server": ("test", 80),
        }

        async def send(message):
            assert message.get("status") != 200

        task = asyncio.create_task(api[1](scope, incoming.get, send))
        assert await asyncio.to_thread(entered.wait, 2)
        if mode == "cancel":
            task.cancel()
        else:
            incoming.put_nowait({"type": "http.disconnect"})
        try:
            await asyncio.wait_for(task, 3)
        except asyncio.CancelledError:
            assert mode == "cancel"

    api[0].portal.call(invoke)
    file, _, copies = snapshot(api, file_id)
    assert file.status == "AVAILABLE" and len(copies) == 3
    assert any(r.status == "PENDING" and not r.cleanup_pending for r in copies)
    assert (
        not api[1].state.operation_lock.locked() and api[1].state.storage_client._active_calls == 0
    )
