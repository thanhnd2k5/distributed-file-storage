from concurrent.futures import ThreadPoolExecutor
from threading import Event
from time import sleep

import grpc
import pytest
from fastapi.testclient import TestClient
from health_control import pause_background
from sqlalchemy import select, update
from sqlalchemy.exc import SQLAlchemyError
from test_metadata_delete import PREFIX, run_cleanup, snapshot, upload
from test_metadata_upload import C, start_http, wait_until
from test_metadata_upload import cluster as production_cluster

from metadata.cleanup import CleanupPass
from metadata.main import create_app
from metadata.models import ChunkReplica, File, StorageNode
from metadata.operations import DataOperation
from metadata.storage_client import StorageRpcError

cluster = production_cluster


@pytest.fixture
def api(cluster, tmp_path):
    settings, sessions, servers = cluster
    settings.cleanup_interval_seconds = 0.03
    settings.chunk_rpc_timeout_seconds = 0.5
    settings.max_file_size_bytes = 10 * C
    settings.download_temp_dir = tmp_path / "downloads"
    app, http = start_http(cluster)
    result = [http, app, sessions, servers, settings]
    try:
        yield result
    finally:
        result[0].__exit__(None, None, None)


def mark_failed(api, file_id):
    with api[2].begin() as session:
        file = session.get(File, file_id)
        file.status, file.error_code = "FAILED", "UPLOAD_INTERRUPTED"
        session.execute(update(ChunkReplica).values(cleanup_pending=True))


def wait_deleted(api, file_id):
    wait_until(lambda: snapshot(api, file_id)[0].status == "DELETED")


def test_background_failed_cleanup_preserves_error_and_available_pending(api):
    file_id = upload(api)
    with api[2].begin() as session:
        session.execute(update(ChunkReplica).values(status="PENDING"))
    sleep(0.1)
    assert all(not r.cleanup_pending and r.status == "PENDING" for r in snapshot(api, file_id)[2])
    assert sum(server.service.store.used_bytes for server in api[3]) > 0
    mark_failed(api, file_id)
    wait_until(lambda: all(r.status == "DELETED" for r in snapshot(api, file_id)[2]))
    file, _, replicas = snapshot(api, file_id)
    assert file.status == "FAILED" and file.error_code == "UPLOAD_INTERRUPTED"
    assert all(not r.cleanup_pending for r in replicas)
    assert sum(server.service.store.used_bytes for server in api[3]) == 0


def test_worker_skips_busy_and_disabled_reenable_resumes_cleanup(api):
    file_id = upload(api)
    node_id = snapshot(api, file_id)[2][0].node_id
    with api[2].begin() as session:
        session.get(StorageNode, node_id).enabled = False
    response = api[0].delete(f"{PREFIX}/{file_id}")
    assert response.status_code == 202
    api[1].state.operation_lock.acquire()
    try:
        api[1].state.health_worker._cleanup()  # nonblocking; no deadlock or new task
        assert snapshot(api, file_id)[0].status == "DELETING"
        assert api[0].get("/api/v1/health/ready").status_code == 200
        assert api[0].get("/api/v1/cluster").status_code == 200
    finally:
        api[1].state.operation_lock.release()
    sleep(0.12)
    assert snapshot(api, file_id)[0].status == "DELETING"
    with api[2].begin() as session:
        session.get(StorageNode, node_id).enabled = True
    wait_deleted(api, file_id)
    assert api[0].delete(f"{PREFIX}/{file_id}").status_code == 200


def test_cleanup_fairness_rotates_past_eight_failures(api, monkeypatch):
    # Manual passes make the cursor's exact progress deterministic.
    pause_background(api[1].state.health_worker)
    file_id = upload(api, b"f" * (9 * C))
    mark_failed(api, file_id)
    replicas = snapshot(api, file_id)[2]
    first = {(str(r.chunk_id), r.node_id) for r in replicas[:8]}
    original = DataOperation.delete_chunk
    seen = []

    def fail_first(operation, node_id, chunk_id, **kwargs):
        seen.append((chunk_id, node_id))
        if (chunk_id, node_id) in first:
            raise StorageRpcError("DeleteChunk", node_id, grpc.StatusCode.INTERNAL, 1)
        return original(operation, node_id, chunk_id, **kwargs)

    monkeypatch.setattr(DataOperation, "delete_chunk", fail_first)
    scan = CleanupPass()
    run_cleanup(api, scan)
    assert len(seen) == 8 and not any(r.status == "DELETED" for r in snapshot(api, file_id)[2])
    run_cleanup(api, scan)
    assert len(seen) == 16 and sum(r.status == "DELETED" for r in snapshot(api, file_id)[2]) == 8
    run_cleanup(api, scan)
    assert len(seen) == 24 and sum(r.status == "DELETED" for r in snapshot(api, file_id)[2]) == 10
    monkeypatch.setattr(DataOperation, "delete_chunk", original)
    run_cleanup(api, scan)
    assert all(r.status == "DELETED" for r in snapshot(api, file_id)[2])


def test_cleanup_transient_db_error_retries_next_interval(api, monkeypatch, caplog):
    file_id = upload(api)
    original = CleanupPass.run
    observed = Event()

    def fail_once(scan, operation, settings, cancel, **kwargs):
        if not observed.is_set():
            observed.set()
            raise SQLAlchemyError("private credentials")
        return original(scan, operation, settings, cancel, **kwargs)

    monkeypatch.setattr(CleanupPass, "run", fail_once)
    mark_failed(api, file_id)
    assert observed.wait(2)
    wait_until(lambda: all(r.status == "DELETED" for r in snapshot(api, file_id)[2]))
    assert api[1].state.health_worker.running
    assert "SQLAlchemyError" in caplog.text and "private credentials" not in caplog.text


def test_cleanup_does_not_block_health_or_queue_overlapping_passes(api, monkeypatch):
    file_id = upload(api)
    node_id = snapshot(api, file_id)[2][0].node_id
    store = api[3][int(node_id[-1]) - 1].service.store
    original = store.delete
    entered, release = Event(), Event()
    calls = []

    def block(chunk_id, is_active=lambda: True):
        calls.append(chunk_id)
        entered.set()
        assert release.wait(3)
        return original(chunk_id, is_active)

    monkeypatch.setattr(store, "delete", block)
    mark_failed(api, file_id)
    try:
        assert entered.wait(2)
        with api[2]() as session:
            before = session.get(StorageNode, node_id).last_success_at
        sleep(0.15)
        with api[2]() as session:
            assert session.get(StorageNode, node_id).last_success_at > before
        assert len(calls) == 1
        assert api[0].get("/api/v1/health/ready").status_code == 200
        assert api[0].get(PREFIX).status_code == 200
        assert api[0].delete(f"{PREFIX}/{file_id}").json()["error"]["code"] == "OPERATION_BUSY"
    finally:
        release.set()
    wait_until(lambda: all(r.status == "DELETED" for r in snapshot(api, file_id)[2]))


def test_restart_recovers_deleting_and_updating_without_dropping_history(api):
    deleting = upload(api)
    interrupted = upload(api)
    api[0].__exit__(None, None, None)
    with api[2].begin() as session:
        session.get(File, deleting).status = "DELETING"
        session.get(File, interrupted).status = "UPLOADING"
        # Bootstrap must reconstruct cleanup flags; pending work is not in memory.
        session.execute(update(ChunkReplica).values(cleanup_pending=False))
    app = create_app(api[4])
    http = TestClient(app)
    http.__enter__()
    api[0], api[1] = http, app
    wait_deleted(api, deleting)
    wait_until(lambda: all(r.status == "DELETED" for r in snapshot(api, interrupted)[2]))
    failed = snapshot(api, interrupted)[0]
    assert failed.status == "FAILED" and failed.error_code == "UPLOAD_INTERRUPTED"
    assert http.get(f"{PREFIX}/{deleting}").status_code == 404
    assert http.get(f"{PREFIX}/{interrupted}/download").status_code == 409
    assert sum(server.service.store.used_bytes for server in api[3]) == 0
    assert len(snapshot(api, deleting)[2]) == 2 and len(snapshot(api, interrupted)[2]) == 2


def test_deleting_zero_pending_finalized_by_worker_and_failed_remains_failed(api):
    file_id = upload(api, b"")
    with api[2].begin() as session:
        session.get(File, file_id).status = "DELETING"
    wait_deleted(api, file_id)


def test_fatal_cleanup_task_stops_scheduler_and_fails_ready(api, monkeypatch):
    file_id = upload(api)
    original = CleanupPass.run

    def crash(*args, **kwargs):
        raise RuntimeError("injected scheduler task failure")

    monkeypatch.setattr(CleanupPass, "run", crash)
    mark_failed(api, file_id)
    wait_until(lambda: not api[1].state.health_worker.running)
    assert api[0].get("/api/v1/health/ready").status_code == 503
    assert api[0].get("/api/v1/health/live").status_code == 200
    assert api[0].get(PREFIX).status_code == 200
    monkeypatch.setattr(CleanupPass, "run", original)
    before = snapshot(api, file_id)
    bytes_before = sum(server.service.store.used_bytes for server in api[3])
    responses = [
        api[0].post(PREFIX, files={"file": ("after-fatal.bin", b"new bytes")}),
        api[0].get(f"{PREFIX}/{file_id}/download"),
        api[0].delete(f"{PREFIX}/{file_id}"),
        api[0].post("/api/v1/admin/repair", json={"file_id": str(file_id)}),
    ]
    assert all(response.status_code == 503 for response in responses)
    assert all(response.json()["error"]["code"] == "METADATA_UNAVAILABLE" for response in responses)
    assert snapshot(api, file_id)[0].status == before[0].status == "FAILED"
    with api[2]() as session:
        assert len(session.scalars(select(File)).all()) == 1
    assert sum(server.service.store.used_bytes for server in api[3]) == bytes_before
    assert not api[1].state.operation_lock.locked()


def test_shutdown_drains_cleanup_before_clients_and_engine_close(api, monkeypatch):
    file_id = upload(api)
    entered, release = Event(), Event()
    original = DataOperation.delete_chunk

    def block(operation, *args, **kwargs):
        entered.set()
        assert release.wait(3)
        ack = original(operation, *args, **(kwargs | {"cancel": None}))
        # Accepted work can still commit after shutdown admission closes.
        with operation.transaction() as session:
            assert session.scalar(select(File.status).where(File.id == file_id)) == "FAILED"
        return ack

    monkeypatch.setattr(DataOperation, "delete_chunk", block)
    mark_failed(api, file_id)
    assert entered.wait(2)
    with ThreadPoolExecutor(max_workers=1) as executor:
        closing = executor.submit(api[0].__exit__, None, None, None)
        try:
            wait_until(lambda: not api[1].state.initialized)
            assert not closing.done() and api[1].state.storage_client.running
        finally:
            release.set()
        closing.result(timeout=3)
    worker = api[1].state.health_worker
    assert all(not t.is_alive() for t in worker._cleanup_executor._threads)
    assert not api[1].state.storage_client.running and not api[1].state.operation_lock.locked()
    assert sum(not r.cleanup_pending for r in snapshot(api, file_id)[2]) == 1
    # Fixture owns a fresh lifespan so its finalizer does not close the old portal twice.
    app = create_app(api[4])
    http = TestClient(app)
    http.__enter__()
    api[0], api[1] = http, app
    wait_until(lambda: all(r.status == "DELETED" for r in snapshot(api, file_id)[2]))
