"""M4 committed crash windows: real SIGKILL/HTTP/PostgreSQL/gRPC/disk recovery."""

import json
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event
from time import monotonic, sleep

import pytest
from sqlalchemy import select

from metadata.models import ChunkReplica, File
from scripts.smoke_files import get, payload, request, upload
from storage.server import create_server
from tests.test_metadata_files_lifecycle import (
    HARNESS,
    child_process,
    expect_error,
    line,
    release,
    wait_active,
)
from tests.test_metadata_files_lifecycle import (
    nodes as production_nodes,
)

nodes = production_nodes

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Linux SIGKILL/RPC harness")

HOOKS = """
from metadata import repair
from metadata import main
from contextlib import asynccontextmanager
create_app = main.create_app
def creating():
    app = create_app()
    lifespan = app.router.lifespan_context
    @asynccontextmanager
    async def drained(app):
        async with lifespan(app):
            yield
        print('SHUTDOWN_DRAINED', flush=True)
    app.router.lifespan_context = drained
    return app
main.create_app = creating
armed = False
fired = False
pending = repair.pending_attempt
def attempting(*args, **kwargs):
    global armed
    result = pending(*args, **kwargs)
    armed = True
    if mode == 'repair_pending':
        barrier('REPAIR_PENDING')
    return result
repair.pending_attempt = attempting
store_rpc = DataOperation.store_chunk
def storing_repair(self, *args, **kwargs):
    global fired
    result = store_rpc(self, *args, **kwargs)
    if armed and mode == 'repair_store' and not fired:
        fired = True
        barrier('REPAIR_STORE')
    return result
DataOperation.store_chunk = storing_repair
delete_rpc = DataOperation.delete_chunk
def deleting(self, *args, **kwargs):
    global fired
    if mode == 'delete_before' and not fired:
        fired = True
        barrier('DELETE_BEFORE')
    result = delete_rpc(self, *args, **kwargs)
    if not fired and (mode == 'delete_after' or (armed and mode == 'repair_delete')):
        fired = True
        barrier(mode.upper())
    return result
DataOperation.delete_chunk = deleting
"""
M4_HARNESS = HARNESS.replace("uvicorn.run(", HOOKS + "\nuvicorn.run(")


@contextmanager
def process(database, configs, directory, mode="normal"):
    with child_process(
        database,
        configs,
        directory,
        mode,
        harness=M4_HARNESS,
        extra_env={"CLEANUP_INTERVAL_SECONDS": "0.1"},
    ) as child:
        yield child


def mutate(base, path, method, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=15) as response:
        return response.status, json.load(response)


def repair(base, file_id):
    return mutate(base, "/admin/repair", "POST", {"file_id": file_id})[1]


def wait_until(predicate):
    deadline = monotonic() + 15
    while monotonic() < deadline:
        if predicate():
            return
        sleep(0.05)
    raise AssertionError("Durable lifecycle did not converge in 15s")


def wait_ready(base):
    def ready():
        try:
            return get(base, "/health/ready") == {"status": "READY"}
        except (urllib.error.URLError, TimeoutError):
            return False  # This child has just been released from its startup barrier.

    wait_until(ready)


def snapshot(sessions):
    with sessions() as session:
        file = session.scalar(select(File))
        replicas = session.scalars(select(ChunkReplica).order_by(ChunkReplica.node_id)).all()
        return file, replicas


def paths(servers, replicas):
    return [servers[r.node_id].service.store.chunk_path(str(r.chunk_id)) for r in replicas]


def kill(child, future):
    child.kill()
    assert child.wait(timeout=3) == -9
    with pytest.raises((urllib.error.URLError, OSError)):
        future.result(timeout=5)


@pytest.mark.parametrize("window", ["delete_before", "delete_after"])
def test_sigkill_delete_tombstone_and_storage_ack(database, nodes, tmp_path, window):
    configs, servers = nodes
    sessions, _ = database
    with process(database, configs, tmp_path, window) as (first, base):
        release(first)
        wait_active(base)
        summary = upload(base, payload(257), "delete-window.bin")
        with ThreadPoolExecutor(max_workers=1) as pool:
            deleting = pool.submit(mutate, base, f"/files/{summary['file_id']}", "DELETE")
            line(first, window.upper())
            file, replicas = snapshot(sessions)
            assert file.status == "DELETING" and file.deleted_at is not None
            assert all(r.cleanup_pending and r.status == "VERIFIED" for r in replicas)
            owned_paths = paths(servers, replicas)
            assert sum(p.exists() for p in owned_paths) == (2 if window == "delete_before" else 1)
            kill(first, deleting)
    with process(database, configs, tmp_path) as (second, base):
        assert second.pid != first.pid
        file, recovered = snapshot(sessions)
        assert file.status == "DELETING" and len(recovered) == 2
        release(second)
        wait_active(base)
        wait_until(lambda: snapshot(sessions)[0].status == "DELETED")
        file, recovered = snapshot(sessions)
        assert file.deleted_at is not None and len(recovered) == 2
        assert all(r.status == "DELETED" and not r.cleanup_pending for r in recovered)
        assert not any(p.exists() for p in owned_paths)
        assert mutate(base, f"/files/{summary['file_id']}", "DELETE")[0] == 200
        with pytest.raises(urllib.error.HTTPError) as error:
            get(base, f"/files/{summary['file_id']}")
        assert error.value.code == 404


@pytest.mark.parametrize("window", ["repair_pending", "repair_store", "repair_delete"])
def test_sigkill_repair_attempt_and_corrupt_replacement(database, nodes, tmp_path, window):
    configs, servers = nodes
    sessions, _ = database
    data = payload(257)
    with process(database, configs, tmp_path, window) as (first, base):
        release(first)
        wait_active(base)
        summary = upload(base, data, "repair-window.bin")
        _, replicas = snapshot(sessions)
        destination = next(r for r in replicas if r.node_id == "node-1")
        target = paths(servers, [destination])[0]
        if window == "repair_delete":
            target.write_bytes(b"x" * len(data))
        else:
            target.unlink()
        with ThreadPoolExecutor(max_workers=1) as pool:
            repairing = pool.submit(repair, base, summary["file_id"])
            line(first, window.upper())
            file, replicas = snapshot(sessions)
            attempted = next(r for r in replicas if r.node_id == destination.node_id)
            assert file.status == "AVAILABLE"
            assert attempted.status == "PENDING" and not attempted.cleanup_pending
            assert target.exists() == (window == "repair_store")
            if target.exists():
                assert target.read_bytes() == data
            assert any(p.read_bytes() == data for p in paths(servers, replicas) if p != target)
            kill(first, repairing)
    with process(database, configs, tmp_path) as (second, base):
        file, replicas = snapshot(sessions)
        assert file.status == "AVAILABLE" and all(not r.cleanup_pending for r in replicas)
        assert next(r for r in replicas if r.node_id == destination.node_id).status == "PENDING"
        release(second)
        wait_active(base)
        result = repair(base, summary["file_id"])
        assert result["checked_chunks"] == 1 and result["remaining_chunks"] == 0
        assert result["repaired_replicas"] == (0 if window == "repair_store" else 1)
        assert result["results"][0]["live_replica_count"] == 2
        assert target.read_bytes() == data
        assert request(base, f"/files/{summary['file_id']}/download")[2] == data
        _, replicas = snapshot(sessions)
        assert len(replicas) == 2 and all(r.status == "VERIFIED" for r in replicas)


def test_sigkill_interrupted_upload_worker_cleans_bytes_keeps_failed(database, nodes, tmp_path):
    configs, servers = nodes
    sessions, _ = database
    with process(database, configs, tmp_path, "pending") as (first, base):
        release(first)
        wait_active(base)
        with ThreadPoolExecutor(max_workers=1) as pool:
            uploading = pool.submit(upload, base, payload(257), "failed-window.bin")
            line(first, "UPLOAD_PENDING")
            _, attempted = snapshot(sessions)
            owned_paths = paths(servers, attempted)
            assert len(attempted) == 1 and owned_paths[0].exists()
            kill(first, uploading)
    with process(database, configs, tmp_path) as (second, base):
        file, attempted = snapshot(sessions)
        assert file.status == "FAILED" and file.error_code == "UPLOAD_INTERRUPTED"
        assert attempted[0].cleanup_pending and owned_paths[0].exists()
        release(second)
        wait_active(base)
        wait_until(lambda: not snapshot(sessions)[1][0].cleanup_pending)
        file, attempted = snapshot(sessions)
        assert file.status == "FAILED" and file.error_code == "UPLOAD_INTERRUPTED"
        assert attempted[0].status == "DELETED" and not owned_paths[0].exists()
        expect_error(base, str(file.id), "FILE_NOT_READY")


def test_real_http_serialization_and_sigterm_drains_repair(database, nodes, tmp_path):
    configs, servers = nodes
    sessions, _ = database
    with process(database, configs, tmp_path, "repair_store") as (child, base):
        release(child)
        wait_active(base)
        summary = upload(base, payload(257), "shutdown-window.bin")
        _, replicas = snapshot(sessions)
        paths(servers, [next(r for r in replicas if r.node_id == "node-1")])[0].unlink()
        with ThreadPoolExecutor(max_workers=1) as pool:
            repairing = pool.submit(repair, base, summary["file_id"])
            line(child, "REPAIR_STORE")
            assert get(base, "/health/ready") == {"status": "READY"}
            for path, method, body in (
                (f"/files/{summary['file_id']}", "DELETE", None),
                ("/admin/repair", "POST", {"file_id": summary["file_id"]}),
            ):
                with pytest.raises(urllib.error.HTTPError) as error:
                    mutate(base, path, method, body)
                assert error.value.code == 409
                assert json.load(error.value)["error"]["code"] == "OPERATION_BUSY"
            child.terminate()
            sleep(0.2)
            assert child.poll() is None  # Uvicorn owns the admitted request until it drains.
            release(child)
            assert repairing.result(timeout=5)["repaired_replicas"] == 1
            assert child.wait(timeout=8) == -15  # Uvicorn re-raises the captured SIGTERM.
            line(child, "SHUTDOWN_DRAINED")
            file, replicas = snapshot(sessions)
            assert file.status == "AVAILABLE" and all(r.status == "VERIFIED" for r in replicas)
            assert not list((tmp_path / "downloads").glob(".download-*.tmp"))
    with process(database, configs, tmp_path) as (restarted, base):
        release(restarted)
        wait_active(base)
        assert repair(base, summary["file_id"])["repaired_replicas"] == 0
        assert request(base, f"/files/{summary['file_id']}/download")[2] == payload(257)


def test_deleting_offline_across_two_restarts_never_revives(database, nodes, tmp_path):
    configs, servers = nodes
    sessions, _ = database
    with process(database, configs, tmp_path) as (first, base):
        release(first)
        wait_active(base)
        summary = upload(base, payload(257), "offline-window.bin")
        _, replicas = snapshot(sessions)
        offline = replicas[0].node_id
        owned_paths = paths(servers, replicas)
        servers[offline].stop(0).wait()
        wait_until(
            lambda: (
                next(n for n in get(base, "/nodes")["items"] if n["node_id"] == offline)["status"]
                == "DOWN"
            )
        )
        status, result = mutate(base, f"/files/{summary['file_id']}", "DELETE")
        assert status == 202 and result["cleanup_pending_replicas"] == 1
    for index in range(2):
        with process(database, configs, tmp_path) as (child, base):
            assert snapshot(sessions)[0].status == "DELETING"
            release(child)
            wait_ready(base)
            wait_until(
                lambda: sum(n["status"] == "ACTIVE" for n in get(base, "/nodes")["items"]) == 2
            )
            expect_error(base, summary["file_id"], "FILE_DELETING")
            with pytest.raises(urllib.error.HTTPError) as error:
                repair(base, summary["file_id"])
            assert error.value.code == 409
            detail = get(base, f"/files/{summary['file_id']}")
            assert detail["status"] == "DELETING" and detail["cleanup_pending_replicas"] == 1
            if index == 1:
                config = next(c for c in configs if c.node_id == offline)
                restored = create_server(
                    servers[offline].service.settings, bind_address=f"127.0.0.1:{config.port}"
                )
                servers[offline] = restored
                restored.start()
                wait_active(base)
                wait_until(lambda: snapshot(sessions)[0].status == "DELETED")
                assert not any(p.exists() for p in owned_paths)
                assert mutate(base, f"/files/{summary['file_id']}", "DELETE")[0] == 200


def test_sigterm_worker_cancels_active_delete_before_unlink(database, nodes, tmp_path, monkeypatch):
    configs, servers = nodes
    sessions, _ = database
    entered, inactive = Event(), Event()
    with process(database, configs, tmp_path) as (child, base):
        release(child)
        wait_active(base)
        upload(base, payload(257), "worker-shutdown.bin")
        _, replicas = snapshot(sessions)
        store = servers[replicas[0].node_id].service.store
        original = store.delete

        def blocking(chunk_id, is_active):
            entered.set()
            deadline = monotonic() + 5
            while is_active() and monotonic() < deadline:
                sleep(0.01)
            if not is_active():
                inactive.set()
            return original(chunk_id, is_active)

        monkeypatch.setattr(store, "delete", blocking)
        with sessions.begin() as session:
            file = session.scalar(select(File))
            file.status, file.error_code = "FAILED", "TEST_INTERRUPTED"
            for replica in session.scalars(select(ChunkReplica)):
                replica.cleanup_pending = True
        assert entered.wait(5)
        child.terminate()
        assert child.wait(timeout=8) == -15 and inactive.wait(3)
        line(child, "SHUTDOWN_DRAINED")
        assert store.chunk_path(str(replicas[0].chunk_id)).exists()
        _, after = snapshot(sessions)
        assert after[0].cleanup_pending and after[0].status == "VERIFIED"
        monkeypatch.setattr(store, "delete", original)
    with process(database, configs, tmp_path) as (restarted, base):
        release(restarted)
        wait_active(base)
        wait_until(lambda: all(not r.cleanup_pending for r in snapshot(sessions)[1]))
        file, replicas = snapshot(sessions)
        assert file.status == "FAILED" and file.error_code == "TEST_INTERRUPTED"
        assert not any(p.exists() for p in paths(servers, replicas))


def test_sigterm_waits_for_active_repair_store_rpc(database, nodes, tmp_path, monkeypatch):
    configs, servers = nodes
    sessions, _ = database
    entered, proceed = Event(), Event()
    with process(database, configs, tmp_path) as (child, base):
        release(child)
        wait_active(base)
        summary = upload(base, payload(257), "active-store-shutdown.bin")
        _, replicas = snapshot(sessions)
        target = paths(servers, [next(r for r in replicas if r.node_id == "node-1")])[0]
        target.unlink()
        store = servers["node-1"].service.store
        original = store.put

        def blocking(*args):
            entered.set()
            assert proceed.wait(5), "Parent did not release active Store RPC"
            return original(*args)

        monkeypatch.setattr(store, "put", blocking)
        with ThreadPoolExecutor(max_workers=1) as pool:
            repairing = pool.submit(repair, base, summary["file_id"])
            try:
                assert entered.wait(5)  # The production gRPC handler is executing put.
                child.terminate()
                sleep(0.2)
                assert child.poll() is None
            finally:
                proceed.set()
            assert repairing.result(timeout=5)["repaired_replicas"] == 1
            assert child.wait(timeout=8) == -15
            line(child, "SHUTDOWN_DRAINED")
        file, replicas = snapshot(sessions)
        assert file.status == "AVAILABLE" and all(r.status == "VERIFIED" for r in replicas)
        assert target.read_bytes() == payload(257)
        assert not list(store.data_dir.glob(".store-*.tmp"))
