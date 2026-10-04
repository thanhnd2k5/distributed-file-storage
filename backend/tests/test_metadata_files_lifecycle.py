"""Real HTTP child processes, SIGKILL windows, RPC disks and isolated DB schema."""

import json
import os
import select
import socket
import subprocess
import sys
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from time import monotonic, sleep
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select as sql_select
from sqlalchemy.engine import make_url

from metadata.config import NodeConfig
from metadata.models import Chunk, ChunkReplica, File, StorageNode
from scripts.smoke_files import get, mapping, payload, request, upload, verify
from storage.config import StorageSettings
from storage.server import create_server

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Linux process/RPC harness")
SIZE = 10 * 1024 * 1024 + 17

# Hooks are test-only, after real committed boundaries. Bounded pipe barriers,
# not timing guesses, let the parent inspect DB/disk and issue SIGKILL.
HARNESS = """
import os, select, sys
import uvicorn
from metadata.worker import MetadataWorker
from metadata.operations import DataOperation
from metadata import upload
def barrier(name):
    print(name, flush=True)
    ready, _, _ = select.select([sys.stdin], [], [], 30)
    if not ready or sys.stdin.readline().strip() != 'continue':
        raise RuntimeError('parent did not release ' + name)
start = MetadataWorker.start
def starting(self):
    barrier('STARTUP_RESET')
    return start(self)
MetadataWorker.start = starting
mode = os.environ['CRASH_WINDOW']
if mode == 'pending':
    store = DataOperation.store_chunk
    fired = False
    def storing(self, *args, **kwargs):
        global fired
        result = store(self, *args, **kwargs)
        if not fired:
            fired = True
            barrier('UPLOAD_PENDING')
        return result
    DataOperation.store_chunk = storing
elif mode == 'available':
    commit = upload.commit_available
    def committed(*args, **kwargs):
        result = commit(*args, **kwargs)
        barrier('UPLOAD_AVAILABLE')
        return result
    upload.commit_available = committed
uvicorn.run('metadata.main:create_app', factory=True, host='127.0.0.1',
            port=int(os.environ['TEST_HTTP_PORT']), log_level='warning')
"""


def line(child, expected):
    readable, _, _ = select.select([child.stdout], [], [], 15)
    assert readable and child.stdout.readline().strip() == expected.encode(), (
        f"Missing child barrier {expected}; inspect child stderr log"
    )


def release(child):
    child.stdin.write(b"continue\n")
    child.stdin.flush()


def wait_active(base):
    deadline = monotonic() + 15
    while monotonic() < deadline:
        try:
            nodes = get(base, "/nodes")["items"]
            if get(base, "/health/ready") == {"status": "READY"} and all(
                node["status"] == "ACTIVE" for node in nodes
            ):
                return nodes
        except (urllib.error.URLError, TimeoutError):
            pass  # Only expected while this child starts.
        sleep(0.05)
    raise AssertionError("Child failed to become ready/ACTIVE in 15s")


@pytest.fixture
def nodes(tmp_path):
    servers, configs = {}, []
    try:
        for index, domain in enumerate(("A", "B", "B"), 1):
            node_id = f"node-{index}"
            settings = StorageSettings(
                _env_file=None, node_id=node_id, failure_domain=domain, data_dir=tmp_path / node_id
            )
            server = create_server(settings, bind_address="127.0.0.1:0")
            server.start()
            servers[node_id] = server
            configs.append(
                NodeConfig(
                    node_id=node_id, host="127.0.0.1", port=server.port, failure_domain=domain
                )
            )
        yield configs, servers
    finally:
        for server in servers.values():
            server.stop(0).wait()


@contextmanager
def child_process(database, configs, directory, mode="normal"):
    sessions, url = database
    schema = sessions.kw["bind"].get_execution_options()["schema_translate_map"][None]
    scoped_url = make_url(url).update_query_dict({"options": f"-csearch_path={schema}"})
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    environment = {
        **os.environ,
        "DATABASE_URL": scoped_url.render_as_string(hide_password=False),
        "STORAGE_NODES_JSON": json.dumps([config.model_dump() for config in configs]),
        "REPLICATION_FACTOR": "2",
        "CHUNK_SIZE_BYTES": "2097152",
        "MAX_FILE_SIZE_BYTES": str(16 * 1024 * 1024),
        "HEALTH_INTERVAL_SECONDS": "0.1",
        "HEALTH_RPC_TIMEOUT_SECONDS": "0.2",
        "NODE_DOWN_AFTER_SECONDS": "0.6",
        "CHUNK_RPC_TIMEOUT_SECONDS": "1",
        "DOWNLOAD_TEMP_DIR": str(directory / "downloads"),
        "TEST_HTTP_PORT": str(port),
        "CRASH_WINDOW": mode,
        "PYTHONUNBUFFERED": "1",
    }
    with (directory / (mode + "-" + uuid4().hex + ".log")).open("wb") as errors:
        child = subprocess.Popen(
            [sys.executable, "-c", HARNESS],
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=errors,
        )
        try:
            line(child, "STARTUP_RESET")
            # Recovery/janitor have committed; no worker has polled yet.
            with sessions() as session:
                assert all(
                    node.status == "DOWN" for node in session.scalars(sql_select(StorageNode))
                )
            yield child, f"http://127.0.0.1:{port}/api/v1"
        finally:
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=3)
            child.stdin.close()
            child.stdout.close()


def manifest(base, summary, path):
    state = {
        "file": summary,
        "chunks": mapping(get(base, f"/files/{summary['file_id']}/chunks")["items"]),
    }
    path.write_text(json.dumps(state, indent=2))
    return state


def expect_error(base, file_id, code):
    with pytest.raises(urllib.error.HTTPError) as error:
        request(base, f"/files/{file_id}/download")
    response = error.value
    try:
        assert response.code in (409, 503)
        assert response.headers.get_content_type() == "application/json"
        assert json.load(response)["error"]["code"] == code
    finally:
        response.close()


def test_multichunk_domains_real_disk_faults_and_normal_restart(database, nodes, tmp_path):
    configs, servers = nodes
    with child_process(database, configs, tmp_path) as (first, base):
        release(first)
        wait_active(base)
        summary = upload(base, payload(SIZE), "lifecycle.bin")
        state = manifest(base, summary, tmp_path / "manifest.json")
        verify(base, state)
        assert all(chunk["domains"] == ["A", "B"] for chunk in state["chunks"])
        # Two faults in different chunks; every mutation is in fixture-only dirs.
        bad, missing = state["chunks"][:2]
        bad_node, missing_node = bad["nodes"][0], missing["nodes"][0]
        corrupted = servers[bad_node].service.store.chunk_path(bad["chunk_id"])
        corrupted.write_bytes(b"x" * bad["size_bytes"])
        removed = servers[missing_node].service.store.chunk_path(missing["chunk_id"])
        removed.unlink()
        assert request(base, f"/files/{summary['file_id']}/download")[2] == payload(SIZE)
        chunks = get(base, f"/files/{summary['file_id']}/chunks")["items"]
        assert (
            next(r for r in chunks[0]["replicas"] if r["node_id"] == bad_node)["status"]
            == "CORRUPTED"
        )
        assert (
            next(r for r in chunks[1]["replicas"] if r["node_id"] == missing_node)["status"]
            == "MISSING"
        )
        last = state["chunks"][-1]
        for node_id in last["nodes"]:
            servers[node_id].service.store.chunk_path(last["chunk_id"]).unlink()
        expect_error(base, summary["file_id"], "CHUNK_UNAVAILABLE")
        # Restore only our fixture bytes so restart can verify the exact snapshot.
        data = payload(SIZE)
        for chunk in (bad, missing, last):
            block = data[
                chunk["chunk_index"] * summary["chunk_size_bytes"] : (chunk["chunk_index"] + 1)
                * summary["chunk_size_bytes"]
            ]
            for node_id in chunk["nodes"]:
                servers[node_id].service.store.chunk_path(chunk["chunk_id"]).write_bytes(block)
        # Probe restored non-VERIFIED sources too; all original mappings reconcile.
        request(base, f"/files/{summary['file_id']}/download")
        # Download may choose a healthy replica without probing every bad one;
        # preserve observed statuses across restart instead of assuming repair.
        before = get(base, f"/files/{summary['file_id']}/chunks")["items"]
    downloads = tmp_path / "downloads"
    stale = downloads / (".download-" + uuid4().hex + ".tmp")
    stale.write_bytes(b"stale fixture")
    foreign = downloads / "keep.txt"
    foreign.write_bytes(b"foreign fixture")
    with child_process(database, configs, tmp_path) as (second, base):
        assert (
            second.pid != first.pid
            and not stale.exists()
            and foreign.read_bytes() == b"foreign fixture"
        )
        # Snapshot all file/chunk IDs, RF and observations before health starts.
        release(second)
        wait_active(base)
        after = get(base, f"/files/{summary['file_id']}/chunks")["items"]
        assert mapping(after) == mapping(before)
        assert [[r["status"] for r in c["replicas"]] for c in after] == [
            [r["status"] for r in c["replicas"]] for c in before
        ]
        detail = get(base, f"/files/{summary['file_id']}")
        assert all(detail[key] == value for key, value in summary.items())
        assert request(base, f"/files/{summary['file_id']}/download")[2] == payload(SIZE)
        assert sorted(path.name for path in downloads.iterdir()) == ["keep.txt"]


@pytest.mark.parametrize("window", ["pending", "available"])
def test_sigkill_upload_commit_windows_recover_actual_bytes(database, nodes, tmp_path, window):
    configs, servers = nodes
    sessions, _ = database
    size = 2 * 1024 * 1024 + 17
    with child_process(database, configs, tmp_path, window) as (first, base):
        release(first)
        wait_active(base)
        with ThreadPoolExecutor(max_workers=1) as pool:
            uploading = pool.submit(upload, base, payload(size), "crash.bin")
            line(first, "UPLOAD_" + window.upper())
            with sessions() as session:
                file = session.scalar(sql_select(File))
                file_id = str(file.id)
                chunks = session.scalars(sql_select(Chunk).order_by(Chunk.chunk_index)).all()
                replicas = session.scalars(sql_select(ChunkReplica)).all()
                assert file.status == ("UPLOADING" if window == "pending" else "AVAILABLE")
                assert all(not replica.cleanup_pending for replica in replicas)
                if window == "pending":
                    assert len(replicas) == 1 and replicas[0].status == "PENDING"
                else:
                    assert len(replicas) == 4 and all(
                        replica.status == "VERIFIED" for replica in replicas
                    )
                actual = {
                    (str(replica.chunk_id), replica.node_id): servers[replica.node_id]
                    .service.store.chunk_path(str(replica.chunk_id))
                    .read_bytes()
                    for replica in replicas
                }
                chunk_ids = [str(chunk.id) for chunk in chunks]
            (tmp_path / "manifest.json").write_text(
                json.dumps(
                    {
                        "window": window,
                        "file_id": file_id,
                        "chunk_ids": chunk_ids,
                        "replicas": list(actual),
                    }
                )
            )
            first.kill()
            assert first.wait(timeout=3) == -9
            with pytest.raises((urllib.error.URLError, OSError)):
                uploading.result(timeout=5)
    with child_process(database, configs, tmp_path) as (second, base):
        assert second.pid != first.pid
        with sessions() as session:
            file = session.get(File, UUID(file_id))
            replicas = session.scalars(sql_select(ChunkReplica)).all()
            assert [
                str(chunk.id)
                for chunk in session.scalars(sql_select(Chunk).order_by(Chunk.chunk_index))
            ] == chunk_ids
            assert set(actual) == {(str(r.chunk_id), r.node_id) for r in replicas}
            if window == "pending":
                assert file.status == "FAILED" and file.error_code == "UPLOAD_INTERRUPTED"
                assert all(r.cleanup_pending and r.status == "PENDING" for r in replicas)
            else:
                assert file.status == "AVAILABLE" and file.error_code is None
                assert file.checksum_sha256 is not None and file.replication_factor == 2
                assert all(not r.cleanup_pending and r.status == "VERIFIED" for r in replicas)
        for (chunk_id, node_id), block in actual.items():
            assert servers[node_id].service.store.chunk_path(chunk_id).read_bytes() == block
        release(second)
        wait_active(base)
        if window == "pending":
            expect_error(base, file_id, "FILE_NOT_READY")
            assert get(base, "/files")["total"] == 0
            assert get(base, "/files?include_inactive=true")["items"][0]["file_id"] == file_id
        else:
            assert get(base, "/files")["items"][0]["file_id"] == file_id
            assert request(base, f"/files/{file_id}/download")[2] == payload(size)
