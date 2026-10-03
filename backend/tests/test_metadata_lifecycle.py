"""Real Linux Metadata child processes, real RPC and isolated PostgreSQL schemas."""

import json
import os
import select
import signal
import socket
import subprocess
import sys
import urllib.error
import urllib.request
import uuid
from contextlib import contextmanager
from time import monotonic, sleep

import pytest
from sqlalchemy.engine import make_url

from metadata.config import NodeConfig
from metadata.models import ChunkReplica, File
from scripts.metadata_fixture import seed, snapshot
from storage.config import StorageSettings
from storage.server import create_server

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Linux process/RPC harness")

# Test-only barrier after real startup recovery, before worker startup/HTTP readiness.
HARNESS = """
import os, sys
import uvicorn
from metadata.worker import MetadataWorker
original = MetadataWorker.start
original_stop = MetadataWorker.stop
def start(self):
    print('STARTUP_RESET', flush=True)
    if sys.stdin.readline().strip() != 'continue':
        raise RuntimeError('parent did not release startup barrier')
    return original(self)
MetadataWorker.start = start
def stop(self):
    original_stop(self)
    print('WORKER_STOPPED', flush=True)
MetadataWorker.stop = stop
uvicorn.run('metadata.main:create_app', factory=True, host='127.0.0.1',
            port=int(os.environ['TEST_HTTP_PORT']), log_level='warning')
"""


def get(base, path):
    with urllib.request.urlopen(base + path, timeout=2) as response:
        assert response.status == 200
        return json.load(response)


def wait_active(base):
    deadline = monotonic() + 10
    while monotonic() < deadline:
        try:
            ready = get(base, "/api/v1/health/ready")
            nodes = get(base, "/api/v1/nodes")["items"]
            if ready == {"status": "READY"} and nodes[0]["status"] == "ACTIVE":
                return nodes[0]
        except (urllib.error.URLError, TimeoutError):
            pass  # Only expected during the child startup boundary.
        sleep(0.05)
    raise AssertionError("Child Metadata did not become ready with an ACTIVE node within 10s")


@contextmanager
def process(database, config, log_path):
    sessions, url = database
    schema = sessions.kw["bind"].get_execution_options()["schema_translate_map"][None]
    scoped_url = make_url(url).update_query_dict({"options": f"-csearch_path={schema}"})
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    environment = {
        **os.environ,
        "DATABASE_URL": scoped_url.render_as_string(hide_password=False),
        "STORAGE_NODES_JSON": json.dumps([config.model_dump()]),
        "REPLICATION_FACTOR": "1",
        "HEALTH_INTERVAL_SECONDS": "0.1",
        "HEALTH_RPC_TIMEOUT_SECONDS": "0.2",
        "NODE_DOWN_AFTER_SECONDS": "0.6",
        "TEST_HTTP_PORT": str(port),
        "PYTHONUNBUFFERED": "1",
    }
    with log_path.open("wb") as errors:
        child = subprocess.Popen(
            [sys.executable, "-c", HARNESS],
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=errors,
        )
        try:
            readable, _, _ = select.select([child.stdout], [], [], 10)
            assert readable and child.stdout.readline().strip() == b"STARTUP_RESET", (
                "Child startup failed before the recovery barrier; inspect child log"
            )
            yield child, f"http://127.0.0.1:{port}"
        finally:
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=3)
            child.shutdown_output = child.stdout.read()
            child.stdin.close()
            child.stdout.close()


def release(child):
    child.stdin.write(b"continue\n")
    child.stdin.flush()


def recovered(state):
    # Derive only the documented startup changes; all other columns must be identical.
    state = json.loads(json.dumps(state))
    for node in state["storage_nodes"]:
        node["status"] = "DOWN"
        node["enabled"] = node["node_id"] != "retired-node"
        if node["enabled"]:
            node["last_error"] = None
    for file in state["files"]:
        if file["status"] == "UPLOADING":
            file["status"] = "FAILED"
            file["error_code"] = "UPLOAD_INTERRUPTED"
    inactive = {file["id"] for file in state["files"] if file["status"] in {"FAILED", "DELETING"}}
    chunks = {chunk["id"] for chunk in state["chunks"] if chunk["file_id"] in inactive}
    for replica in state["chunk_replicas"]:
        if replica["chunk_id"] in chunks and replica["status"] != "DELETED":
            replica["cleanup_pending"] = True
    return state


def test_metadata_process_restart_preserves_fixture_and_resets_health_before_poll(
    database, tmp_path
):
    settings = StorageSettings(
        _env_file=None, node_id="node-1", failure_domain="test", data_dir=tmp_path / "chunks"
    )
    server = create_server(settings, bind_address="127.0.0.1:0")
    server.start()
    config = NodeConfig(node_id="node-1", host="127.0.0.1", port=server.port, failure_domain="test")
    sessions, _ = database
    try:
        seed(sessions, [config])
        initial = snapshot(sessions)
        with process(database, config, tmp_path / "first.log") as (first, base):
            assert snapshot(sessions) == recovered(initial)
            release(first)
            active = wait_active(base)
            assert active["last_error"] is None and active["last_success_at"] is not None
            assert active["capacity_bytes"] > 0 and active["used_bytes"] == 0
            assert get(base, "/api/v1/health/live") == {"status": "LIVE"}
            assert get(base, "/api/v1/cluster")["files_available"] == 1
            first_pid = first.pid
            # Simulate an interrupted upload in this process's isolated schema.
            with sessions.begin() as session:
                uploading = session.get(File, uuid.UUID(int=2))
                uploading.status = "UPLOADING"
                uploading.error_code = None
                pending = session.get(ChunkReplica, (uuid.UUID(int=102), "node-1"))
                pending.cleanup_pending = False
        assert first.returncode in (0, -signal.SIGTERM)
        assert b"WORKER_STOPPED" in first.shutdown_output
        before_restart = snapshot(sessions)
        with process(database, config, tmp_path / "second.log") as (second, base):
            assert second.pid != first_pid
            assert snapshot(sessions) == recovered(before_restart)
            release(second)
            active = wait_active(base)
            assert active["last_error"] is None
            assert active["last_success_at"] > before_restart["storage_nodes"][0]["last_success_at"]
            assert get(base, "/api/v1/cluster")["cleanup_pending_replicas"] == 3
        assert second.returncode in (0, -signal.SIGTERM)
        assert b"WORKER_STOPPED" in second.shutdown_output
        final = snapshot(sessions)
        for table in ("files", "chunks", "chunk_replicas"):
            assert final[table] == recovered(initial)[table]
    finally:
        server.stop(0).wait()
