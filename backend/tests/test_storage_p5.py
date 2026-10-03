"""Linux process lifecycle tests; fault injection stays in a child test harness."""

import hashlib
import os
import select
import signal
import socket
import subprocess
import sys
from contextlib import contextmanager

import grpc
import pytest
import storage_pb2 as pb
import storage_pb2_grpc

from common.config import TransferSettings
from scripts.smoke_storage import fixture

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Linux subprocess crash harness")
ID = "ffffffff-ffff-4fff-8fff-ffffffffffff"
OTHER = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
DATA = fixture(2097152)

# Block the Store worker after fsync until the parent kills the child process.
CRASH_HARNESS = """
import os
from pathlib import Path
from threading import Event
import storage.chunk_store as chunks
original = chunks.os.fsync
def sync(fd):
    original(fd)
    name = Path(os.readlink(f'/proc/self/fd/{fd}')).name
    if name.startswith('.store-') and name.endswith('.tmp'):
        print('STORE_SYNCED', flush=True)
        Event().wait()
chunks.os.fsync = sync
from storage.server import main
main()
"""


@contextmanager
def process(directory, crash=False):
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    environment = {
        **os.environ,
        "NODE_ID": "process-node",
        "FAILURE_DOMAIN": "process-test",
        "GRPC_BIND_HOST": "127.0.0.1",
        "GRPC_PORT": str(port),
        "DATA_DIR": str(directory),
        "PYTHONUNBUFFERED": "1",
    }
    command = (
        [sys.executable, "-c", CRASH_HARNESS] if crash else [sys.executable, "-m", "storage.server"]
    )
    child = subprocess.Popen(
        command, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    try:
        with grpc.insecure_channel(
            f"127.0.0.1:{port}", options=TransferSettings(_env_file=None).grpc_options()
        ) as channel:
            grpc.channel_ready_future(channel).result(timeout=10)
            yield child, storage_pb2_grpc.StorageServiceStub(channel)
    finally:
        if child.poll() is None:
            if crash:
                child.kill()  # The injected Store worker deliberately cannot finish.
            else:
                child.terminate()
            try:
                child.wait(timeout=8)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=3)
        child.stdout.close()
        child.stderr.close()


def store(stub, chunk_id=ID):
    return stub.StoreChunk(
        pb.StoreChunkRequest(
            chunk_id=chunk_id, data=DATA, checksum_sha256=hashlib.sha256(DATA).hexdigest()
        ),
        timeout=5,
    )


def verify(stub, chunk_id=ID):
    result = stub.GetChunk(pb.GetChunkRequest(chunk_id=chunk_id), timeout=5)
    assert result.data == DATA and result.checksum_sha256 == hashlib.sha256(DATA).hexdigest()
    health = stub.HealthCheck(pb.HealthCheckRequest(), timeout=2)
    assert health.node_id == "process-node" and health.failure_domain == "process-test"
    assert health.storage_writable and health.used_bytes == len(DATA)


def test_committed_2mib_survives_real_process_restart(tmp_path):
    with process(tmp_path) as (first, stub):
        assert not store(stub).already_existed
        verify(stub)
        first_pid = first.pid
    assert first.returncode == 0
    with process(tmp_path) as (second, stub):
        assert second.pid != first_pid
        verify(stub)
        assert store(stub).already_existed
        assert stub.DeleteChunk(pb.DeleteChunkRequest(chunk_id=ID), timeout=2).existed
        assert not stub.DeleteChunk(pb.DeleteChunkRequest(chunk_id=ID), timeout=2).existed
        assert stub.HealthCheck(pb.HealthCheckRequest(), timeout=2).used_bytes == 0
    assert list(tmp_path.iterdir()) == []


def test_sigkill_after_fsync_before_replace_cleans_temp_and_preserves_committed(tmp_path):
    with process(tmp_path) as (_, stub):
        store(stub, OTHER)
    with process(tmp_path, crash=True) as (child, stub):
        pending = stub.StoreChunk.future(
            pb.StoreChunkRequest(
                chunk_id=ID, data=DATA, checksum_sha256=hashlib.sha256(DATA).hexdigest()
            ),
            timeout=5,
        )
        ready, _, _ = select.select([child.stdout], [], [], 5)
        assert ready and child.stdout.readline().strip() == b"STORE_SYNCED"
        temporaries = list(tmp_path.glob(".store-*.tmp"))
        assert len(temporaries) == 1 and temporaries[0].read_bytes() == DATA
        assert not (tmp_path / f"{ID}.chunk").exists()
        child.kill()
        assert child.wait(timeout=3) == -signal.SIGKILL
        with pytest.raises(grpc.RpcError):
            pending.result(timeout=3)
    with process(tmp_path) as (_, stub):
        assert not list(tmp_path.glob(".store-*.tmp"))
        verify(stub, OTHER)
        with pytest.raises(grpc.RpcError) as error:
            stub.GetChunk(pb.GetChunkRequest(chunk_id=ID), timeout=2)
        assert error.value.code() == grpc.StatusCode.NOT_FOUND
        assert not store(stub).already_existed
        assert stub.DeleteChunk(pb.DeleteChunkRequest(chunk_id=ID), timeout=2).existed
        assert stub.DeleteChunk(pb.DeleteChunkRequest(chunk_id=OTHER), timeout=2).existed
    assert list(tmp_path.iterdir()) == []
