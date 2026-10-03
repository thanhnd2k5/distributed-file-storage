import errno
import hashlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import grpc
import pytest
import storage_pb2

from storage.chunk_store import ChunkStore

CHUNK_ID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
LIMIT = 2 * 1024 * 1024


@pytest.fixture
def node(storage_node_factory):
    return storage_node_factory("p3-node")


def store(stub, data):
    return stub.StoreChunk(
        storage_pb2.StoreChunkRequest(
            chunk_id=CHUNK_ID, data=data, checksum_sha256=hashlib.sha256(data).hexdigest()
        ),
        timeout=5,
    )


@pytest.mark.parametrize("size", [1, LIMIT])
def test_store_get_delete_twice_and_health(node, size):
    stub, service, directory = node
    data = bytes(range(256)) * (size // 256) + b"x" * (size % 256)
    store(stub, data)
    result = stub.GetChunk(storage_pb2.GetChunkRequest(chunk_id=CHUNK_ID), timeout=5)
    assert result.chunk_id == CHUNK_ID and result.data == data
    assert result.checksum_sha256 == hashlib.sha256(data).hexdigest()
    first = stub.DeleteChunk(storage_pb2.DeleteChunkRequest(chunk_id=CHUNK_ID), timeout=2)
    second = stub.DeleteChunk(storage_pb2.DeleteChunkRequest(chunk_id=CHUNK_ID), timeout=2)
    assert first.chunk_id == second.chunk_id == CHUNK_ID
    assert first.existed and not second.existed
    with pytest.raises(grpc.RpcError) as error:
        stub.GetChunk(storage_pb2.GetChunkRequest(chunk_id=CHUNK_ID), timeout=2)
    assert error.value.code() == grpc.StatusCode.NOT_FOUND
    assert service.store.used_bytes == 0 and list(directory.iterdir()) == []
    assert stub.HealthCheck(storage_pb2.HealthCheckRequest(), timeout=2).used_bytes == 0


def test_missing_get_and_delete_do_not_serve_or_remove_tempfiles(node):
    stub, service, directory = node
    temporary = service.store.temporary_path()
    temporary.write_bytes(b"partial")
    with pytest.raises(grpc.RpcError) as error:
        stub.GetChunk(storage_pb2.GetChunkRequest(chunk_id=CHUNK_ID), timeout=2)
    assert error.value.code() == grpc.StatusCode.NOT_FOUND
    assert not stub.DeleteChunk(
        storage_pb2.DeleteChunkRequest(chunk_id=CHUNK_ID), timeout=2
    ).existed
    assert temporary.read_bytes() == b"partial" and service.store.used_bytes == 0


def test_get_hashes_modified_actual_bytes_and_delete_uses_cached_accounting(node):
    stub, service, directory = node
    store(stub, b"original bytes")
    (directory / f"{CHUNK_ID}.chunk").write_bytes(b"changed")
    result = stub.GetChunk(storage_pb2.GetChunkRequest(chunk_id=CHUNK_ID), timeout=2)
    assert result.data == b"changed"
    assert result.checksum_sha256 == hashlib.sha256(b"changed").hexdigest()
    assert stub.DeleteChunk(storage_pb2.DeleteChunkRequest(chunk_id=CHUNK_ID), timeout=2).existed
    assert service.store.used_bytes == 0


@pytest.mark.parametrize("size", [0, LIMIT + 1])
def test_get_rejects_detectable_local_size_corruption(node, size):
    stub, _, directory = node
    store(stub, b"chunk")
    (directory / f"{CHUNK_ID}.chunk").write_bytes(b"x" * size)
    with pytest.raises(grpc.RpcError) as error:
        stub.GetChunk(storage_pb2.GetChunkRequest(chunk_id=CHUNK_ID), timeout=2)
    assert error.value.code() == grpc.StatusCode.DATA_LOSS


def test_get_read_failure_reports_data_loss_without_exposing_private_detail(node, monkeypatch):
    stub, service, directory = node
    store(stub, b"chunk")
    original_open = Path.open

    def unreadable(path, mode="r", *args, **kwargs):
        if mode == "rb":
            raise OSError(errno.EIO, "private injected read failure")
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", unreadable)
    with pytest.raises(grpc.RpcError) as error:
        stub.GetChunk(storage_pb2.GetChunkRequest(chunk_id=CHUNK_ID), timeout=2)
    assert error.value.code() == grpc.StatusCode.DATA_LOSS
    assert "private" not in error.value.details()
    assert service.store.used_bytes == len(b"chunk")
    assert (directory / f"{CHUNK_ID}.chunk").exists()


@pytest.mark.parametrize(
    "error_number,expected",
    [
        (errno.EIO, grpc.StatusCode.INTERNAL),
        (errno.EACCES, grpc.StatusCode.FAILED_PRECONDITION),
    ],
)
def test_failed_delete_keeps_chunk_and_accounting(node, monkeypatch, error_number, expected):
    stub, service, directory = node
    store(stub, b"chunk")
    original_unlink = Path.unlink

    def denied(path, *args, **kwargs):
        if path.suffix == ".chunk":
            raise OSError(error_number, "private injected delete failure")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", denied)
    with pytest.raises(grpc.RpcError) as error:
        stub.DeleteChunk(storage_pb2.DeleteChunkRequest(chunk_id=CHUNK_ID), timeout=2)
    assert error.value.code() == expected and "private" not in error.value.details()
    assert (directory / f"{CHUNK_ID}.chunk").read_bytes() == b"chunk"
    assert service.store.used_bytes == len(b"chunk")


def test_startup_accounting_and_delete_keep_other_chunks(tmp_path):
    second_id = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    (tmp_path / f"{CHUNK_ID}.chunk").write_bytes(b"first")
    (tmp_path / f"{second_id}.chunk").write_bytes(b"second")
    instance = ChunkStore(tmp_path, LIMIT)
    assert instance.delete(CHUNK_ID)
    assert instance.used_bytes == len(b"second")
    assert instance.get(second_id).data == b"second"
    assert not instance.delete(CHUNK_ID) and instance.used_bytes == len(b"second")


def test_get_detects_short_read(node, monkeypatch):
    stub, _, _ = node
    store(stub, b"chunk")
    original_open = Path.open

    class ShortRead:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.handle.close()

        def fileno(self):
            return self.handle.fileno()

        def read(self, size):
            return self.handle.read(1)

    def shortened(path, mode="r", *args, **kwargs):
        handle = original_open(path, mode, *args, **kwargs)
        return ShortRead(handle) if mode == "rb" else handle

    monkeypatch.setattr(Path, "open", shortened)
    with pytest.raises(grpc.RpcError) as error:
        stub.GetChunk(storage_pb2.GetChunkRequest(chunk_id=CHUNK_ID), timeout=2)
    assert error.value.code() == grpc.StatusCode.DATA_LOSS


def test_get_snapshot_remains_valid_after_delete(tmp_path, monkeypatch):
    instance = ChunkStore(tmp_path, LIMIT)
    instance.put(CHUNK_ID, b"chunk", hashlib.sha256(b"chunk").hexdigest(), lambda: True)
    reading = Event()
    release_read = Event()
    original_open = Path.open

    class PausedRead:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.handle.close()

        def fileno(self):
            return self.handle.fileno()

        def read(self, size):
            data = self.handle.read(size)
            reading.set()
            assert release_read.wait(timeout=3)
            return data

    def paused(path, mode="r", *args, **kwargs):
        handle = original_open(path, mode, *args, **kwargs)
        return PausedRead(handle) if mode == "rb" else handle

    monkeypatch.setattr(Path, "open", paused)
    with ThreadPoolExecutor(max_workers=2) as pool:
        get_future = pool.submit(instance.get, CHUNK_ID)
        try:
            assert reading.wait(timeout=2)
            delete_future = pool.submit(instance.delete, CHUNK_ID)
            assert instance.chunk_path(CHUNK_ID).exists()
        finally:
            release_read.set()
        result = get_future.result(timeout=3)
        assert delete_future.result(timeout=3)
    assert (
        result.data == b"chunk" and result.checksum_sha256 == hashlib.sha256(b"chunk").hexdigest()
    )
    assert not instance.chunk_path(CHUNK_ID).exists() and instance.used_bytes == 0
