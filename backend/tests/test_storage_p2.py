import errno
import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Event

import grpc
import pytest
import storage_pb2

from storage.chunk_store import ChunkStore, InactiveStoreError

CHUNK_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
LIMIT = 2 * 1024 * 1024


def request(data=b"chunk"):
    return storage_pb2.StoreChunkRequest(
        chunk_id=CHUNK_ID, data=data, checksum_sha256=hashlib.sha256(data).hexdigest()
    )


@pytest.fixture
def node(storage_node_factory):
    return storage_node_factory("p2-node")


def test_store_ack_retry_and_health_accounting(node):
    stub, service, directory = node
    payload = request(b"x" * LIMIT)
    first = stub.StoreChunk(payload, timeout=5)
    assert first.chunk_id == CHUNK_ID
    assert first.size_bytes == LIMIT and first.checksum_sha256 == payload.checksum_sha256
    assert not first.already_existed
    committed = directory / f"{CHUNK_ID}.chunk"
    assert committed.read_bytes() == payload.data
    duplicate = stub.StoreChunk(payload, timeout=5)
    assert duplicate.already_existed
    assert (
        duplicate.chunk_id == first.chunk_id and duplicate.checksum_sha256 == first.checksum_sha256
    )
    assert duplicate.size_bytes == LIMIT
    assert service.store.used_bytes == LIMIT
    assert stub.HealthCheck(storage_pb2.HealthCheckRequest(), timeout=2).used_bytes == LIMIT
    assert list(directory.iterdir()) == [committed]


def test_store_conflict_does_not_overwrite_or_change_accounting(node):
    stub, service, directory = node
    stub.StoreChunk(request(b"original"), timeout=2)
    with pytest.raises(grpc.RpcError) as error:
        stub.StoreChunk(request(b"different"), timeout=2)
    assert error.value.code() == grpc.StatusCode.ALREADY_EXISTS
    assert (directory / f"{CHUNK_ID}.chunk").read_bytes() == b"original"
    assert service.store.used_bytes == len(b"original")
    assert len(list(directory.iterdir())) == 1


def test_duplicate_store_checks_actual_disk_bytes(node):
    stub, _, directory = node
    payload = request(b"original")
    stub.StoreChunk(payload, timeout=2)
    committed = directory / f"{CHUNK_ID}.chunk"
    committed.write_bytes(b"modified")
    with pytest.raises(grpc.RpcError) as error:
        stub.StoreChunk(payload, timeout=2)
    assert error.value.code() == grpc.StatusCode.ALREADY_EXISTS
    assert committed.read_bytes() == b"modified"


@pytest.mark.parametrize("stage", ["open", "write", "flush", "fsync", "replace"])
def test_store_io_failure_leaves_no_committed_or_temporary_file(node, monkeypatch, stage):
    stub, service, directory = node
    original_open = Path.open

    def fail(*args, **kwargs):
        raise OSError(errno.EIO, "injected private filesystem error")

    class FaultyFile:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.handle.close()

        def write(self, data):
            if stage == "write":
                self.handle.write(data[:1])
                fail()
            return self.handle.write(data)

        def flush(self):
            if stage == "flush":
                fail()
            self.handle.flush()

        def fileno(self):
            return self.handle.fileno()

    def patched_open(path, mode="r", *args, **kwargs):
        if mode == "xb":
            if stage == "open":
                fail()
            return FaultyFile(original_open(path, mode, *args, **kwargs))
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", patched_open)
    if stage in {"fsync", "replace"}:
        monkeypatch.setattr(f"storage.chunk_store.os.{stage}", fail)
    with pytest.raises(grpc.RpcError) as error:
        stub.StoreChunk(request(), timeout=2)
    assert error.value.code() == grpc.StatusCode.INTERNAL
    assert "private" not in error.value.details()
    assert list(directory.iterdir()) == []
    assert service.store.used_bytes == 0


@pytest.mark.parametrize(
    "error_number,expected",
    [
        (errno.ENOSPC, grpc.StatusCode.RESOURCE_EXHAUSTED),
        (errno.EDQUOT, grpc.StatusCode.RESOURCE_EXHAUSTED),
        (errno.EACCES, grpc.StatusCode.FAILED_PRECONDITION),
        (errno.EROFS, grpc.StatusCode.FAILED_PRECONDITION),
    ],
)
def test_store_classifies_capacity_and_writable_errors(node, monkeypatch, error_number, expected):
    stub, service, directory = node

    def fail(*args):
        raise OSError(error_number, "injected")

    monkeypatch.setattr("storage.chunk_store.os.replace", fail)
    with pytest.raises(grpc.RpcError) as error:
        stub.StoreChunk(request(), timeout=2)
    assert error.value.code() == expected
    assert list(directory.iterdir()) == [] and service.store.used_bytes == 0


def test_two_conflicting_rpc_stores_commit_exactly_one_payload(node):
    stub, service, directory = node
    ready = Barrier(2)
    payloads = [b"first", b"second"]

    def send(data):
        ready.wait(timeout=5)
        try:
            response = stub.StoreChunk(request(data), timeout=5)
            return data, response
        except grpc.RpcError as error:
            return data, error.code()

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(send, payloads))
    successes = [
        (data, result) for data, result in outcomes if not isinstance(result, grpc.StatusCode)
    ]
    assert len(successes) == 1
    winning_data, response = successes[0]
    assert not response.already_existed
    assert [result for _, result in outcomes if isinstance(result, grpc.StatusCode)] == [
        grpc.StatusCode.ALREADY_EXISTS
    ]
    assert (directory / f"{CHUNK_ID}.chunk").read_bytes() == winning_data
    assert service.store.used_bytes == len(winning_data)
    assert len(list(directory.iterdir())) == 1


def test_inactive_request_after_waiting_for_lock_cannot_commit(tmp_path):
    store = ChunkStore(tmp_path, LIMIT)
    started = Event()

    def send():
        started.set()
        return store.put(CHUNK_ID, b"chunk", request().checksum_sha256, lambda: False)

    with ThreadPoolExecutor(max_workers=1) as pool:
        with store.operation_lock:
            future = pool.submit(send)
            assert started.wait(timeout=2)
        with pytest.raises(InactiveStoreError):
            future.result(timeout=2)
    assert list(tmp_path.iterdir()) == [] and store.used_bytes == 0


def test_cancellation_after_fsync_prevents_replace(tmp_path, monkeypatch):
    store = ChunkStore(tmp_path, LIMIT)
    active = True
    original_fsync = os.fsync

    def expire(fd):
        nonlocal active
        original_fsync(fd)
        active = False

    monkeypatch.setattr("storage.chunk_store.os.fsync", expire)
    with pytest.raises(InactiveStoreError):
        store.put(CHUNK_ID, b"chunk", request().checksum_sha256, lambda: active)
    assert list(tmp_path.iterdir()) == [] and store.used_bytes == 0


def test_commit_is_only_visible_after_fsync_and_replace(tmp_path, monkeypatch):
    store = ChunkStore(tmp_path, LIMIT)
    committed = store.chunk_path(CHUNK_ID)
    original_fsync = os.fsync
    original_replace = os.replace
    stages = []

    def sync(fd):
        assert not committed.exists() and store.used_bytes == 0
        original_fsync(fd)
        stages.append("synced")

    def replace(source, target):
        assert stages == ["synced"]
        assert source.parent == target.parent == tmp_path
        assert source.read_bytes() == b"chunk" and not committed.exists()
        original_replace(source, target)
        stages.append("committed")

    monkeypatch.setattr("storage.chunk_store.os.fsync", sync)
    monkeypatch.setattr("storage.chunk_store.os.replace", replace)
    result = store.put(CHUNK_ID, b"chunk", request().checksum_sha256, lambda: True)
    assert stages == ["synced", "committed"] and not result.already_existed
    assert committed.read_bytes() == b"chunk" and store.used_bytes == len(b"chunk")


def test_retry_after_client_cancels_committed_rpc_is_idempotent(node, monkeypatch):
    stub, service, directory = node
    committed = Event()
    release_response = Event()
    original_put = service.store.put

    def delayed_response(*args):
        result = original_put(*args)
        committed.set()
        assert release_response.wait(timeout=5)
        return result

    monkeypatch.setattr(service.store, "put", delayed_response)
    call = stub.StoreChunk.future(request(), timeout=5)
    try:
        assert committed.wait(timeout=3)
        assert call.cancel()
        with pytest.raises(grpc.FutureCancelledError):
            call.result()
    finally:
        release_response.set()
    monkeypatch.setattr(service.store, "put", original_put)
    retry = stub.StoreChunk(request(), timeout=3)
    assert retry.already_existed
    assert (directory / f"{CHUNK_ID}.chunk").read_bytes() == b"chunk"
    assert service.store.used_bytes == len(b"chunk")


def test_temp_collision_does_not_remove_file_it_did_not_create(tmp_path, monkeypatch):
    store = ChunkStore(tmp_path, LIMIT)
    temporary = store.temporary_path()
    temporary.write_bytes(b"keep")
    monkeypatch.setattr(store, "temporary_path", lambda: temporary)
    with pytest.raises(FileExistsError):
        store.put(CHUNK_ID, b"chunk", request().checksum_sha256, lambda: True)
    assert temporary.read_bytes() == b"keep"
    assert not store.chunk_path(CHUNK_ID).exists() and store.used_bytes == 0
