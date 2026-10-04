import errno
import hashlib
import os
from threading import Event

import grpc
import pytest
import storage_pb2

CHUNK_ID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
DATA = b"complete chunk snapshot"


def request():
    return storage_pb2.StoreChunkRequest(
        chunk_id=CHUNK_ID, data=DATA, checksum_sha256=hashlib.sha256(DATA).hexdigest()
    )


@pytest.fixture
def node(storage_node_factory):
    return storage_node_factory("p4-node")


def health(stub):
    return stub.HealthCheck(storage_pb2.HealthCheckRequest(), timeout=2)


@pytest.mark.parametrize("stage", ["waiting", "before_unlink"])
@pytest.mark.parametrize("outcome", ["cancel", "deadline"])
def test_delete_cancel_or_deadline_cannot_unlink_after_rpc_expired(
    node, monkeypatch, stage, outcome
):
    stub, service, directory = node
    stub.StoreChunk(request(), timeout=3)
    entered, inactive, finished, release = Event(), Event(), Event(), Event()
    original_delete = service.store.delete
    original_lstat = type(directory).lstat

    def observe(chunk_id, is_active):
        is_active.__self__.add_callback(inactive.set)
        entered.set()
        try:
            return original_delete(chunk_id, is_active)
        finally:
            finished.set()

    def paused_lstat(path, *args, **kwargs):
        result = original_lstat(path, *args, **kwargs)
        if path.name == f"{CHUNK_ID}.chunk":
            assert release.wait(3)
        return result

    monkeypatch.setattr(service.store, "delete", observe)
    if stage == "waiting":
        service.store.operation_lock.acquire()
    else:
        monkeypatch.setattr(type(directory), "lstat", paused_lstat)
    try:
        call = stub.DeleteChunk.future(
            storage_pb2.DeleteChunkRequest(chunk_id=CHUNK_ID),
            timeout=0.2 if outcome == "deadline" else 3,
        )
        assert entered.wait(2)
        if outcome == "cancel":
            call.cancel()
            with pytest.raises(grpc.FutureCancelledError):
                call.result(timeout=2)
        else:
            with pytest.raises(grpc.RpcError) as caught:
                call.result(timeout=2)
            assert caught.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
        assert inactive.wait(2)
    finally:
        release.set()
        if stage == "waiting":
            service.store.operation_lock.release()
    assert finished.wait(2)
    assert (directory / f"{CHUNK_ID}.chunk").read_bytes() == DATA
    assert service.store.used_bytes == len(DATA)


def test_health_uses_cached_snapshot_without_chunk_lock_or_directory_walk(node, monkeypatch):
    stub, service, directory = node
    stub.StoreChunk(request(), timeout=3)
    temporary = service.store.temporary_path()
    temporary.write_bytes(b"partial")

    def forbid_walk(*args):
        raise AssertionError("Health must not scan chunk files")

    monkeypatch.setattr("storage.chunk_store.os.scandir", forbid_walk)
    with service.store.operation_lock:
        result = health(stub)
    assert result.node_id == "p4-node" and result.failure_domain == "test"
    assert result.storage_writable and result.used_bytes == len(DATA)
    assert 0 <= result.available_bytes <= result.capacity_bytes
    assert temporary.read_bytes() == b"partial"
    assert sorted(path.name for path in directory.iterdir()) == sorted(
        [f"{CHUNK_ID}.chunk", temporary.name]
    )


@pytest.mark.parametrize("stage", ["waiting", "synced"])
@pytest.mark.parametrize("outcome", ["cancel", "deadline"])
def test_real_rpc_cancel_or_deadline_before_commit_leaves_no_artifacts(
    node, monkeypatch, stage, outcome
):
    stub, service, directory = node
    entered = Event()
    inactive = Event()
    finished = Event()
    synced = Event()
    release = Event()
    original_put = service.store.put
    original_fsync = os.fsync

    def observed_put(chunk_id, data, checksum, is_active):
        is_active.__self__.add_callback(inactive.set)
        entered.set()
        try:
            return original_put(chunk_id, data, checksum, is_active)
        finally:
            finished.set()

    def pause_after_sync(fd):
        original_fsync(fd)
        synced.set()
        assert release.wait(timeout=5)

    monkeypatch.setattr(service.store, "put", observed_put)
    if stage == "synced":
        monkeypatch.setattr("storage.chunk_store.os.fsync", pause_after_sync)
    else:
        service.store.operation_lock.acquire()
    try:
        call = stub.StoreChunk.future(request(), timeout=1 if outcome == "deadline" else 5)
        assert entered.wait(timeout=2)
        if stage == "synced":
            assert synced.wait(timeout=2)
        if outcome == "cancel":
            assert call.cancel()
            with pytest.raises(grpc.FutureCancelledError):
                call.result(timeout=2)
        else:
            with pytest.raises(grpc.RpcError) as error:
                call.result(timeout=3)
            assert error.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
        assert inactive.wait(timeout=2)
    finally:
        if stage == "waiting":
            service.store.operation_lock.release()
        release.set()
    assert finished.wait(timeout=3)
    assert list(directory.iterdir()) == [] and service.store.used_bytes == 0


def test_get_delete_and_health_during_store_commit_observe_legal_order(node, monkeypatch):
    stub, service, directory = node
    synced = Event()
    release = Event()
    get_entered = Event()
    delete_entered = Event()
    original_fsync = os.fsync
    original_get = service.store.get
    original_delete = service.store.delete

    def pause_store_sync(fd):
        original_fsync(fd)
        # Health also fsyncs its probe; only hold the Store worker.
        if not synced.is_set():
            synced.set()
            assert release.wait(timeout=5)

    def get(chunk_id):
        get_entered.set()
        return original_get(chunk_id)

    def delete(chunk_id, is_active=lambda: True):
        delete_entered.set()
        return original_delete(chunk_id, is_active)

    monkeypatch.setattr("storage.chunk_store.os.fsync", pause_store_sync)
    monkeypatch.setattr(service.store, "get", get)
    monkeypatch.setattr(service.store, "delete", delete)
    store_call = stub.StoreChunk.future(request(), timeout=5)
    try:
        assert synced.wait(timeout=2)
        assert not service.store.chunk_path(CHUNK_ID).exists()
        temporaries = list(directory.glob(".store-*.tmp"))
        assert len(temporaries) == 1 and temporaries[0].read_bytes() == DATA
        get_call = stub.GetChunk.future(storage_pb2.GetChunkRequest(chunk_id=CHUNK_ID), timeout=5)
        delete_call = stub.DeleteChunk.future(
            storage_pb2.DeleteChunkRequest(chunk_id=CHUNK_ID), timeout=5
        )
        assert get_entered.wait(timeout=2) and delete_entered.wait(timeout=2)
        assert not get_call.done() and not delete_call.done()
        assert health(stub).used_bytes == 0
    finally:
        release.set()
    assert not store_call.result(timeout=3).already_existed
    assert delete_call.result(timeout=3).existed
    try:
        snapshot = get_call.result(timeout=3)
    except grpc.RpcError as error:
        assert error.code() == grpc.StatusCode.NOT_FOUND  # Delete acquired the lock first.
    else:
        assert snapshot.data == DATA and snapshot.checksum_sha256 == request().checksum_sha256
    assert list(directory.iterdir()) == [] and health(stub).used_bytes == 0


def test_ack_loss_retry_then_delete_preserves_accounting(node, monkeypatch):
    stub, service, directory = node
    committed = Event()
    release = Event()
    finished = Event()
    original_put = service.store.put

    def lose_ack(*args):
        result = original_put(*args)
        committed.set()
        try:
            assert release.wait(timeout=5)
            return result
        finally:
            finished.set()

    monkeypatch.setattr(service.store, "put", lose_ack)
    call = stub.StoreChunk.future(request(), timeout=5)
    try:
        assert committed.wait(timeout=2)
        assert call.cancel()
        with pytest.raises(grpc.FutureCancelledError):
            call.result(timeout=2)
        assert health(stub).used_bytes == len(DATA)
    finally:
        release.set()
    assert finished.wait(timeout=2)
    monkeypatch.setattr(service.store, "put", original_put)
    assert stub.StoreChunk(request(), timeout=3).already_existed
    assert health(stub).used_bytes == len(DATA)
    assert stub.DeleteChunk(storage_pb2.DeleteChunkRequest(chunk_id=CHUNK_ID), timeout=2).existed
    assert not stub.DeleteChunk(
        storage_pb2.DeleteChunkRequest(chunk_id=CHUNK_ID), timeout=2
    ).existed
    assert health(stub).used_bytes == 0 and list(directory.iterdir()) == []


@pytest.mark.parametrize("stage", ["open", "short_write", "flush", "fsync"])
def test_health_probe_failure_reports_unwritable_and_keeps_accounting(node, monkeypatch, stage):
    stub, _, directory = node
    stub.StoreChunk(request(), timeout=3)
    original_temporary = __import__("tempfile").TemporaryFile

    def fail(*args, **kwargs):
        raise OSError(errno.EIO, "private health probe detail")

    class Probe:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.handle.close()

        def write(self, data):
            return self.handle.write(data[:1] if stage == "short_write" else data)

        def flush(self):
            if stage == "flush":
                fail()
            self.handle.flush()

        def fileno(self):
            return self.handle.fileno()

    def temporary(*args, **kwargs):
        return Probe(original_temporary(*args, **kwargs))

    monkeypatch.setattr(
        "storage.service.tempfile.TemporaryFile", fail if stage == "open" else temporary
    )
    if stage == "fsync":
        monkeypatch.setattr("storage.service.os.fsync", fail)
    result = health(stub)
    assert not result.storage_writable and result.used_bytes == len(DATA)
    assert [path.name for path in directory.iterdir()] == [f"{CHUNK_ID}.chunk"]


@pytest.mark.parametrize("error_type", [OSError, RuntimeError])
def test_health_capacity_failure_is_sanitized(node, monkeypatch, error_type):
    stub, service, directory = node

    def fail(*args):
        raise error_type("private capacity detail")

    monkeypatch.setattr("storage.service.shutil.disk_usage", fail)
    with pytest.raises(grpc.RpcError) as error:
        health(stub)
    expected = (
        grpc.StatusCode.FAILED_PRECONDITION if error_type is OSError else grpc.StatusCode.INTERNAL
    )
    assert error.value.code() == expected and "private" not in error.value.details()
    assert list(directory.iterdir()) == [] and service.store.used_bytes == 0


@pytest.mark.parametrize("operation", ["put", "get", "delete"])
def test_unexpected_data_operation_failure_is_sanitized(node, monkeypatch, operation):
    stub, service, directory = node

    def fail(*args):
        raise RuntimeError("private unexpected detail")

    monkeypatch.setattr(service.store, operation, fail)
    rpc, payload = {
        "put": (stub.StoreChunk, request()),
        "get": (stub.GetChunk, storage_pb2.GetChunkRequest(chunk_id=CHUNK_ID)),
        "delete": (stub.DeleteChunk, storage_pb2.DeleteChunkRequest(chunk_id=CHUNK_ID)),
    }[operation]
    with pytest.raises(grpc.RpcError) as error:
        rpc(payload, timeout=2)
    assert error.value.code() == grpc.StatusCode.INTERNAL
    assert "private" not in error.value.details()
    assert list(directory.iterdir()) == [] and service.store.used_bytes == 0
