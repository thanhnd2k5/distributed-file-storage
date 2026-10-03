import hashlib
from threading import Event, Lock

import pytest
import storage_pb2 as pb

ID = "11111111-1111-4111-8111-111111111111"
OTHER = "22222222-2222-4222-8222-222222222222"


def store(stub, data, chunk_id=ID):
    return stub.StoreChunk(
        pb.StoreChunkRequest(
            chunk_id=chunk_id, data=data, checksum_sha256=hashlib.sha256(data).hexdigest()
        ),
        timeout=3,
    )


@pytest.mark.parametrize("restored", [b"abcdefgh", b"short"])
@pytest.mark.parametrize("startup", [False, True])
def test_restore_missing_chunk_accounts_replacement_once_and_delete_returns_to_baseline(
    storage_node_factory, tmp_path, restored, startup
):
    if startup:
        (tmp_path / f"{ID}.chunk").write_bytes(b"abcdefgh")
    stub, service, directory = storage_node_factory()
    if not startup:
        store(stub, b"abcdefgh")
    store(stub, b"other", OTHER)
    service.store.chunk_path(ID).unlink()
    assert not store(stub, restored).already_existed
    assert store(stub, restored).already_existed
    health = stub.HealthCheck(pb.HealthCheckRequest(), timeout=1)
    assert health.used_bytes == len(restored) + len(b"other")
    assert stub.GetChunk(pb.GetChunkRequest(chunk_id=ID), timeout=1).data == restored
    assert stub.DeleteChunk(pb.DeleteChunkRequest(chunk_id=ID), timeout=1).existed
    assert not stub.DeleteChunk(pb.DeleteChunkRequest(chunk_id=ID), timeout=1).existed
    assert service.store.used_bytes == len(b"other")
    assert stub.DeleteChunk(pb.DeleteChunkRequest(chunk_id=OTHER), timeout=1).existed
    assert service.store.used_bytes == 0 and list(directory.iterdir()) == []


def test_health_has_executor_capacity_when_all_four_transfer_workers_wait_for_lock(
    storage_node_factory,
):
    stub, service, directory = storage_node_factory()
    arrived = Event()
    finished = Event()

    class ObservedLock:
        def __init__(self):
            self.lock = Lock()
            self.count_lock = Lock()
            self.entered = 0
            self.exited = 0

        def __enter__(self):
            with self.count_lock:
                self.entered += 1
                if self.entered == 4:
                    arrived.set()
            self.lock.acquire()
            return self

        def __exit__(self, *args):
            self.lock.release()
            with self.count_lock:
                self.exited += 1
                if self.exited == 4:
                    finished.set()

    observed = ObservedLock()
    service.store.operation_lock = observed
    observed.lock.acquire()
    calls = []
    try:
        for _ in range(4):
            calls.append(
                stub.StoreChunk.future(
                    pb.StoreChunkRequest(
                        chunk_id=ID,
                        data=b"chunk",
                        checksum_sha256=hashlib.sha256(b"chunk").hexdigest(),
                    ),
                    timeout=5,
                )
            )
        assert arrived.wait(timeout=3)
        response = stub.HealthCheck(pb.HealthCheckRequest(), timeout=1)
        assert response.storage_writable and response.used_bytes == 0
        assert all(not call.done() for call in calls)
    finally:
        observed.lock.release()
    for call in calls:
        call.result(timeout=3)
    assert finished.wait(timeout=2)
    assert service.store.used_bytes == len(b"chunk")
    assert len(list(directory.iterdir())) == 1
