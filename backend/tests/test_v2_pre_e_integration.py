"""Regression for concurrent same-file HTTP reads and durable Storage fencing."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from threading import Event, Lock, get_ident

import grpc
import httpx
import pytest
import v2_storage_pb2 as pb
from test_v2_c2 import cluster as cluster
from test_v2_c3 import http_cluster as http_cluster
from test_v2_c3 import until
from test_v2_d4 import published, released
from test_v2_metadata import foundation as foundation
from test_v2_metadata import sql
from test_v2_streaming import prepare
from test_v2_streaming import storage as storage

from storage.v2.service import StorageService
from storage.v2.streaming import BusyError


def test_four_readers_same_file_are_not_limited_by_replica_count(http_cluster, monkeypatch):
    repo, resources, _, _, _, _, _ = http_cluster
    # Hold each real GetChunk while it owns the read handle to force overlap.
    _, path = published(http_cluster, b"abc")
    release, mutex = Event(), Lock()
    entered = []
    for ledger, _, _ in resources:
        original = ledger.require_scope
        seen = set()

        def hold(scope, *, original=original, seen=seen, node=ledger.scope.node_id):
            original(scope)
            # GetChunk calls require_scope both before and during streaming. The
            # second call runs while the shared read handle is owned.
            thread = get_ident()
            with mutex:
                if thread not in seen:
                    seen.add(thread)
                    return
                entered.append(node)
            while not release.wait(0.01):
                pass

        monkeypatch.setattr(ledger, "require_scope", hold)
    transfer = http_cluster[4]
    headers = [{}, {"Range": "bytes=0-0"}, {"Range": "bytes=1-"}, {"Range": "bytes=-1"}]
    with ThreadPoolExecutor(4) as pool:
        futures = [pool.submit(httpx.get, transfer + path, headers=h, timeout=5) for h in headers]
        try:
            until(lambda: len(entered) >= 4, 3)
            assert entered.count("node-0") >= 4
            assert len(sql(repo, "SELECT * FROM download_leases WHERE state='ACTIVE'")) == 4
        finally:
            release.set()
        responses = [future.result(timeout=5) for future in futures]
    assert [r.status_code for r in responses] == [200, 206, 206, 206]
    assert [r.content for r in responses] == [b"abc", b"a", b"bc", b"c"]
    assert len({r.headers["ETag"] for r in responses}) == 1
    released(repo)


class Rejected(Exception):
    def __init__(self, code):
        self.code = code


class Context:
    def time_remaining(self):
        return 5

    def is_active(self):
        return True

    def abort(self, code, message):
        raise Rejected(code)


def request_for(scope, allocation, spec):
    return pb.GetChunkRequest(
        key=pb.AllocationKey(
            scope=pb.NodeScope(
                namespace_id=scope.namespace_id,
                recovery_epoch=scope.recovery_epoch,
                node_id=scope.node_id,
                node_incarnation=scope.node_incarnation,
            ),
            allocation_id=allocation,
        ),
        expected_chunk=pb.ChunkSpec(
            chunk_id=spec.chunk_id, size_bytes=spec.size_bytes, checksum_sha256=spec.checksum_sha256
        ),
    )


def test_delete_fences_new_reads_but_drains_every_existing_reader(storage):
    store, scope = storage
    allocation, spec = prepare(store, scope)
    store.store(scope, allocation, 1, spec, [(0, b"abc")])
    service = StorageService(store, "test")
    request = request_for(scope, allocation, spec)
    with ExitStack() as stack:
        readers = [service.GetChunk(request, Context()) for _ in range(3)]
        for reader in readers:
            stack.callback(reader.close)
            assert next(reader).WhichOneof("payload") == "header"
        final = store.ledger.root / "chunks" / allocation
        with pytest.raises(BusyError):
            store.store(scope, allocation, 1, spec, [(0, b"abc")])
        with pytest.raises(BusyError):
            store.prepare(scope, allocation, 2, spec)
        assert store.ledger.lookup(allocation)["generation"] == 1
        with pytest.raises(BusyError):
            store.delete(scope, allocation, 1)
        assert final.read_bytes() == b"abc"
        assert store.ledger.lookup(allocation)["state"] == "DELETED"
        assert not store.ledger.lookup(allocation)["cleanup_complete"]
        with pytest.raises(Rejected) as fenced:
            next(service.GetChunk(request, Context()))
        assert fenced.value.code == grpc.StatusCode.FAILED_PRECONDITION
        readers[0].close()  # Cancellation of one reader cannot release the others.
        with pytest.raises(BusyError):
            store.delete(scope, allocation, 1)
        readers[1].close()
        with pytest.raises(BusyError):
            store.delete(scope, allocation, 1)
        frames = list(readers[2])
        assert (
            b"".join(f.block.data for f in frames if f.WhichOneof("payload") == "block") == b"abc"
        )
        assert frames[-1].WhichOneof("payload") == "footer"
    assert store.delete(scope, allocation, 1)["cleanup_complete"]
    assert not final.exists() and not store._owners


def test_prepare_blocks_late_readers_until_existing_read_handle_drains(storage):
    store, scope = storage
    allocation, spec = prepare(store, scope)
    store.store(scope, allocation, 1, spec, [(0, b"abc")])
    store.drain_timeout = 1
    service = StorageService(store, "test")
    request = request_for(scope, allocation, spec)
    reader = service.GetChunk(request, Context())
    assert next(reader).WhichOneof("payload") == "header"
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(store.prepare, scope, allocation, 2, spec)
        try:
            until(lambda: store._owners[allocation].writer.locked())
            with pytest.raises(Rejected) as busy:
                next(service.GetChunk(request, Context()))
            assert busy.value.code == grpc.StatusCode.ABORTED
        finally:
            reader.close()
        assert future.result(timeout=2)["generation"] == 2
    frames = list(service.GetChunk(request, Context()))
    assert frames[0].header.generation == 2
    assert store.delete(scope, allocation, 2)["cleanup_complete"]
