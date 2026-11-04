import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from uuid import uuid4

import grpc
import pytest
import v2_storage_pb2 as pb
import v2_storage_pb2_grpc as rpc

from storage.v2.ledger import Ledger, PreconditionError, Scope, Spec
from storage.v2.server import create_server
from storage.v2.service import StorageService
from storage.v2.streaming import BusyError, DataError, StreamingStore

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX filesystem gate")


@pytest.fixture
def storage(tmp_path):
    scope = Scope(str(uuid4()), str(uuid4()), "test-v2", str(uuid4()))
    ledger = Ledger(tmp_path / "v2", scope, provision=True)
    try:
        yield StreamingStore(ledger, drain_timeout=0.05), scope
    finally:
        ledger.close()


def prepare(store, scope, data=b"abc"):
    allocation = str(uuid4())
    spec = Spec(str(uuid4()), len(data), hashlib.sha256(data).hexdigest())
    store.prepare(scope, allocation, 1, spec)
    return allocation, spec


def test_store_replay_validates_bytes(storage):
    store, scope = storage
    allocation, spec = prepare(store, scope)
    row, replay = store.store(scope, allocation, 1, spec, [(0, b"abc")])
    assert row["state"] == "STORED" and not replay
    repeated, replay = store.store(scope, allocation, 1, spec, [(0, b"a"), (1, b"bc")])
    assert replay and repeated == row
    with pytest.raises(DataError):
        store.store(scope, allocation, 1, spec, [(0, b"bad")])
    assert (store.ledger.root / "chunks" / allocation).read_bytes() == b"abc"


@pytest.mark.parametrize(
    "blocks", [[(1, b"abc")], [(0, b"a"), (0, b"bc")], [(0, b"")], [(0, b"abcd")]]
)
def test_malformed_stream_cleanup(storage, blocks):
    store, scope = storage
    allocation, spec = prepare(store, scope)
    with pytest.raises(ValueError):
        store.store(scope, allocation, 1, spec, blocks)
    assert store.ledger.lookup(allocation)["state"] == "PREPARED"
    assert not list((store.ledger.root / "tmp").iterdir())


def test_interrupted_stream_retry(storage):
    store, scope = storage
    allocation, spec = prepare(store, scope)

    def broken():
        yield 0, b"a"
        raise ConnectionError("transport dropped")

    with pytest.raises(ConnectionError):
        store.store(scope, allocation, 1, spec, broken())
    assert store.ledger.lookup(allocation)["state"] == "PREPARED"
    assert not list((store.ledger.root / "tmp").iterdir())
    assert store.store(scope, allocation, 1, spec, [(0, b"abc")])[0]["state"] == "STORED"


@pytest.mark.parametrize("operation", ["prepare", "delete"])
def test_slow_writer_fence_and_other_allocation_progress(storage, operation):
    store, scope = storage
    allocation, spec = prepare(store, scope)
    waiting, release = Event(), Event()

    def slow():
        yield 0, b"a"
        waiting.set()
        assert release.wait(5)
        yield 1, b"bc"

    with ThreadPoolExecutor(1) as executor:
        writing = executor.submit(store.store, scope, allocation, 1, spec, slow())
        try:
            assert waiting.wait(5)
            with pytest.raises(BusyError):
                if operation == "prepare":
                    store.prepare(scope, allocation, 2, spec)
                else:
                    store.delete(scope, allocation, 2)
            assert store.ledger.lookup(allocation)["generation"] == 2
            other, other_spec = prepare(store, scope)
            assert store.store(scope, other, 1, other_spec, [(0, b"abc")])[0]["state"] == "STORED"
            assert len(list((store.ledger.root / "tmp").iterdir())) == 1
        finally:
            release.set()
        with pytest.raises(PreconditionError):
            writing.result(timeout=5)
    assert not (store.ledger.root / "chunks" / allocation).exists()
    assert not list((store.ledger.root / "tmp").iterdir())
    if operation == "delete":
        assert store.delete(scope, allocation, 2)["cleanup_complete"]
    else:
        store.prepare(scope, allocation, 2, spec)
        assert store.store(scope, allocation, 2, spec, [(0, b"abc")])[0]["state"] == "STORED"


def test_disk_full_keeps_intent_without_ack(storage, monkeypatch):
    store, scope = storage
    allocation, spec = prepare(store, scope)
    original = os.fsync
    failed = False

    def full(descriptor):
        nonlocal failed
        if not failed:
            failed = True
            raise OSError(28, "injected ENOSPC at fsync")
        return original(descriptor)

    monkeypatch.setattr(os, "fsync", full)
    with pytest.raises(OSError):
        store.store(scope, allocation, 1, spec, [(0, b"abc")])
    assert store.ledger.lookup(allocation)["state"] == "PREPARED"
    assert not (store.ledger.root / "chunks" / allocation).exists()


def test_actual_grpc_all_seven_rpcs(storage):
    store, scope = storage
    server, port, executor = create_server(store.ledger)
    server.start()
    wire_scope = pb.NodeScope(
        namespace_id=scope.namespace_id,
        recovery_epoch=scope.recovery_epoch,
        node_id=scope.node_id,
        node_incarnation=scope.node_incarnation,
    )
    key = pb.AllocationKey(scope=wire_scope, allocation_id=str(uuid4()))
    chunk = pb.ChunkSpec(
        chunk_id=str(uuid4()), size_bytes=3, checksum_sha256=hashlib.sha256(b"abc").hexdigest()
    )
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            grpc.channel_ready_future(channel).result(timeout=5)
            stub = rpc.StorageServiceStub(channel)
            assert (
                stub.HealthCheck(pb.HealthCheckRequest(scope=wire_scope), timeout=5).scope
                == wire_scope
            )
            prepared = stub.PrepareAllocation(
                pb.PrepareAllocationRequest(key=key, generation=1, chunk=chunk), timeout=5
            )
            assert prepared.state == pb.PREPARED
            header = pb.StoreChunkFrame(
                header=pb.StoreChunkHeader(key=key, generation=1, chunk=chunk)
            )
            block = pb.StoreChunkFrame(block=pb.DataBlock(offset=0, data=b"abc"))
            ack = stub.StoreChunk(iter([header, block]), timeout=5)
            assert ack.allocation.state == pb.STORED and not ack.already_stored
            assert stub.StoreChunk(iter([header, block]), timeout=5).already_stored
            frames = list(
                stub.GetChunk(pb.GetChunkRequest(key=key, expected_chunk=chunk), timeout=5)
            )
            assert [f.WhichOneof("payload") for f in frames] == ["header", "block", "footer"]
            assert frames[1].block.data == b"abc"
            assert frames[-1].footer.actual_checksum_sha256 == chunk.checksum_sha256
            request = pb.AllocationRequest(key=key, generation=1)
            assert stub.InspectAllocation(request, timeout=5).actual_size_bytes == 3
            listing = stub.ListAllocations(
                pb.ListAllocationsRequest(scope=wire_scope, limit=1), timeout=5
            )
            assert len(listing.items) == 1
            assert stub.DeleteAllocation(request, timeout=5).cleanup_complete
            assert stub.InspectAllocation(request, timeout=5).state == pb.DELETED
            with pytest.raises(grpc.RpcError) as failure:
                stub.PrepareAllocation(
                    pb.PrepareAllocationRequest(key=key, generation=2, chunk=chunk), timeout=5
                )
            assert failure.value.code() == grpc.StatusCode.FAILED_PRECONDITION
            assert not (store.ledger.root / "chunks" / key.allocation_id).exists()
    finally:
        server.stop(0).wait()
        executor.shutdown(wait=True)


def test_offline_epoch_adoption_never_reopens_tombstone(storage):
    store, scope = storage
    retained, spec = prepare(store, scope)
    store.store(scope, retained, 1, spec, [(0, b"abc")])
    quarantined, other_spec = prepare(store, scope)
    store.store(scope, quarantined, 1, other_spec, [(0, b"abc")])
    deleted = str(uuid4())
    store.delete(scope, deleted, 1)
    root = store.ledger.root
    store.ledger.close()
    epoch = str(uuid4())
    administrator = Ledger(root, scope, maintenance=True)
    try:
        administrator.begin_recovery(epoch)
        with pytest.raises(PreconditionError):
            administrator.recover()
        administrator.adopt(retained)
        with pytest.raises(PreconditionError):
            administrator.adopt(deleted)
        administrator.adopt(deleted, close=True)
        administrator.activate_epoch(epoch)
    finally:
        administrator.close()
    with pytest.raises(PreconditionError):
        Ledger(root, scope)
    new_scope = Scope(scope.namespace_id, epoch, scope.node_id, scope.node_incarnation)
    reopened = Ledger(root, new_scope)
    try:
        assert reopened.inspect(new_scope, retained, 1)["state"] == "STORED"
        with pytest.raises(PreconditionError):
            reopened.inspect(new_scope, quarantined, 1)
        with pytest.raises(PreconditionError):
            reopened.fence_delete(new_scope, quarantined, 1)
        assert (root / "chunks" / quarantined).read_bytes() == b"abc"
        with pytest.raises(PreconditionError):
            reopened.prepare(new_scope, deleted, 999, spec)
    finally:
        reopened.close()


@pytest.mark.parametrize("point", ["temp_fsync", "rename_fsync", "stored_commit", "delete_fence"])
def test_kill_real_streaming_boundaries(storage, point):
    import selectors
    import subprocess
    import sys

    store, scope = storage
    root = store.ledger.root
    store.ledger.close()
    allocation, chunk_id = str(uuid4()), str(uuid4())
    code = """
import hashlib, signal, sys
from pathlib import Path
from storage.v2.ledger import Ledger, Scope, Spec
from storage.v2.streaming import StreamingStore
root, ns, epoch, node, inc, aid, cid, point = sys.argv[1:]
scope = Scope(ns,epoch,node,inc)
ledger = Ledger(Path(root),scope)
def boundary(name):
    if name == point:
        print('BOUNDARY',flush=True)
        signal.pause()
store = StreamingStore(ledger,hook=boundary)
spec = Spec(cid,3,hashlib.sha256(b'abc').hexdigest())
store.prepare(scope,aid,1,spec)
store.store(scope,aid,1,spec,[(0,b'abc')])
if point == 'delete_fence':
    store.delete(scope,aid,1)
"""
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            code,
            str(root),
            scope.namespace_id,
            scope.recovery_epoch,
            scope.node_id,
            scope.node_incarnation,
            allocation,
            chunk_id,
            point,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=10)
            assert child.stdout.readline().strip() == "BOUNDARY"
    finally:
        child.kill()
        child.communicate(timeout=10)
    recovered = Ledger(root, scope)
    try:
        row = recovered.inspect(scope, allocation, 1)
        if point == "temp_fsync":
            assert row["state"] == "PREPARED"
            assert not (root / "chunks" / allocation).exists()
        elif point == "delete_fence":
            assert row["state"] == "DELETED" and row["cleanup_complete"]
            assert not (root / "chunks" / allocation).exists()
        else:
            assert row["state"] == "STORED" and recovered.verify_final(row)
        assert not list((root / "tmp").iterdir())
    finally:
        recovered.close()


def test_real_disk_full_preserves_tombstone():
    import errno
    import tempfile
    from pathlib import Path

    configured = os.environ.get("V2_DISK_FULL_DIR")
    if not configured:
        pytest.skip("Requires compose.v2-tests.yml bounded 8 MiB tmpfs")
    with tempfile.TemporaryDirectory(dir=configured) as directory:
        root = Path(directory)
        scope = Scope(str(uuid4()), str(uuid4()), "disk-full", str(uuid4()))
        ledger = Ledger(root, scope, provision=True)
        store = StreamingStore(ledger)
        allocation, spec = prepare(store, scope)
        store.store(scope, allocation, 1, spec, [(0, b"abc")])
        filler = root / "owned-test-filler"
        try:
            with filler.open("wb", buffering=0) as handle:
                while True:
                    try:
                        handle.write(b"x" * 65536)
                    except OSError as exc:
                        assert exc.errno == errno.ENOSPC
                        break
            assert not ledger.writable(1)
            assert store.delete(scope, allocation, 2)["cleanup_complete"]
            assert not (root / "chunks" / allocation).exists()
            with pytest.raises(PreconditionError):
                store.prepare(scope, allocation, 99, spec)
        finally:
            filler.unlink(missing_ok=True)
            ledger.close()
        recovered = Ledger(root, scope)
        try:
            row = recovered.inspect(scope, allocation, 2)
            assert row["state"] == "DELETED" and row["cleanup_complete"]
            assert recovered.writable(1)
        finally:
            recovered.close()


@pytest.mark.parametrize("category", ["temporary", "ledger"])
@pytest.mark.parametrize("race", ["after_is_file", "before_stat"])
def test_health_tolerates_files_removed_during_scan(storage, monkeypatch, category, race):
    store, scope = storage
    root = store.ledger.root
    stable = root / "tmp" / "stable.part"
    stable.write_bytes(b"data")
    transient = (
        root / "tmp" / "finished.part"
        if category == "temporary"
        else root / "ledger.sqlite3-checkpoint-review"
    )
    transient.write_bytes(b"transient")
    original_stat, original_is_file = Path.stat, Path.is_file

    def removed_before_stat(path, *args, **kwargs):
        if path == transient:
            path.unlink(missing_ok=True)
        return original_stat(path, *args, **kwargs)

    def removed_after_is_file(path):
        result = original_is_file(path)
        if path == transient and result:
            path.unlink()
        return result

    if race == "before_stat":
        monkeypatch.setattr(Path, "stat", removed_before_stat)
    else:
        # Regression for the former two-stat path: removal between is_file and
        # stat used to turn an ordinary write/checkpoint into Health INTERNAL.
        monkeypatch.setattr(Path, "is_file", removed_after_is_file)

    class Context:
        def time_remaining(self):
            return 5

        def is_active(self):
            return True

        def abort(self, code, message):
            pytest.fail(f"Health unexpectedly aborted: {code}: {message}")

    request = pb.HealthCheckRequest(
        scope=pb.NodeScope(
            namespace_id=scope.namespace_id,
            recovery_epoch=scope.recovery_epoch,
            node_id=scope.node_id,
            node_incarnation=scope.node_incarnation,
        )
    )
    response = StorageService(store, "test-domain").HealthCheck(request, Context())
    assert response.scope == request.scope
    assert response.failure_domain == "test-domain"
    assert response.temporary_bytes >= len(b"data")
    assert response.ledger_bytes >= (root / "ledger.sqlite3").stat().st_size
