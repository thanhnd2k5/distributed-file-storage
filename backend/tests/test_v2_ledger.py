"""B1: real SQLite/filesystem and process death; POSIX durability gate only."""

import hashlib
import os
import sqlite3
import subprocess
import sys
from uuid import uuid4

import pytest

from storage.v2 import ledger as ledger_module
from storage.v2 import streaming as streaming_module
from storage.v2.ledger import (
    ConflictError,
    Ledger,
    MissingError,
    PreconditionError,
    Scope,
    Spec,
)
from storage.v2.streaming import StreamingStore

pytestmark = pytest.mark.skipif(os.name != "posix", reason="Requires POSIX fsync/flock")


@pytest.fixture
def scope():
    return Scope(str(uuid4()), str(uuid4()), "b-test", str(uuid4()))


@pytest.fixture
def ledger(tmp_path, scope):
    instance = Ledger(tmp_path / "v2", scope, provision=True)
    try:
        yield instance
    finally:
        instance.close()


def spec(data=b"abc"):
    return Spec(str(uuid4()), len(data), hashlib.sha256(data).hexdigest())


def test_prepare_replay_restart_conflict(ledger, scope):
    allocation = str(uuid4())
    chunk = spec()
    first = ledger.prepare(scope, allocation, 1, chunk)
    assert ledger.prepare(scope, allocation, 1, chunk) == first
    ledger.close()
    reopened = Ledger(ledger.root, scope)
    try:
        assert reopened.inspect(scope, allocation, 1) == first
        with pytest.raises(ConflictError):
            reopened.prepare(scope, allocation, 2, spec(b"other"))
        assert reopened.lookup(allocation) == first
        newer = reopened.prepare(scope, allocation, 2, chunk)
        assert newer["inventory_sequence"] == first["inventory_sequence"]
        for generation in (1, 3):
            with pytest.raises(PreconditionError):
                reopened.inspect(scope, allocation, generation)
    finally:
        reopened.close()


def test_delete_before_prepare_and_retry(ledger, scope):
    allocation = str(uuid4())
    with pytest.raises(MissingError):
        ledger.inspect(scope, allocation, 1)
    fence = ledger.fence_delete(scope, allocation, 3)
    assert fence["state"] == "DELETED" and not fence["cleanup_complete"]
    done = ledger.cleanup_deleted(allocation)
    assert done["cleanup_complete"]
    for generation in (1, 3, 10):
        with pytest.raises(PreconditionError):
            ledger.prepare(scope, allocation, generation, spec())
    with pytest.raises(PreconditionError):
        ledger.fence_delete(scope, allocation, 2)
    ledger.fence_delete(scope, allocation, 3)
    assert ledger.cleanup_deleted(allocation) == done


def test_scope_gate_and_exclusive_volume(ledger, scope):
    with pytest.raises((BlockingIOError, OSError)):
        Ledger(ledger.root, scope)
    wrong = Scope(scope.namespace_id, scope.recovery_epoch, scope.node_id, str(uuid4()))
    with pytest.raises(PreconditionError):
        ledger.inspect(wrong, str(uuid4()), 1)
    with ledger.transaction() as db:
        db.execute("UPDATE maintenance SET mode='RECOVERING'")
    with pytest.raises(PreconditionError):
        ledger.prepare(scope, str(uuid4()), 1, spec())
    ledger.close()
    with pytest.raises(PreconditionError):
        Ledger(ledger.root, wrong)


def test_missing_ledger_never_adopts_old_bytes(tmp_path, scope):
    root = tmp_path / "lost"
    (root / "chunks").mkdir(parents=True)
    old = root / "chunks" / str(uuid4())
    old.write_bytes(b"old")
    with pytest.raises(PreconditionError):
        Ledger(root, scope, provision=True)
    assert old.read_bytes() == b"old"
    assert not (root / "ledger.sqlite3").exists()


@pytest.mark.parametrize("point", ["prepare", "temp", "rename", "stored", "deleted"])
def test_process_kill_recovery(tmp_path, scope, point):
    root = tmp_path / "volume"
    allocation = str(uuid4())
    chunk = spec()
    # The child signals only after the selected durable boundary. Parent sends
    # SIGKILL, so no Python finally/SQLite close or cooperative shutdown can run.
    code = """
import os, signal, sys
from pathlib import Path
from storage.v2.ledger import Ledger, Scope, Spec, fsync_directory
root, ns, epoch, node, incarnation, aid, cid, checksum, point = sys.argv[1:]
scope = Scope(ns, epoch, node, incarnation)
ledger = Ledger(Path(root), scope, provision=True)
ledger.prepare(scope, aid, 1, Spec(cid, 3, checksum))
if point != 'prepare':
    temporary = f'{aid}.1.part'
    with ledger.transaction() as db:
        db.execute("UPDATE allocations SET state='WRITING',temp_name=? "
                   "WHERE allocation_id=?", (temporary,aid))
    with (Path(root)/'tmp'/temporary).open('xb') as handle:
        handle.write(b'abc')
        handle.flush()
        os.fsync(handle.fileno())
    if point in ('rename','stored','deleted'):
        (Path(root)/'tmp'/temporary).rename(Path(root)/'chunks'/aid)
        fsync_directory(Path(root)/'chunks')
        fsync_directory(Path(root)/'tmp')
    if point in ('stored','deleted'):
        with ledger.transaction() as db:
            db.execute("UPDATE allocations SET state='STORED',actual_size=3,actual_hash=?,"
                       "temp_name=NULL WHERE allocation_id=?", (checksum,aid))
    if point == 'deleted':
        ledger.fence_delete(scope, aid, 1)
print('BOUNDARY', flush=True)
signal.pause()
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
            chunk.chunk_id,
            chunk.checksum_sha256,
            point,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        # communicate timeout bounds even a child failing before the boundary.
        import selectors

        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=10), "Child did not reach durable boundary"
            assert child.stdout.readline().strip() == "BOUNDARY"
    finally:
        child.kill()
        child.communicate(timeout=10)
    reopened = Ledger(root, scope)
    try:
        row = reopened.inspect(scope, allocation, 1)
        expected = {
            "prepare": "PREPARED",
            "temp": "PREPARED",
            "rename": "STORED",
            "stored": "STORED",
            "deleted": "DELETED",
        }[point]
        assert row["state"] == expected
        assert not list((root / "tmp").iterdir())
        if expected == "STORED":
            assert (root / "chunks" / allocation).read_bytes() == b"abc"
            assert row["actual_size"] == 3 and row["actual_hash"] == chunk.checksum_sha256
        if expected == "DELETED":
            assert row["cleanup_complete"]
            assert not (root / "chunks" / allocation).exists()
            with pytest.raises(PreconditionError):
                reopened.prepare(scope, allocation, 999, chunk)
        rows, _, _ = reopened.inventory(scope)
        assert len(rows) == 1
    finally:
        reopened.close()


def test_stored_corruption_and_unowned_quarantine(ledger, scope):
    allocation = str(uuid4())
    chunk = spec()
    ledger.prepare(scope, allocation, 1, chunk)
    with ledger.transaction() as db:
        db.execute(
            "UPDATE allocations SET state='STORED',actual_size=3,actual_hash=? "
            "WHERE allocation_id=?",
            (chunk.checksum_sha256, allocation),
        )
    (ledger.root / "chunks" / allocation).write_bytes(b"bad")
    orphan = ledger.root / "chunks" / str(uuid4())
    orphan.write_bytes(b"unknown")
    ledger.recover()
    assert ledger.lookup(allocation)["state"] == "CORRUPT"
    assert (ledger.root / "chunks" / allocation).read_bytes() == b"bad"
    assert not orphan.exists()
    assert [p.read_bytes() for p in (ledger.root / "quarantine").iterdir()] == [b"unknown"]


def test_prepared_without_write_intent_quarantines_unowned_final(ledger, scope):
    allocation = str(uuid4())
    ledger.prepare(scope, allocation, 1, spec())
    (ledger.root / "chunks" / allocation).write_bytes(b"unowned")
    ledger.recover()
    assert ledger.lookup(allocation)["state"] == "PREPARED"
    assert not (ledger.root / "chunks" / allocation).exists()
    assert [p.read_bytes() for p in (ledger.root / "quarantine").iterdir()] == [b"unowned"]


def test_inventory_bound_and_tombstone(ledger, scope):
    for _ in range(3):
        ledger.fence_delete(scope, str(uuid4()), 1)
    first, upper, more = ledger.inventory(scope, limit=2)
    assert len(first) == 2 and more
    ledger.prepare(scope, str(uuid4()), 1, spec())
    second, bound, more = ledger.inventory(scope, first[-1]["inventory_sequence"], upper, 2)
    assert len(second) == 1 and bound == upper and not more
    assert all(row["state"] == "DELETED" for row in first + second)


@pytest.mark.parametrize("generation", [0, -1, 2**63, True])
def test_invalid_generation_no_intent(ledger, scope, generation):
    with pytest.raises(ValueError):
        ledger.prepare(scope, str(uuid4()), generation, spec())
    assert ledger.inventory(scope)[0] == []


def test_transaction_rollback(ledger, scope):
    with pytest.raises(RuntimeError):
        with ledger.transaction() as db:
            db.execute("UPDATE maintenance SET mode='RECOVERING'")
            raise RuntimeError("crash before commit")
    ledger.require_scope(scope)


@pytest.mark.parametrize("failure", [None, "file", "directory"])
def test_recovery_flushes_final_before_stored(ledger, scope, monkeypatch, failure):
    allocation = str(uuid4())
    chunk = spec()
    store = StreamingStore(ledger)
    store.prepare(scope, allocation, 1, chunk)
    root = ledger.root
    original_directory_flush = ledger_module.fsync_directory

    def failed_rename_flush(path):
        if path == root / "chunks":
            raise OSError(5, "directory fsync failed after rename")
        original_directory_flush(path)

    with monkeypatch.context() as patch:
        patch.setattr(streaming_module, "fsync_directory", failed_rename_flush)
        with pytest.raises(OSError):
            store.store(scope, allocation, 1, chunk, [(0, b"abc")])
    assert ledger.lookup(allocation)["state"] == "WRITING"
    ledger.close()

    def persisted_state():
        db = sqlite3.connect(root / "ledger.sqlite3")
        try:
            return db.execute(
                "SELECT state FROM allocations WHERE allocation_id=?", (allocation,)
            ).fetchone()[0]
        finally:
            db.close()

    final_stat = (root / "chunks" / allocation).stat()
    original_file_flush = os.fsync
    flushed = []

    def file_flush(descriptor):
        observed = os.fstat(descriptor)
        if (observed.st_dev, observed.st_ino) == (final_stat.st_dev, final_stat.st_ino):
            assert persisted_state() == "WRITING"
            flushed.append("file")
            if failure == "file":
                raise OSError(5, "recovery file fsync failed")
        original_file_flush(descriptor)

    def directory_flush(path):
        if path == root / "chunks":
            assert persisted_state() == "WRITING"
            flushed.append("directory")
            if failure == "directory":
                raise OSError(5, "recovery directory fsync failed")
        original_directory_flush(path)

    with monkeypatch.context() as patch:
        patch.setattr(ledger_module.os, "fsync", file_flush)
        patch.setattr(ledger_module, "fsync_directory", directory_flush)
        if failure:
            with pytest.raises(OSError, match="recovery"):
                Ledger(root, scope)
            assert persisted_state() == "WRITING"
        else:
            reopened = Ledger(root, scope)
            try:
                assert reopened.lookup(allocation)["state"] == "STORED"
                assert flushed == ["file", "directory"]
            finally:
                reopened.close()
    # A failed recovery released volume ownership and can be retried after the
    # filesystem is repaired; the failure never persisted a false STORED state.
    if failure:
        reopened = Ledger(root, scope)
        try:
            assert reopened.lookup(allocation)["state"] == "STORED"
        finally:
            reopened.close()


@pytest.mark.parametrize("interrupted_size", [0, 4096, 1024 * 1024])
def test_startup_repairs_incomplete_or_sparse_headroom(ledger, scope, interrupted_size):
    root = ledger.root
    ledger.close()
    reserve = root / ".ledger-headroom"
    # Crash-equivalent on-disk states, including a full-length sparse file that
    # has no physical allocation yet. Size alone must not admit a new write.
    with reserve.open("wb") as handle:
        handle.truncate(interrupted_size)
    assert reserve.stat().st_blocks == 0
    reopened = Ledger(root, scope)
    try:
        assert reserve.stat().st_size == 1024 * 1024
        assert reserve.stat().st_blocks * 512 >= 1024 * 1024
        assert reopened.writable(3)
        allocation, chunk = str(uuid4()), spec()
        store = StreamingStore(reopened)
        store.prepare(scope, allocation, 1, chunk)
        assert store.store(scope, allocation, 1, chunk, [(0, b"abc")])[0]["state"] == "STORED"
    finally:
        reopened.close()
