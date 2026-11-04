"""Bounded streaming operations, with short commit locks and durable fences."""

import errno
import hashlib
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import Event, Lock, RLock, Semaphore

from storage.v2.ledger import (
    ConflictError,
    PreconditionError,
    canonical_uuid,
    fsync_directory,
    generation_value,
)


class BusyError(Exception):
    pass


class DataError(Exception):
    pass


class InactiveError(Exception):
    pass


@dataclass
class Ownership:
    commit: RLock = field(default_factory=RLock)
    writer: Lock = field(default_factory=Lock)
    readers: int = 0
    readers_idle: Event = field(default_factory=Event)
    references: int = 0

    def __post_init__(self):
        self.readers_idle.set()


class StreamingStore:
    def __init__(self, ledger, *, max_streams=16, drain_timeout=2.0, hook=lambda point: None):
        self.ledger = ledger
        self.drain_timeout = drain_timeout
        self.hook = hook
        self._admission = Semaphore(max_streams)
        self._owners = {}
        self._mutex = Lock()

    @contextmanager
    def owner(self, allocation_id):
        canonical_uuid(allocation_id)
        with self._mutex:
            owner = self._owners.setdefault(allocation_id, Ownership())
            owner.references += 1
        try:
            yield owner
        finally:
            with self._mutex:
                owner.references -= 1
                if not owner.references:
                    del self._owners[allocation_id]

    @contextmanager
    def reader(self, allocation_id):
        with self.owner(allocation_id) as owner:
            with owner.commit:
                if owner.writer.locked():
                    raise BusyError("Allocation writer/delete active")
                owner.readers += 1
                owner.readers_idle.clear()
            try:
                yield owner
            finally:
                with owner.commit:
                    owner.readers -= 1
                    if not owner.readers:
                        owner.readers_idle.set()

    @contextmanager
    def exclusive(self, owner):
        deadline = time.monotonic() + self.drain_timeout
        if not owner.writer.acquire(timeout=self.drain_timeout):
            raise BusyError("Writer has not drained; fence retained")
        try:
            # Holding the writer slot blocks new readers while old ones drain.
            if not owner.readers_idle.wait(max(0, deadline - time.monotonic())):
                raise BusyError("Readers have not drained; fence retained")
            yield
        finally:
            owner.writer.release()

    def prepare(self, scope, allocation_id, generation, spec):
        self.ledger.require_scope(scope)
        generation_value(generation)
        spec.validate(self.ledger.chunk_limit)
        with self.owner(allocation_id) as owner:
            with owner.commit:
                # Fence a live writer without waiting for its next network block.
                with self.ledger.transaction() as db:
                    row = db.execute(
                        "SELECT * FROM allocations WHERE allocation_id=?", (allocation_id,)
                    ).fetchone()
                    if row is not None and row["epoch"] != scope.recovery_epoch:
                        raise PreconditionError("Allocation requires explicit epoch adoption")
                    if row is not None and row["state"] == "WRITING":
                        if (row["chunk_id"], row["size_bytes"], row["checksum_sha256"]) != (
                            spec.chunk_id,
                            spec.size_bytes,
                            spec.checksum_sha256,
                        ):
                            raise ConflictError("Immutable spec differs")
                        if generation < row["generation"]:
                            raise PreconditionError("Stale generation")
                        db.execute(
                            "UPDATE allocations SET generation=? WHERE allocation_id=?",
                            (generation, allocation_id),
                        )
            with self.exclusive(owner):
                with owner.commit:
                    return self.ledger.prepare(scope, allocation_id, generation, spec)

    def delete(self, scope, allocation_id, generation):
        with self.owner(allocation_id) as owner:
            with owner.commit:
                self.ledger.fence_delete(scope, allocation_id, generation)
                self.hook("delete_fence")
            with self.exclusive(owner):
                with owner.commit:
                    return self.ledger.cleanup_deleted(allocation_id)

    def store(self, scope, allocation_id, generation, spec, blocks, active=lambda: True):
        """blocks yields (offset, bytes), never an entire assembled chunk."""
        self.ledger.require_scope(scope)
        spec.validate(self.ledger.chunk_limit)
        generation_value(generation)
        if not self._admission.acquire(blocking=False):
            raise BusyError("Node stream budget exhausted")
        try:
            with self.owner(allocation_id) as owner:
                if not owner.writer.acquire(blocking=False):
                    raise BusyError("Allocation writer already active")
                try:
                    return self._store(
                        owner, scope, allocation_id, generation, spec, blocks, active
                    )
                finally:
                    owner.writer.release()
        finally:
            self._admission.release()

    def _store(self, owner, scope, allocation_id, generation, spec, blocks, active):
        temporary = None
        handle = None
        with owner.commit:
            if owner.readers:
                raise BusyError("Allocation readers active")
            row = self.ledger.inspect(scope, allocation_id, generation)
            if (row["chunk_id"], row["size_bytes"], row["checksum_sha256"]) != (
                spec.chunk_id,
                spec.size_bytes,
                spec.checksum_sha256,
            ):
                raise ConflictError("Immutable spec differs")
            if row["state"] not in ("PREPARED", "STORED"):
                raise PreconditionError("Allocation not writable")
            replay = row["state"] == "STORED"
            if replay and not self.ledger.verify_final(row):
                with self.ledger.transaction() as db:
                    db.execute(
                        "UPDATE allocations SET state='CORRUPT' WHERE allocation_id=?",
                        (allocation_id,),
                    )
                raise DataError("Stored bytes missing or corrupt")
            if not replay:
                if not self.ledger.writable(spec.size_bytes):
                    raise OSError(errno.ENOSPC, "Ledger headroom/capacity unavailable")
                name = f"{allocation_id}.{generation}.part"
                with self.ledger.transaction() as db:
                    db.execute(
                        "UPDATE allocations SET state='WRITING',temp_name=? WHERE allocation_id=?",
                        (name, allocation_id),
                    )
                temporary = self.ledger.root / "tmp" / name
        try:
            if temporary is not None:
                handle = temporary.open("xb", buffering=0)
            count = 0
            digest = hashlib.sha256()
            for offset, data in blocks:
                self._require_writer(scope, allocation_id, generation, active)
                if (
                    offset != count
                    or not 1 <= len(data) <= 262144
                    or count + len(data) > spec.size_bytes
                ):
                    raise ValueError("Invalid block offset/length")
                if handle is not None:
                    self.ledger.write_block(handle, data)
                digest.update(data)
                count += len(data)
            if count != spec.size_bytes or digest.hexdigest() != spec.checksum_sha256:
                raise DataError("Stream size/hash mismatch")
            if handle is not None:
                handle.flush()
                os.fsync(handle.fileno())
                handle.close()
                handle = None
                self.hook("temp_fsync")
            with owner.commit:
                self._require_writer(scope, allocation_id, generation, active)
                if not replay:
                    final = self.ledger.root / "chunks" / allocation_id
                    if final.exists() or final.is_symlink():
                        raise DataError("Unexpected final bytes; recovery required")
                    temporary.rename(final)
                    fsync_directory(final.parent)
                    fsync_directory(temporary.parent)
                    self.hook("rename_fsync")
                    with self.ledger.transaction() as db:
                        db.execute(
                            "UPDATE allocations SET state='STORED',temp_name=NULL,"
                            "actual_size=?,actual_hash=? WHERE allocation_id=?",
                            (count, digest.hexdigest(), allocation_id),
                        )
                    self.hook("stored_commit")
                return self.ledger.lookup(allocation_id), replay
        finally:
            if handle is not None:
                handle.close()
            if temporary is not None:
                # A renamed final belongs to startup reconcile if commit failed.
                # Preserve WRITING when final exists; never turn that into PREPARED.
                with owner.commit:
                    temporary.unlink(missing_ok=True)
                    fsync_directory(temporary.parent)
                    final = self.ledger.root / "chunks" / allocation_id
                    if not final.exists():
                        with self.ledger.transaction() as db:
                            db.execute(
                                "UPDATE allocations SET state='PREPARED',temp_name=NULL "
                                "WHERE allocation_id=? AND state='WRITING'",
                                (allocation_id,),
                            )

    def _require_writer(self, scope, allocation_id, generation, active):
        if not active():
            raise InactiveError("Stream cancelled or deadline expired")
        row = self.ledger.inspect(scope, allocation_id, generation)
        if row["state"] not in ("WRITING", "STORED"):
            raise PreconditionError("Writer fenced")
