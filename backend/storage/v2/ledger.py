"""Durable allocation intents. One owner process per local filesystem volume.

SQLite commits and filesystem flushes are separate boundaries. Recovery resolves
their gap before the ledger can serve. Directory fsync requires a POSIX filesystem.
"""

import errno
import hashlib
import os
import re
import shutil
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Lock, RLock
from uuid import UUID, uuid4


class LedgerError(Exception):
    """An allocation operation cannot be completed safely."""


class PreconditionError(LedgerError):
    pass


class ConflictError(LedgerError):
    pass


class MissingError(LedgerError):
    pass


def canonical_uuid(value):
    try:
        if str(UUID(value)) != value:
            raise ValueError("UUID must be canonical lowercase")
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError("Invalid canonical UUID") from exc
    return value


def generation_value(value):
    if type(value) is not int or not 0 < value <= 2**63 - 1:
        raise ValueError("Generation must fit positive signed BIGINT")
    return value


@dataclass(frozen=True)
class Scope:
    namespace_id: str
    recovery_epoch: str
    node_id: str
    node_incarnation: str

    def __post_init__(self):
        for value in (self.namespace_id, self.recovery_epoch, self.node_incarnation):
            canonical_uuid(value)
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", self.node_id):
            raise ValueError("Invalid node ID")


@dataclass(frozen=True)
class Spec:
    chunk_id: str
    size_bytes: int
    checksum_sha256: str

    def validate(self, limit):
        canonical_uuid(self.chunk_id)
        if type(self.size_bytes) is not int or not 0 < self.size_bytes <= limit:
            raise ValueError("Invalid chunk size")
        if not re.fullmatch(r"[0-9a-f]{64}", self.checksum_sha256):
            raise ValueError("Invalid SHA-256")


def fsync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class Ledger:
    def __init__(
        self,
        root: Path,
        scope: Scope,
        *,
        provision=False,
        chunk_limit=64 * 1024**2,
        maintenance=False,
    ):
        if os.name != "posix":
            raise PreconditionError("V2 durable ledger requires POSIX directory fsync")
        import fcntl

        self.root = Path(root).resolve()
        self.scope = scope
        self.chunk_limit = chunk_limit
        self.maintenance = maintenance
        self._mutex = RLock()
        self._headroom_lock = Lock()
        self._db = None
        self._lockfile = None
        if not self.root.exists():
            if not provision:
                raise PreconditionError("Unprovisioned volume; explicit provisioning required")
            self.root.mkdir(parents=True)
            fsync_directory(self.root.parent)
        self._lockfile = (self.root / ".volume.lock").open("a+b")
        try:
            fcntl.flock(self._lockfile.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            path = self.root / "ledger.sqlite3"
            fresh = not path.exists()
            if path.is_symlink():
                raise PreconditionError("Ledger cannot be a symlink")
            if not path.exists():
                unknown = [p for p in self.root.iterdir() if p.name != ".volume.lock"]
                if not provision or unknown:
                    raise PreconditionError("Missing ledger; quarantine volume, do not recreate")
            self._db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
            for pragma in (
                "journal_mode=WAL",
                "synchronous=FULL",
                "foreign_keys=ON",
                "busy_timeout=5000",
            ):
                self._db.execute(f"PRAGMA {pragma}")
            actual = [
                self._db.execute(f"PRAGMA {p}").fetchone()[0]
                for p in ("journal_mode", "synchronous", "foreign_keys", "busy_timeout")
            ]
            if actual != ["wal", 2, 1, 5000]:
                raise PreconditionError("SQLite durability PRAGMAs unavailable")
            self._db.row_factory = sqlite3.Row
            self._create_schema()
            identity = self._db.execute("SELECT * FROM node_identity").fetchone()
            expected = (
                scope.namespace_id,
                scope.recovery_epoch,
                scope.node_id,
                scope.node_incarnation,
            )
            if identity is None:
                if not provision or not fresh:
                    raise PreconditionError("Missing node identity")
                with self.transaction() as db:
                    db.execute("INSERT INTO node_identity VALUES (1,?,?,?,?)", expected)
                    db.execute("INSERT INTO maintenance VALUES (1,'ACTIVE',NULL)")
            elif tuple(identity)[1:] != expected:
                raise PreconditionError(
                    "Volume scope/incarnation/epoch mismatch; maintenance required"
                )
            for name in ("chunks", "tmp", "quarantine"):
                directory = self.root / name
                if directory.is_symlink():
                    raise PreconditionError("Storage directory cannot be symlink")
                directory.mkdir(exist_ok=True)
            fsync_directory(self.root)
            if not maintenance:
                self.require_scope(scope)
                self.recover()
                self.restore_headroom()
        except BaseException:
            self.close()
            raise

    def _create_schema(self):
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS node_identity (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                namespace_id TEXT NOT NULL, active_epoch TEXT NOT NULL,
                node_id TEXT NOT NULL, incarnation TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS maintenance (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                mode TEXT NOT NULL CHECK(mode IN ('ACTIVE','RECOVERING')),
                pending_epoch TEXT
            );
            CREATE TABLE IF NOT EXISTS allocations (
                inventory_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                allocation_id TEXT UNIQUE NOT NULL, epoch TEXT NOT NULL,
                generation INTEGER NOT NULL CHECK(generation>0),
                chunk_id TEXT, size_bytes INTEGER CHECK(size_bytes>0), checksum_sha256 TEXT,
                state TEXT NOT NULL CHECK(state IN
                    ('PREPARED','WRITING','STORED','DELETED','CORRUPT')),
                temp_name TEXT, actual_size INTEGER, actual_hash TEXT,
                cleanup_complete INTEGER NOT NULL DEFAULT 0 CHECK(cleanup_complete IN (0,1)),
                CHECK(state='DELETED' OR
                    (chunk_id IS NOT NULL AND size_bytes IS NOT NULL
                     AND checksum_sha256 IS NOT NULL)),
                CHECK(state!='STORED' OR (actual_size=size_bytes AND actual_hash=checksum_sha256
                    AND actual_size IS NOT NULL AND actual_hash IS NOT NULL)),
                CHECK(cleanup_complete=0 OR state='DELETED')
            );
        """)

    @contextmanager
    def transaction(self):
        with self._mutex:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield self._db
                self._db.execute("COMMIT")
            except BaseException:
                if self._db.in_transaction:
                    self._db.execute("ROLLBACK")
                raise

    def close(self):
        if self._db is not None:
            self._db.close()
            self._db = None
        if self._lockfile is not None:
            self._lockfile.close()
            self._lockfile = None

    def require_scope(self, scope):
        if self.maintenance:
            raise PreconditionError("Offline maintenance owns this volume")
        if scope != self.scope:
            raise PreconditionError("Scope does not match provisioned volume")
        with self._mutex:
            mode = self._db.execute("SELECT mode FROM maintenance WHERE singleton=1").fetchone()
        if mode is None or mode[0] != "ACTIVE":
            raise PreconditionError("Recovery gate closed")

    def lookup(self, allocation_id):
        canonical_uuid(allocation_id)
        with self._mutex:
            row = self._db.execute(
                "SELECT * FROM allocations WHERE allocation_id=?", (allocation_id,)
            ).fetchone()
        if row is None:
            raise MissingError("Unknown allocation")
        return dict(row)

    def inspect(self, scope, allocation_id, generation):
        self.require_scope(scope)
        generation_value(generation)
        row = self.lookup(allocation_id)
        if row["epoch"] != scope.recovery_epoch:
            raise PreconditionError("Allocation requires explicit epoch adoption")
        if row["generation"] != generation:
            raise PreconditionError("Generation does not match high-water")
        return row

    def prepare(self, scope, allocation_id, generation, spec):
        self.require_scope(scope)
        canonical_uuid(allocation_id)
        generation_value(generation)
        spec.validate(self.chunk_limit)
        with self.transaction() as db:
            row = db.execute(
                "SELECT * FROM allocations WHERE allocation_id=?", (allocation_id,)
            ).fetchone()
            if row is not None:
                if row["epoch"] != scope.recovery_epoch:
                    raise PreconditionError("Allocation requires explicit epoch adoption")
                if row["state"] in ("DELETED", "CORRUPT") or generation < row["generation"]:
                    raise PreconditionError("Allocation is fenced")
                if (row["chunk_id"], row["size_bytes"], row["checksum_sha256"]) != (
                    spec.chunk_id,
                    spec.size_bytes,
                    spec.checksum_sha256,
                ):
                    raise ConflictError("Immutable spec differs")
                # WRITING must be drained by the streaming owner before replacing its intent.
                if row["state"] == "WRITING" and generation > row["generation"]:
                    raise PreconditionError("Writer must be fenced/drained by streaming service")
                db.execute(
                    "UPDATE allocations SET generation=? WHERE allocation_id=?",
                    (generation, allocation_id),
                )
            else:
                db.execute(
                    """INSERT INTO allocations
                    (allocation_id,epoch,generation,chunk_id,size_bytes,checksum_sha256,state)
                    VALUES (?,?,?,?,?,?,'PREPARED')""",
                    (
                        allocation_id,
                        scope.recovery_epoch,
                        generation,
                        spec.chunk_id,
                        spec.size_bytes,
                        spec.checksum_sha256,
                    ),
                )
        return self.lookup(allocation_id)

    def fence_delete(self, scope, allocation_id, generation):
        """Commit the permanent fence; caller drains writers before physical cleanup."""
        self.require_scope(scope)
        canonical_uuid(allocation_id)
        generation_value(generation)
        with self._headroom_lock:
            return self._fence_delete(scope, allocation_id, generation)

    def _fence_delete(self, scope, allocation_id, generation):
        # Real preallocated space is reserved for tombstones, never chunk bytes.
        # Release it before the short ledger write when the filesystem is full.
        if shutil.disk_usage(self.root).free < 1024 * 1024:
            (self.root / ".ledger-headroom").unlink(missing_ok=True)
            fsync_directory(self.root)
        with self.transaction() as db:
            row = db.execute(
                "SELECT generation,epoch FROM allocations WHERE allocation_id=?", (allocation_id,)
            ).fetchone()
            if row is not None and generation < row[0]:
                raise PreconditionError("Generation below high-water")
            if row is not None and row[1] != scope.recovery_epoch:
                raise PreconditionError("Old epoch cleanup requires explicit reconciliation")
            db.execute(
                """INSERT INTO allocations (allocation_id,epoch,generation,state)
                VALUES (?,?,?,'DELETED') ON CONFLICT(allocation_id) DO UPDATE SET
                generation=excluded.generation,state='DELETED',cleanup_complete=0""",
                (allocation_id, scope.recovery_epoch, generation),
            )
        return self.lookup(allocation_id)

    def cleanup_deleted(self, allocation_id):
        self.require_scope(self.scope)
        row = self.lookup(allocation_id)
        if row["state"] != "DELETED":
            raise PreconditionError("Delete fence required before unlink")
        if row["epoch"] != self.scope.recovery_epoch:
            raise PreconditionError("Old epoch cleanup requires explicit reconciliation")
        (self.root / "chunks" / allocation_id).unlink(missing_ok=True)
        if row["temp_name"]:
            self._temp_path(row).unlink(missing_ok=True)
        fsync_directory(self.root / "chunks")
        fsync_directory(self.root / "tmp")
        with self.transaction() as db:
            db.execute(
                "UPDATE allocations SET cleanup_complete=1,temp_name=NULL "
                "WHERE allocation_id=? AND state='DELETED'",
                (allocation_id,),
            )
        self.restore_headroom()
        return self.lookup(allocation_id)

    def restore_headroom(self):
        with self._headroom_lock:
            self._restore_headroom()

    def _restore_headroom(self):
        path = self.root / ".ledger-headroom"
        if path.is_symlink():
            raise PreconditionError("Ledger headroom cannot be a symlink")
        try:
            # A crash may leave an empty/partial/sparse file after open and
            # before fallocate/fsync. Reestablish physical reservation even if
            # the pathname already exists; logical file size is not evidence.
            with path.open("a+b") as handle:
                os.posix_fallocate(handle.fileno(), 0, 1024 * 1024)
                os.fsync(handle.fileno())
            fsync_directory(self.root)
        except OSError as exc:
            path.unlink(missing_ok=True)
            if exc.errno not in (errno.ENOSPC, errno.EDQUOT):
                raise
            # Cleanup may still ACK its completed durable fence/unlink; new
            # streams remain closed until physical headroom can be restored.

    def writable(self, incoming=0):
        path = self.root / ".ledger-headroom"
        try:
            reserved = not path.is_symlink() and path.stat().st_size >= 1024 * 1024
        except FileNotFoundError:
            reserved = False
        return reserved and (shutil.disk_usage(self.root).free >= incoming + 1024 * 1024)

    def write_block(self, handle, block):
        # Only a bounded disk write is serialized, never a network read/stream.
        # A tombstone can reclaim headroom without a live writer consuming it.
        with self._headroom_lock:
            if not self.writable(len(block)):
                raise OSError(errno.ENOSPC, "Ledger headroom/capacity unavailable")
            if handle.write(block) != len(block):
                raise OSError(errno.EIO, "Short chunk write")

    def _temp_path(self, row):
        name = row["temp_name"]
        if not re.fullmatch(re.escape(row["allocation_id"]) + r"\.[1-9][0-9]*\.part", name):
            raise PreconditionError("Invalid persisted temporary intent")
        return self.root / "tmp" / name

    def verify_final(self, row):
        path = self.root / "chunks" / row["allocation_id"]
        if path.is_symlink() or not path.is_file():
            return False
        count = 0
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while block := handle.read(262144):
                count += len(block)
                if count > row["size_bytes"]:
                    return False
                digest.update(block)
        return count == row["size_bytes"] and digest.hexdigest() == row["checksum_sha256"]

    def recover(self):
        """Called with volume ownership, before any request/stream is admitted."""
        self.require_scope(self.scope)
        # Page recovery rather than loading allocation history into RAM.
        after = 0
        while True:
            rows = self._db.execute(
                "SELECT * FROM allocations WHERE inventory_sequence>? "
                "ORDER BY inventory_sequence LIMIT 128",
                (after,),
            ).fetchall()
            if not rows:
                break
            for raw in rows:
                row = dict(raw)
                after = row["inventory_sequence"]
                if row["epoch"] != self.scope.recovery_epoch:
                    continue
                if row["state"] == "DELETED":
                    self.cleanup_deleted(row["allocation_id"])
                elif row["state"] in ("WRITING", "STORED"):
                    valid = self.verify_final(row)
                    if valid:
                        # The prior process may have renamed successfully but
                        # failed before flushing the directory. Verification
                        # alone must not promote those visible bytes to STORED.
                        with (self.root / "chunks" / row["allocation_id"]).open("rb") as final:
                            os.fsync(final.fileno())
                        fsync_directory(self.root / "chunks")
                    if row["temp_name"]:
                        self._temp_path(row).unlink(missing_ok=True)
                        fsync_directory(self.root / "tmp")
                    state = (
                        "STORED"
                        if valid
                        else (
                            "CORRUPT"
                            if row["state"] == "STORED"
                            or (self.root / "chunks" / row["allocation_id"]).exists()
                            else "PREPARED"
                        )
                    )
                    with self.transaction() as db:
                        db.execute(
                            "UPDATE allocations SET state=?,temp_name=NULL,"
                            "actual_size=?,actual_hash=? "
                            "WHERE allocation_id=?",
                            (
                                state,
                                row["size_bytes"] if valid else None,
                                row["checksum_sha256"] if valid else None,
                                row["allocation_id"],
                            ),
                        )
        for directory in ("chunks", "tmp"):
            for path in (self.root / directory).iterdir():
                # All remaining tmp files lack a live intent after startup recovery.
                known = (
                    directory == "chunks"
                    and self._db.execute(
                        "SELECT 1 FROM allocations WHERE allocation_id=? "
                        "AND (state!='PREPARED' OR epoch!=?)",
                        (path.name, self.scope.recovery_epoch),
                    ).fetchone()
                )
                if not known:
                    path.rename(self.root / "quarantine" / f"{uuid4()}.{path.name}")
                    fsync_directory(self.root / directory)
                    fsync_directory(self.root / "quarantine")

    def inventory(self, scope, after=0, upper=None, limit=100):
        self.require_scope(scope)
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("Invalid inventory limit")
        for value in (after, upper if upper is not None else 0):
            if type(value) is not int or not 0 <= value <= 2**63 - 1:
                raise ValueError("Invalid inventory sequence")
        with self._mutex:
            if upper is None:
                upper = self._db.execute(
                    "SELECT COALESCE(MAX(inventory_sequence),0) FROM allocations"
                ).fetchone()[0]
            if after > upper:
                raise ValueError("Cursor exceeds upper sequence")
            rows = self._db.execute(
                "SELECT * FROM allocations WHERE inventory_sequence>? AND inventory_sequence<=? "
                "ORDER BY inventory_sequence LIMIT ?",
                (after, upper, limit + 1),
            ).fetchall()
        return [dict(row) for row in rows[:limit]], upper, len(rows) > limit

    def begin_recovery(self, new_epoch):
        """Offline administrator only: flock proves no local serving process remains."""
        if not self.maintenance:
            raise PreconditionError("Offline maintenance required")
        canonical_uuid(new_epoch)
        if new_epoch == self.scope.recovery_epoch:
            raise ValueError("Recovery must install a new epoch")
        with self.transaction() as db:
            pending = db.execute("SELECT pending_epoch FROM maintenance").fetchone()[0]
            if pending is not None and pending != new_epoch:
                raise PreconditionError("Different recovery already pending")
            db.execute("UPDATE maintenance SET mode='RECOVERING',pending_epoch=?", (new_epoch,))

    def adopt(self, allocation_id, *, close=False):
        """Explicit manifest decision: verify stored bytes or retain a terminal fence.

        No cleanup here. Unknown outcomes require close=True and are cleaned only
        after the administrator has reconciled metadata and activates the epoch.
        """
        if not self.maintenance:
            raise PreconditionError("Offline maintenance required")
        row = self.lookup(allocation_id)
        pending = self._db.execute("SELECT mode,pending_epoch FROM maintenance").fetchone()
        if pending[0] != "RECOVERING" or pending[1] is None:
            raise PreconditionError("Recovery not started")
        if not close and (row["state"] != "STORED" or not self.verify_final(row)):
            raise PreconditionError("Only verified STORED bytes can be adopted")
        with self.transaction() as db:
            db.execute(
                "UPDATE allocations SET epoch=?,state=?,cleanup_complete=0 WHERE allocation_id=?",
                (
                    pending[1],
                    "DELETED" if close else "STORED",
                    allocation_id,
                ),
            )

    def activate_epoch(self, new_epoch):
        if not self.maintenance:
            raise PreconditionError("Offline maintenance required")
        canonical_uuid(new_epoch)
        with self.transaction() as db:
            pending = db.execute("SELECT mode,pending_epoch FROM maintenance").fetchone()
            if tuple(pending) != ("RECOVERING", new_epoch):
                raise PreconditionError("Pending epoch differs")
            db.execute("UPDATE node_identity SET active_epoch=?", (new_epoch,))
            db.execute("UPDATE maintenance SET mode='ACTIVE',pending_epoch=NULL")
        # Caller must close and reopen with externally configured new scope.
