"""Node-local chunk layout, startup and immutable atomic Store operations."""

import errno
import hashlib
import logging
import os
import re
import stat
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from uuid import uuid4

from storage.validation import validate_chunk_id, validate_store

logger = logging.getLogger(__name__)

TEMP_PREFIX = ".store-"
TEMP_PATTERN = re.compile(r"\.store-[0-9a-f]{32}\.tmp")


class StorageStartupError(RuntimeError):
    """The configured storage directory cannot safely serve this node."""


class ChunkConflictError(Exception):
    """The immutable chunk ID already contains different bytes."""


class InactiveStoreError(Exception):
    """The caller cancelled or expired before commit."""


class ChunkDataError(Exception):
    """Local chunk bytes cannot be read as a complete valid-sized snapshot."""


@dataclass(frozen=True)
class GetResult:
    chunk_id: str
    data: bytes
    checksum_sha256: str


@dataclass(frozen=True)
class StoreResult:
    chunk_id: str
    size_bytes: int
    checksum_sha256: str
    already_existed: bool


class ChunkStore:
    """One process owns this directory; never share it between running nodes."""

    def __init__(self, data_dir: Path, chunk_size_limit: int):
        self.data_dir = data_dir.resolve()
        self.chunk_size_limit = chunk_size_limit
        self.operation_lock = Lock()
        self._stats_lock = Lock()
        self._accounted_sizes: dict[str, int] = {}
        self._used_bytes = self._initialize_directory()

    @property
    def used_bytes(self) -> int:
        # Health never acquires the lock held across chunk filesystem I/O.
        with self._stats_lock:
            return self._used_bytes

    def put(
        self, chunk_id: str, data: bytes, checksum_sha256: str, is_active: Callable[[], bool]
    ) -> StoreResult:
        checksum = validate_store(chunk_id, data, checksum_sha256, self.chunk_size_limit)
        committed = self.chunk_path(chunk_id)
        with self.operation_lock:
            self._require_active(is_active)
            try:
                existing_stat = committed.lstat()
            except FileNotFoundError:
                existing_stat = None
            if existing_stat is not None:
                if not stat.S_ISREG(existing_stat.st_mode):
                    raise OSError(errno.EIO, "Committed chunk is not a regular file")
                with committed.open("rb") as handle:
                    existing = handle.read(self.chunk_size_limit + 1)
                if existing != data:
                    raise ChunkConflictError("Chunk ID already contains different bytes")
                # Check actual bytes on every retry, not a cached hash/sidecar.
                return StoreResult(
                    chunk_id, len(existing), hashlib.sha256(existing).hexdigest(), True
                )

            temporary = self.temporary_path()
            created = False
            try:
                with temporary.open("xb") as handle:
                    created = True
                    if handle.write(data) != len(data):
                        raise OSError(errno.EIO, "Incomplete temporary chunk write")
                    handle.flush()
                    os.fsync(handle.fileno())
                self._require_active(is_active)
                os.replace(temporary, committed)
                previous = self._accounted_sizes.get(chunk_id, 0)
                self._accounted_sizes[chunk_id] = len(data)
                with self._stats_lock:
                    self._used_bytes += len(data) - previous
                return StoreResult(chunk_id, len(data), checksum, False)
            finally:
                if created:
                    try:
                        temporary.unlink(missing_ok=True)
                    except OSError:
                        # Preserve the primary failure/committed result. Startup retries cleanup.
                        logger.exception("Could not clean Store tempfile")

    def get(self, chunk_id: str) -> GetResult:
        committed = self.chunk_path(chunk_id)
        with self.operation_lock:
            if not stat.S_ISREG(committed.lstat().st_mode):
                raise ChunkDataError("Committed chunk is not a regular file")
            with committed.open("rb") as handle:
                expected_size = os.fstat(handle.fileno()).st_size
                if not 1 <= expected_size <= self.chunk_size_limit:
                    raise ChunkDataError("Local chunk size is outside the configured limit")
                data = handle.read(self.chunk_size_limit + 1)
                if len(data) != expected_size:
                    raise ChunkDataError("Local chunk snapshot is incomplete")
        # Bytes are already an immutable snapshot; hash/response need not hold the lock.
        return GetResult(chunk_id, data, hashlib.sha256(data).hexdigest())

    def delete(self, chunk_id: str) -> bool:
        committed = self.chunk_path(chunk_id)
        with self.operation_lock:
            try:
                info = committed.lstat()
            except FileNotFoundError:
                return False
            if not stat.S_ISREG(info.st_mode):
                raise OSError(errno.EIO, "Committed chunk is not a regular file")
            committed.unlink()
            # Subtract the bytes previously counted, even if an external edit changed size.
            counted = self._accounted_sizes.pop(chunk_id, 0)
            with self._stats_lock:
                self._used_bytes -= counted
            return True

    @staticmethod
    def _require_active(is_active: Callable[[], bool]) -> None:
        if not is_active():
            raise InactiveStoreError("Store request is no longer active")

    def chunk_path(self, chunk_id: str) -> Path:
        return self.data_dir / f"{validate_chunk_id(chunk_id)}.chunk"

    def temporary_path(self) -> Path:
        # P2 creates this with exclusive open in the same filesystem as committed chunks.
        return self.data_dir / f"{TEMP_PREFIX}{uuid4().hex}.tmp"

    def _initialize_directory(self) -> int:
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            # Do a real write/flush probe; os.access alone is insufficient (e.g. root/ACLs).
            with tempfile.TemporaryFile(prefix=".probe-", dir=self.data_dir) as probe:
                probe.write(b"startup")
                probe.flush()
                os.fsync(probe.fileno())

            used_bytes = 0
            stale_temporaries = []
            with os.scandir(self.data_dir) as entries:
                for entry in entries:
                    if TEMP_PATTERN.fullmatch(entry.name):
                        if entry.is_file(follow_symlinks=False):
                            stale_temporaries.append(self.data_dir / entry.name)
                        continue
                    if not entry.name.endswith(".chunk"):
                        continue
                    try:
                        validate_chunk_id(entry.name.removesuffix(".chunk"))
                    except ValueError:
                        continue  # Unrecognized files do not belong to this layout.
                    if not entry.is_file(follow_symlinks=False):
                        raise StorageStartupError(
                            f"Committed chunk must be a regular file: {entry.name}"
                        )
                    size = entry.stat(follow_symlinks=False).st_size
                    if size > self.chunk_size_limit:
                        raise StorageStartupError(
                            f"CHUNK_SIZE_BYTES is below stored chunk size: {entry.name} "
                            f"({size} > {self.chunk_size_limit})"
                        )
                    used_bytes += size
                    self._accounted_sizes[entry.name.removesuffix(".chunk")] = size

            # Only exact owned tempfile names are removed, after directory validation.
            # Startup requires the previous process to have stopped.
            for path in stale_temporaries:
                path.unlink()
            return used_bytes
        except OSError as exc:
            raise StorageStartupError(
                f"Cannot initialize DATA_DIR {self.data_dir}: {exc.strerror or type(exc).__name__}"
            ) from exc
