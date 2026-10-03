"""Node-local layout/startup only; chunk writes/reads/deletes arrive in P2/P3."""

import os
import re
import tempfile
from pathlib import Path
from uuid import uuid4

from storage.validation import validate_chunk_id

TEMP_PREFIX = ".store-"
TEMP_PATTERN = re.compile(r"\.store-[0-9a-f]{32}\.tmp")


class StorageStartupError(RuntimeError):
    """The configured storage directory cannot safely serve this node."""


class ChunkStore:
    """One process owns this directory; never share it between running nodes."""

    def __init__(self, data_dir: Path, chunk_size_limit: int):
        self.data_dir = data_dir.resolve()
        self.chunk_size_limit = chunk_size_limit
        self.used_bytes = self._initialize_directory()

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

            # Only exact owned tempfile names are removed, after directory validation.
            # Startup requires the previous process to have stopped.
            for path in stale_temporaries:
                path.unlink()
            return used_bytes
        except OSError as exc:
            raise StorageStartupError(
                f"Cannot initialize DATA_DIR {self.data_dir}: {exc.strerror or type(exc).__name__}"
            ) from exc
