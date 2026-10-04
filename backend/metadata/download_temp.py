"""Only exact Metadata download names in the configured directory are managed."""

import logging
import re
import stat
from pathlib import Path
from threading import Lock
from uuid import uuid4

logger = logging.getLogger(__name__)
TEMP_PATTERN = re.compile(r"\.download-[0-9a-f]{32}\.tmp")


def initialize_download_temp(directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    for path in directory.iterdir():
        if TEMP_PATTERN.fullmatch(path.name) and stat.S_ISREG(path.lstat().st_mode):
            # No recursion, no following symlinks, no foreign names or subdirectories.
            path.unlink()


class DownloadTemp:
    def __init__(self, directory):
        self.path = Path(directory) / f".download-{uuid4().hex}.tmp"
        self.file = self.path.open("x+b")
        self._lock = Lock()
        self._closed = False
        self.original_name = None
        self.size_bytes = None
        self.checksum_sha256 = None

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                self.file.close()
            finally:
                try:
                    self.path.unlink(missing_ok=True)
                except OSError as exc:
                    # A failed unlink can be retried by the scoped startup janitor.
                    logger.warning("Cannot remove download temp (%s)", type(exc).__name__)
