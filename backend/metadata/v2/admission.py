"""Bounded admission for new work and progress of existing transfers."""

import re
from threading import Lock

_UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
_INDEX = r"(?:0|[1-9][0-9]{0,15})"
_PROGRESS = {
    "POST": re.compile(
        rf"(?:/api/v2/uploads/{_UUID}/(?:complete|abort)"
        rf"|/internal/v2/uploads/{_UUID}/parts/{_INDEX}/(?:renew|confirm|uncertain)"
        rf"|/internal/v2/downloads/{_UUID}/(?:renew|release))"
    ),
    "GET": re.compile(rf"/internal/v2/downloads/{_UUID}/manifest"),
    "DELETE": re.compile(rf"/api/v2/files/{_UUID}"),
}


def progress_request(method, path):
    """Route classification only; permission, DTO and epoch checks still apply."""
    pattern = _PROGRESS.get(method)
    return pattern is not None and pattern.fullmatch(path.split("?", 1)[0]) is not None


class AdmissionBudget:
    """Two class reservations within one total cap, without a waiting queue.

    Each class can use shared capacity up to limit-reserve, leaving reserve for
    the other class. A lock keeps the bound when callers use different loops.
    """

    def __init__(self, limit, reserve=None):
        if type(limit) is not int or limit <= 0:
            raise ValueError("Positive integer admission limit required")
        reserve = min(8, limit // 4) if reserve is None else reserve
        if type(reserve) is not int or reserve < 0 or 2 * reserve > limit:
            raise ValueError("Admission reserve must fit both classes")
        self.limit, self.reserve = limit, reserve
        self._counts, self._lock = [0, 0], Lock()

    def acquire(self, progress=False):
        lane = int(progress)
        with self._lock:
            if sum(self._counts) >= self.limit or self._counts[lane] >= self.limit - self.reserve:
                return False
            self._counts[lane] += 1
            return True

    def release(self, progress=False):
        lane = int(progress)
        with self._lock:
            if self._counts[lane] <= 0:
                raise RuntimeError("Admission slot already released")
            self._counts[lane] -= 1

    @property
    def active(self):
        with self._lock:
            return sum(self._counts)
