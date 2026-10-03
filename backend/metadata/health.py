"""Health transitions only: no RPC, database writes or background scheduling."""

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from math import isfinite
from time import monotonic
from typing import Literal

import grpc
import storage_pb2

from metadata.config import NodeConfig


@dataclass(frozen=True)
class HealthSnapshot:
    status: Literal["ACTIVE", "SUSPECTED", "DOWN"] = "DOWN"
    last_success_at: datetime | None = None
    last_error: str | None = None
    capacity_bytes: int | None = None
    available_bytes: int | None = None
    used_bytes: int | None = None
    # Process-local fields: never restore these from persisted health history.
    last_success_monotonic: float | None = None
    hard_down: bool = False


def utc_now() -> datetime:
    return datetime.now(UTC)


class HealthDetector:
    """Return candidates; the caller publishes them only after its DB commit."""

    def __init__(
        self,
        node: NodeConfig,
        down_after_seconds: float,
        *,
        monotonic_clock: Callable[[], float] = monotonic,
        utc_clock: Callable[[], datetime] = utc_now,
    ):
        if not isfinite(down_after_seconds) or down_after_seconds <= 0:
            raise ValueError("down_after_seconds must be finite and positive")
        self.node_id = node.node_id
        self.failure_domain = node.failure_domain
        self.down_after_seconds = down_after_seconds
        self.monotonic_clock = monotonic_clock
        self.utc_clock = utc_clock

    @staticmethod
    def disabled(previous: HealthSnapshot) -> HealthSnapshot:
        # Re-enabling needs a fresh success even if the old observation was recent.
        return replace(previous, status="DOWN", last_success_monotonic=None, hard_down=False)

    def success(
        self,
        previous: HealthSnapshot,
        response: storage_pb2.HealthCheckResponse,
        *,
        enabled: bool = True,
    ) -> HealthSnapshot:
        if not enabled:
            return self.disabled(previous)
        if response.node_id != self.node_id or response.failure_domain != self.failure_domain:
            return replace(previous, status="DOWN", last_error="IDENTITY_MISMATCH", hard_down=True)
        if not response.storage_writable:
            return replace(
                previous, status="DOWN", last_error="STORAGE_NOT_WRITABLE", hard_down=True
            )
        now = self.utc_clock()
        if now.utcoffset() is None:
            raise ValueError("utc_clock must return a timezone-aware datetime")
        return HealthSnapshot(
            status="ACTIVE",
            last_success_at=now.astimezone(UTC),
            capacity_bytes=response.capacity_bytes,
            available_bytes=response.available_bytes,
            used_bytes=response.used_bytes,
            last_success_monotonic=self.monotonic_clock(),
        )

    def failure(
        self,
        previous: HealthSnapshot,
        code: grpc.StatusCode,
        *,
        enabled: bool = True,
    ) -> HealthSnapshot:
        if not enabled:
            return self.disabled(previous)
        if previous.hard_down:
            # Preserve the blocking diagnosis until a valid writable response.
            return replace(previous, status="DOWN")
        last_success = previous.last_success_monotonic
        status = "DOWN"
        if (
            last_success is not None
            and self.monotonic_clock() - last_success < self.down_after_seconds
        ):
            status = "SUSPECTED"
        # Do not expose raw RPC details, addresses, credentials or exception text.
        return replace(previous, status=status, last_error=code.name)
