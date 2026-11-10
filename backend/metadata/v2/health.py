"""Node health policy. Reachability observations never change replica evidence."""

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class HealthObservation:
    outcome: Literal["reachable", "unreachable", "scope_mismatch"]
    available_bytes: int | None = None
    writable: bool | None = None


def next_health(state, failures, successes, observation):
    if observation.outcome == "scope_mismatch":
        return "QUARANTINED", 0, 0
    if observation.outcome == "reachable":
        if state == "SUSPECTED":
            successes += 1
            if successes < 2:
                return state, 0, successes
            return "ACTIVE", 0, 0
        return state, 0, 0
    failures = min(failures + 1, 3)
    if state == "DOWN" or failures == 3:
        return "DOWN", failures, 0
    return "SUSPECTED", failures, 0
