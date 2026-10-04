"""Greedy destination selection over detached node snapshots; no DB or RPC."""

import logging
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PlacementNode:
    node_id: str
    failure_domain: str
    enabled: bool
    status: Literal["ACTIVE", "SUSPECTED", "DOWN"]
    available_bytes: int | None
    used_bytes: int | None


def select_destination(
    nodes: Sequence[PlacementNode],
    chunk_size_bytes: int,
    replication_factor: int,
    *,
    verified_replicas: Collection[PlacementNode] = (),
    failed_node_ids: Collection[str] = (),
    allocated_bytes: Mapping[str, int] | None = None,
) -> PlacementNode | None:
    """Select one missing replica, or None when RF is met/no destination exists.

    ACTIVE already includes the M2 identity/writable check. The coordinator
    supplies only confirmed replicas, persists PENDING before Store and repeats
    selection until the file's RF is met. Increase allocated_bytes only after a
    valid Store ack; exclude final failed/ambiguous destinations for the rest of
    the operation. This function never changes caller snapshots/accounting.
    Free space is a health snapshot, not a reservation or Store guarantee.
    """
    if chunk_size_bytes <= 0 or replication_factor <= 0:
        raise ValueError("chunk_size_bytes and replication_factor must be positive")
    existing_ids = {node.node_id for node in verified_replicas}
    if len(existing_ids) >= replication_factor:
        return None
    used_domains = {node.failure_domain for node in verified_replicas}
    excluded = existing_ids | set(failed_node_ids)
    candidates = [
        node
        for node in nodes
        if node.node_id not in excluded
        and node.enabled
        and node.status == "ACTIVE"
        and node.available_bytes is not None
        and node.available_bytes >= chunk_size_bytes
        and node.used_bytes is not None
        and node.used_bytes >= 0
    ]
    if not candidates:
        return None
    different_domain = [node for node in candidates if node.failure_domain not in used_domains]
    allocation = allocated_bytes if allocated_bytes is not None else {}
    selected = min(
        different_domain or candidates,
        key=lambda node: (allocation.get(node.node_id, 0), node.used_bytes, node.node_id),
    )
    if used_domains and not different_domain:
        logger.warning(
            "Replica placement shares failure domain %s on node %s",
            selected.failure_domain,
            selected.node_id,
        )
    return selected
