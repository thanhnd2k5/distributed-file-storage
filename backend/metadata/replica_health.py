"""Shared cached replica eligibility and health flags for cluster/file snapshots."""

from sqlalchemy import func, select

from metadata.models import Chunk, ChunkReplica, StorageNode


def live_replica_counts(file_id=None):
    statement = (
        select(
            ChunkReplica.chunk_id,
            func.count().label("live_count"),
            func.count(func.distinct(StorageNode.failure_domain)).label("domain_count"),
        )
        .join(StorageNode, StorageNode.node_id == ChunkReplica.node_id)
        .where(
            ChunkReplica.status == "VERIFIED",
            ~ChunkReplica.cleanup_pending,
            StorageNode.enabled,
            StorageNode.status == "ACTIVE",
        )
    )
    if file_id is not None:
        statement = statement.join(Chunk).where(Chunk.file_id == file_id)
    return statement.group_by(ChunkReplica.chunk_id).cte("live_replicas")


def health_flags(live_count, domain_count, rf, domain_target):
    # & works for both SQL expressions and detached integer comparisons.
    return {
        "under_replicated": (live_count > 0) & (live_count < rf),
        "unavailable": live_count == 0,
        "domain_degraded": domain_count < domain_target,
        "over_replicated": live_count > rf,
    }


def chunk_state(file_status, live_count, rf):
    if file_status in {"FAILED", "DELETING"}:
        return "INACTIVE"
    if file_status == "UPLOADING":
        return "PENDING"
    if live_count >= rf:
        return "AVAILABLE"
    return "UNDER_REPLICATED" if live_count else "UNAVAILABLE"
