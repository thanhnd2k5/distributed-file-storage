from sqlalchemy import func, select, true

from metadata.models import Chunk, ChunkReplica, File, StorageNode
from metadata.replica_health import health_flags, live_replica_counts
from metadata.schemas import ClusterSummary, NodeCounts, NodeList, NodeSummary


def list_nodes(session):
    nodes = session.scalars(select(StorageNode).order_by(StorageNode.node_id))
    return NodeList(items=[NodeSummary.model_validate(node) for node in nodes])


def cluster_summary(session, settings, operation_busy):
    node_counts = (
        select(
            func.count()
            .filter(StorageNode.enabled, StorageNode.status == "ACTIVE")
            .label("active"),
            func.count()
            .filter(StorageNode.enabled, StorageNode.status == "SUSPECTED")
            .label("suspected"),
            func.count().filter(StorageNode.enabled, StorageNode.status == "DOWN").label("down"),
            func.count().filter(~StorageNode.enabled).label("disabled"),
            func.count(func.distinct(StorageNode.failure_domain))
            .filter(StorageNode.enabled)
            .label("configured_failure_domains"),
            func.count(func.distinct(StorageNode.failure_domain))
            .filter(StorageNode.enabled, StorageNode.status == "ACTIVE")
            .label("active_failure_domains"),
        )
        .select_from(StorageNode)
        .cte("node_counts")
    )

    live_replicas = live_replica_counts()

    chunk_health = (
        select(
            Chunk.id,
            File.replication_factor.label("rf"),
            func.coalesce(live_replicas.c.live_count, 0).label("live_count"),
            func.coalesce(live_replicas.c.domain_count, 0).label("domain_count"),
        )
        .join(File, File.id == Chunk.file_id)
        .outerjoin(live_replicas, live_replicas.c.chunk_id == Chunk.id)
        .where(File.status == "AVAILABLE")
        .cte("chunk_health")
    )

    flags = health_flags(
        chunk_health.c.live_count,
        chunk_health.c.domain_count,
        chunk_health.c.rf,
        func.least(chunk_health.c.rf, node_counts.c.configured_failure_domains),
    )
    chunk_counts = (
        select(
            *[
                func.count().filter(condition).label(f"{name}_chunks")
                for name, condition in flags.items()
            ]
        )
        .select_from(chunk_health.join(node_counts, true()))
        .cte("chunk_counts")
    )

    file_counts = (
        select(func.count().label("files_available"))
        .select_from(File)
        .where(File.status == "AVAILABLE")
        .cte("file_counts")
    )
    cleanup_counts = (
        select(func.count().label("cleanup_pending_replicas"))
        .select_from(ChunkReplica)
        .where(ChunkReplica.cleanup_pending)
        .cte("cleanup_counts")
    )

    # One SQL statement: every aggregate sees the same MVCC snapshot, with no N+1.
    statement = select(node_counts, file_counts, chunk_counts, cleanup_counts).select_from(
        node_counts.join(file_counts, true())
        .join(chunk_counts, true())
        .join(cleanup_counts, true())
    )
    values = dict(session.execute(statement).mappings().one())
    nodes = NodeCounts(
        **{key: values.pop(key) for key in ("active", "suspected", "down", "disabled")}
    )
    return ClusterSummary(
        chunk_size_bytes=settings.chunk_size_bytes,
        default_replication_factor=settings.replication_factor,
        max_file_size_bytes=settings.max_file_size_bytes,
        nodes=nodes,
        operation_busy=operation_busy,
        **values,
    )
