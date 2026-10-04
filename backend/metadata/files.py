"""Read-only file views. Transactions share one PostgreSQL snapshot, without RPC/lock."""

from contextlib import contextmanager

from sqlalchemy import func, select, text

from metadata.models import Chunk, ChunkReplica, File, StorageNode
from metadata.replica_health import chunk_state, health_flags, live_replica_counts
from metadata.schemas import (
    ChunkList,
    ChunkSummary,
    FileDetail,
    FileList,
    FileSummary,
    ReplicaSummary,
)


class FileNotFound(Exception):
    pass


@contextmanager
def file_snapshot(sessions):
    with sessions.begin() as session:
        session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        session.execute(text("SET LOCAL statement_timeout = '5s'"))
        yield session


def file_summary(file):
    return FileSummary(
        file_id=file.id,
        original_name=file.original_name,
        content_type=file.content_type,
        size_bytes=file.size_bytes,
        chunk_size_bytes=file.chunk_size_bytes,
        total_chunks=file.total_chunks,
        replication_factor=file.replication_factor,
        checksum_sha256=file.checksum_sha256,
        status=file.status,
        created_at=file.created_at,
    )


def visible_file(session, file_id):
    file = session.get(File, file_id)
    if file is None or file.status == "DELETED":
        raise FileNotFound
    return file


def list_files(session, limit, offset, include_inactive):
    statuses = (
        ("AVAILABLE", "UPLOADING", "FAILED", "DELETING") if include_inactive else ("AVAILABLE",)
    )
    total = session.scalar(select(func.count()).select_from(File).where(File.status.in_(statuses)))
    files = session.scalars(
        select(File)
        .where(File.status.in_(statuses))
        .order_by(File.created_at.desc(), File.id.desc())
        .limit(limit)
        .offset(offset)
    )
    return FileList(
        items=[file_summary(file) for file in files], total=total, limit=limit, offset=offset
    )


def chunk_health_query(file_id):
    live = live_replica_counts(file_id)
    enabled_domains = (
        select(func.count(func.distinct(StorageNode.failure_domain)))
        .where(StorageNode.enabled)
        .scalar_subquery()
    )
    return (
        select(
            Chunk,
            func.coalesce(live.c.live_count, 0).label("live_count"),
            func.coalesce(live.c.domain_count, 0).label("domain_count"),
            enabled_domains.label("enabled_domains"),
        )
        .outerjoin(live, live.c.chunk_id == Chunk.id)
        .where(Chunk.file_id == file_id)
        .order_by(Chunk.chunk_index)
    )


def file_detail(session, file_id):
    file = visible_file(session, file_id)
    counters = dict.fromkeys(
        (
            "under_replicated_chunks",
            "unavailable_chunks",
            "domain_degraded_chunks",
            "over_replicated_chunks",
        ),
        0,
    )
    for _, live_count, domains, enabled_domains in session.execute(chunk_health_query(file_id)):
        for name, value in health_flags(
            live_count,
            domains,
            file.replication_factor,
            min(file.replication_factor, enabled_domains),
        ).items():
            counters[f"{name}_chunks"] += int(value)
    cleanup_count = session.scalar(
        select(func.count())
        .select_from(ChunkReplica)
        .join(Chunk)
        .where(Chunk.file_id == file_id, ChunkReplica.cleanup_pending)
    )
    return FileDetail(
        **file_summary(file).model_dump(),
        known_readable=file.status == "AVAILABLE" and counters["unavailable_chunks"] == 0,
        cleanup_pending_replicas=cleanup_count,
        error_code=file.error_code,
        **counters,
    )


def file_chunks(session, file_id):
    file = visible_file(session, file_id)
    statement = (
        chunk_health_query(file_id)
        .add_columns(ChunkReplica, StorageNode.failure_domain, StorageNode.status)
        .outerjoin(ChunkReplica, ChunkReplica.chunk_id == Chunk.id)
        .outerjoin(StorageNode, StorageNode.node_id == ChunkReplica.node_id)
        .order_by(ChunkReplica.node_id)
    )
    items = {}
    for (
        chunk,
        live_count,
        domains,
        enabled_domains,
        replica,
        domain,
        node_status,
    ) in session.execute(statement):
        if chunk.id not in items:
            flags = health_flags(
                live_count,
                domains,
                file.replication_factor,
                min(file.replication_factor, enabled_domains),
            )
            items[chunk.id] = ChunkSummary(
                chunk_id=chunk.id,
                chunk_index=chunk.chunk_index,
                size_bytes=chunk.size_bytes,
                checksum_sha256=chunk.checksum_sha256,
                state=chunk_state(file.status, live_count, file.replication_factor),
                live_replica_count=live_count,
                live_failure_domain_count=domains,
                domain_degraded=flags["domain_degraded"],
                over_replicated=flags["over_replicated"],
                replicas=[],
            )
        if replica is not None:
            items[chunk.id].replicas.append(
                ReplicaSummary(
                    node_id=replica.node_id,
                    failure_domain=domain,
                    node_status=node_status,
                    status=replica.status,
                    cleanup_pending=replica.cleanup_pending,
                    last_verified_at=replica.last_verified_at,
                    last_error=replica.last_error,
                )
            )
    return ChunkList(file_id=file.id, items=list(items.values()))
