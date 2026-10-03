from sqlalchemy import select, update

from metadata.models import Chunk, ChunkReplica, File, StorageNode


def initialize_metadata(session_factory, settings) -> None:
    """Registry/recovery uses one short transaction, with no network operations."""
    with session_factory.begin() as session:
        session.execute(update(StorageNode).values(enabled=False, status="DOWN"))
        for config in settings.storage_nodes_json:
            node = session.get(StorageNode, config.node_id)
            if node is None:
                node = StorageNode(node_id=config.node_id)
                session.add(node)
            elif node.failure_domain != config.failure_domain:
                has_replica = session.scalar(
                    select(ChunkReplica.chunk_id)
                    .where(ChunkReplica.node_id == config.node_id)
                    .limit(1)
                )
                if has_replica:
                    raise ValueError(f"Cannot change failure_domain for node {config.node_id}")
            if (node.host, node.port, node.failure_domain) != (
                config.host,
                config.port,
                config.failure_domain,
            ):
                # A snapshot belongs to its endpoint/domain, not just the stable ID.
                node.last_success_at = None
                node.capacity_bytes = None
                node.available_bytes = None
                node.used_bytes = None
            node.host = config.host
            node.port = config.port
            node.failure_domain = config.failure_domain
            node.enabled = True
            node.status = "DOWN"
            node.last_error = None

        session.execute(
            update(File)
            .where(File.status == "UPLOADING")
            .values(status="FAILED", error_code="UPLOAD_INTERRUPTED")
        )
        inactive_chunks = select(Chunk.id).join(File).where(File.status.in_(["FAILED", "DELETING"]))
        session.execute(
            update(ChunkReplica)
            .where(ChunkReplica.chunk_id.in_(inactive_chunks), ChunkReplica.status != "DELETED")
            .values(cleanup_pending=True)
        )
