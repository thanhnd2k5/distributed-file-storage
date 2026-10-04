"""Bounded deletion of known attempted replicas; pending rows are the durable queue."""

from dataclasses import dataclass

from sqlalchemy import exists, or_, select, tuple_, update

from metadata.models import Chunk, ChunkReplica, File, StorageNode
from metadata.storage_client import StorageRpcError

CLEANUP_MAX_REPLICAS = 8


def finalize_deleting(operation, file_id=None):
    remaining = (
        select(ChunkReplica.chunk_id)
        .join(Chunk, Chunk.id == ChunkReplica.chunk_id)
        .where(
            Chunk.file_id == File.id,
            (ChunkReplica.status != "DELETED") | ChunkReplica.cleanup_pending,
        )
    )
    statement = update(File).where(File.status == "DELETING", ~exists(remaining))
    if file_id is not None:
        statement = statement.where(File.id == file_id)
    with operation.transaction() as session:
        session.execute(statement.values(status="DELETED"))


@dataclass
class CleanupPass:
    # Process-local cursor only controls fairness; restart may safely repeat the scan.
    after: tuple | None = None

    def run(self, operation, settings, cancel, *, file_id=None):
        configured = {node.node_id: node for node in settings.storage_nodes_json}
        endpoint_matches = [
            (StorageNode.node_id == node.node_id)
            & (StorageNode.host == node.host)
            & (StorageNode.port == node.port)
            & (StorageNode.failure_domain == node.failure_domain)
            for node in configured.values()
        ]
        statement = (
            select(ChunkReplica.chunk_id, ChunkReplica.node_id)
            .join(Chunk, Chunk.id == ChunkReplica.chunk_id)
            .join(File, File.id == Chunk.file_id)
            .join(StorageNode, StorageNode.node_id == ChunkReplica.node_id)
            .where(
                File.status.in_(["FAILED", "DELETING"]),
                ChunkReplica.cleanup_pending,
                StorageNode.enabled,
                StorageNode.status.in_(["ACTIVE", "SUSPECTED"]),
                or_(*endpoint_matches) if endpoint_matches else False,
            )
            .order_by(ChunkReplica.chunk_id, ChunkReplica.node_id)
        )
        if file_id is not None:
            statement = statement.where(File.id == file_id)
        key = tuple_(ChunkReplica.chunk_id, ChunkReplica.node_id)
        with operation.transaction() as session:
            selected = session.execute(
                (statement.where(key > self.after) if self.after else statement).limit(
                    CLEANUP_MAX_REPLICAS
                )
            ).all()
            if self.after and len(selected) < CLEANUP_MAX_REPLICAS:
                selected += session.execute(
                    statement.where(key <= self.after).limit(CLEANUP_MAX_REPLICAS - len(selected))
                ).all()
        for chunk_id, node_id in selected:
            if cancel.is_set():
                break
            self.after = (chunk_id, node_id)
            try:
                operation.delete_chunk(node_id, str(chunk_id), cancel=cancel)
            except StorageRpcError as exc:
                with operation.transaction() as session:
                    replica = session.get(ChunkReplica, (chunk_id, node_id))
                    replica.last_error = exc.reason
                if exc.cancelled:
                    break
            else:
                with operation.transaction() as session:
                    replica = session.get(ChunkReplica, (chunk_id, node_id))
                    replica.status = "DELETED"
                    replica.cleanup_pending = False
                    replica.last_error = None
        finalize_deleting(operation, file_id)
