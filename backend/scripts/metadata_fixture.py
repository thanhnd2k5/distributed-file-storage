"""Deterministic metadata-only fixtures for isolated lifecycle schemas."""

import hashlib
import json
import uuid
from datetime import UTC, datetime

from sqlalchemy import select

from metadata.models import Chunk, ChunkReplica, File, StorageNode

STAMP = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
MODELS = (File, Chunk, StorageNode, ChunkReplica)


def seed(sessions, configs):
    """Caller must supply a newly created, isolated schema, never app tables."""
    with sessions.begin() as session:
        for config in configs:
            session.add(
                StorageNode(
                    **config.model_dump(),
                    enabled=True,
                    status="ACTIVE",
                    last_success_at=STAMP,
                    last_error="old-error",
                    capacity_bytes=1000,
                    available_bytes=900,
                    used_bytes=100,
                )
            )
        session.add(
            StorageNode(
                node_id="retired-node",
                host="retired",
                port=50050,
                failure_domain="retired-domain",
                enabled=True,
                status="ACTIVE",
                last_success_at=STAMP,
                capacity_bytes=2000,
                available_bytes=1800,
                used_bytes=200,
            )
        )
        session.flush()
        for index, status in enumerate(("AVAILABLE", "UPLOADING", "DELETING", "FAILED", "DELETED")):
            file_id = uuid.UUID(int=index + 1)
            chunk_id = uuid.UUID(int=index + 101)
            session.add(
                File(
                    id=file_id,
                    original_name=f"{status}.bin",
                    content_type="application/octet-stream",
                    size_bytes=100,
                    chunk_size_bytes=2097152,
                    total_chunks=1,
                    replication_factor=1,
                    checksum_sha256=hashlib.sha256(status.encode()).hexdigest(),
                    status=status,
                    error_code="fixture-error" if status == "FAILED" else None,
                    created_at=STAMP,
                    deleted_at=STAMP if status == "DELETED" else None,
                )
            )
            session.flush()
            session.add(
                Chunk(
                    id=chunk_id,
                    file_id=file_id,
                    chunk_index=0,
                    size_bytes=100,
                    checksum_sha256=hashlib.sha256(f"chunk-{status}".encode()).hexdigest(),
                )
            )
            session.flush()
            session.add(
                ChunkReplica(
                    chunk_id=chunk_id,
                    node_id=configs[0].node_id,
                    status={"UPLOADING": "PENDING", "FAILED": "MISSING", "DELETED": "DELETED"}.get(
                        status, "VERIFIED"
                    ),
                    cleanup_pending=status == "FAILED",
                    last_verified_at=STAMP if status in {"AVAILABLE", "DELETING"} else None,
                    last_error="fixture-replica-error" if status == "FAILED" else None,
                )
            )
            if status == "UPLOADING":
                session.add(
                    ChunkReplica(
                        chunk_id=chunk_id,
                        node_id="retired-node",
                        status="DELETED",
                        cleanup_pending=False,
                    )
                )


def snapshot(sessions):
    """Compare every column of every fixture row, in stable primary-key order."""
    result = {}
    with sessions.begin() as session:
        for model in MODELS:
            table = model.__table__
            rows = session.execute(select(table).order_by(*table.primary_key.columns)).mappings()
            result[table.name] = [
                {
                    key: value.isoformat()
                    if isinstance(value, datetime)
                    else str(value)
                    if isinstance(value, uuid.UUID)
                    else value
                    for key, value in row.items()
                }
                for row in rows
            ]
    return result


def digest(state):
    return hashlib.sha256(json.dumps(state, sort_keys=True).encode()).hexdigest()
