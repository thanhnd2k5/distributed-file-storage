import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from metadata.db import Base


class File(Base):
    __tablename__ = "files"
    __table_args__ = (
        CheckConstraint("size_bytes >= 0 AND total_chunks >= 0 AND replication_factor >= 1"),
        CheckConstraint("chunk_size_bytes > 0"),
        CheckConstraint("status IN ('UPLOADING','AVAILABLE','FAILED','DELETING','DELETED')"),
    )
    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    original_name: Mapped[str] = mapped_column(String(255))
    content_type: Mapped[str] = mapped_column(String(255))
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    chunk_size_bytes: Mapped[int] = mapped_column(Integer)
    total_chunks: Mapped[int] = mapped_column(Integer)
    replication_factor: Mapped[int] = mapped_column(Integer)
    checksum_sha256: Mapped[str | None] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16))
    error_code: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Chunk(Base):
    __tablename__ = "chunks"
    __table_args__ = (
        UniqueConstraint("file_id", "chunk_index"),
        CheckConstraint("chunk_index >= 0 AND size_bytes > 0"),
    )
    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    file_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("files.id"), index=True)
    chunk_index: Mapped[int] = mapped_column(Integer)
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    checksum_sha256: Mapped[str] = mapped_column(String(64))


class StorageNode(Base):
    __tablename__ = "storage_nodes"
    __table_args__ = (
        CheckConstraint("port BETWEEN 1 AND 65535"),
        CheckConstraint("status IN ('ACTIVE','SUSPECTED','DOWN')"),
        CheckConstraint("capacity_bytes >= 0 AND available_bytes >= 0 AND used_bytes >= 0"),
    )
    node_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    host: Mapped[str] = mapped_column(String(255))
    port: Mapped[int] = mapped_column(Integer)
    failure_domain: Mapped[str] = mapped_column(String(255))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    status: Mapped[str] = mapped_column(String(16), default="DOWN")
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(String(255))
    capacity_bytes: Mapped[int | None] = mapped_column(BigInteger)
    available_bytes: Mapped[int | None] = mapped_column(BigInteger)
    used_bytes: Mapped[int | None] = mapped_column(BigInteger)


class ChunkReplica(Base):
    __tablename__ = "chunk_replicas"
    __table_args__ = (
        CheckConstraint("status IN ('PENDING','VERIFIED','MISSING','CORRUPTED','DELETED')"),
    )
    chunk_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("chunks.id"), primary_key=True)
    node_id: Mapped[str] = mapped_column(ForeignKey("storage_nodes.node_id"), primary_key=True)
    status: Mapped[str] = mapped_column(String(16), default="PENDING")
    cleanup_pending: Mapped[bool] = mapped_column(Boolean, default=False)
    last_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(String(255))
