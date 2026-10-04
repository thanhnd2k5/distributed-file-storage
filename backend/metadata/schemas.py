from datetime import UTC
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, field_serializer, model_validator


class FileSummary(BaseModel):
    file_id: UUID
    original_name: str
    content_type: str
    size_bytes: int
    chunk_size_bytes: int
    total_chunks: int
    replication_factor: int
    checksum_sha256: str | None
    status: Literal["UPLOADING", "AVAILABLE", "FAILED", "DELETING", "DELETED"]
    created_at: AwareDatetime

    @field_serializer("created_at")
    def utc_timestamp(self, value):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


class FileList(BaseModel):
    items: list[FileSummary]
    total: int
    limit: int
    offset: int


class FileDetail(FileSummary):
    known_readable: bool
    under_replicated_chunks: int
    unavailable_chunks: int
    domain_degraded_chunks: int
    over_replicated_chunks: int
    cleanup_pending_replicas: int
    error_code: str | None


class ReplicaSummary(BaseModel):
    node_id: str
    failure_domain: str
    node_status: Literal["ACTIVE", "SUSPECTED", "DOWN"]
    status: Literal["PENDING", "VERIFIED", "MISSING", "CORRUPTED", "DELETED"]
    cleanup_pending: bool
    last_verified_at: AwareDatetime | None
    last_error: str | None

    @field_serializer("last_verified_at")
    def utc_timestamp(self, value):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value else None


class ChunkSummary(BaseModel):
    chunk_id: UUID
    chunk_index: int
    size_bytes: int
    checksum_sha256: str
    state: Literal["PENDING", "AVAILABLE", "UNDER_REPLICATED", "UNAVAILABLE", "INACTIVE"]
    live_replica_count: int
    live_failure_domain_count: int
    domain_degraded: bool
    over_replicated: bool
    replicas: list[ReplicaSummary]


class ChunkList(BaseModel):
    file_id: UUID
    items: list[ChunkSummary]


class NodeSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    node_id: str
    host: str
    port: int
    failure_domain: str
    enabled: bool
    status: Literal["ACTIVE", "SUSPECTED", "DOWN"]
    last_success_at: AwareDatetime | None
    last_error: str | None
    capacity_bytes: int | None
    available_bytes: int | None
    used_bytes: int | None

    @model_validator(mode="after")
    def unknown_metrics(self):
        if self.last_success_at is None:
            self.capacity_bytes = self.available_bytes = self.used_bytes = None
        return self

    @field_serializer("last_success_at")
    def utc_timestamp(self, value):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value else None


class NodeList(BaseModel):
    items: list[NodeSummary]


class NodeCounts(BaseModel):
    active: int
    suspected: int
    down: int
    disabled: int


class ClusterSummary(BaseModel):
    chunk_size_bytes: int
    default_replication_factor: int
    max_file_size_bytes: int
    nodes: NodeCounts
    configured_failure_domains: int
    active_failure_domains: int
    files_available: int
    under_replicated_chunks: int
    unavailable_chunks: int
    domain_degraded_chunks: int
    over_replicated_chunks: int
    cleanup_pending_replicas: int
    operation_busy: bool
