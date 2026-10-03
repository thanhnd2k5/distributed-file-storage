from datetime import UTC
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, field_serializer, model_validator


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
