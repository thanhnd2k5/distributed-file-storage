from pathlib import Path
from typing import Self

from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy.engine import make_url

from common.config import TransferSettings


class NodeConfig(BaseModel):
    node_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    host: str = Field(min_length=1)
    port: int = Field(ge=1, le=65535)
    failure_domain: str = Field(min_length=1)


class MetadataSettings(TransferSettings):
    database_url: str
    storage_nodes_json: list[NodeConfig]
    replication_factor: int = Field(default=2, ge=1)
    max_file_size_bytes: int = Field(default=64 * 1024 * 1024, ge=0)
    health_interval_seconds: float = Field(default=3, gt=0)
    health_rpc_timeout_seconds: float = Field(default=1, gt=0)
    node_down_after_seconds: float = Field(default=10, gt=0)
    chunk_rpc_timeout_seconds: float = Field(default=5, gt=0)
    rpc_max_attempts: int = Field(default=2, ge=1, le=2)
    cleanup_interval_seconds: float = Field(default=5, gt=0)
    repair_max_chunks: int = Field(default=8, ge=1, le=8)
    repair_time_budget_seconds: float = Field(default=30, gt=0)
    download_temp_dir: Path = Path(".runtime/downloads")
    cors_origins: list[str] = ["http://localhost:5173"]

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: str) -> str:
        url = make_url(value)
        if url.drivername != "postgresql+psycopg":
            raise ValueError("DATABASE_URL must use postgresql+psycopg")
        return value

    @model_validator(mode="after")
    def validate_cluster(self) -> Self:
        ids = [node.node_id for node in self.storage_nodes_json]
        if len(ids) != len(set(ids)):
            raise ValueError("STORAGE_NODES_JSON contains duplicate node_id")
        if self.replication_factor > len(ids):
            raise ValueError("REPLICATION_FACTOR exceeds configured node count")
        if self.node_down_after_seconds <= self.health_rpc_timeout_seconds:
            raise ValueError("NODE_DOWN_AFTER_SECONDS must exceed health RPC timeout")
        return self
