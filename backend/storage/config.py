from pathlib import Path

from pydantic import Field

from common.config import TransferSettings


class StorageSettings(TransferSettings):
    node_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    failure_domain: str = Field(min_length=1)
    grpc_bind_host: str = "0.0.0.0"
    grpc_port: int = Field(default=50051, ge=1, le=65535)
    data_dir: Path = Path(".runtime/chunks")
