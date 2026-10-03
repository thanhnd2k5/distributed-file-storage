from typing import Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class TransferSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    chunk_size_bytes: int = Field(default=2 * 1024 * 1024, ge=256 * 1024, le=4 * 1024 * 1024)
    grpc_max_message_bytes: int = Field(default=8 * 1024 * 1024, ge=1)
    log_level: str = "INFO"

    @model_validator(mode="after")
    def validate_message_limit(self) -> Self:
        if self.grpc_max_message_bytes < self.chunk_size_bytes + 1024:
            raise ValueError("GRPC_MAX_MESSAGE_BYTES must exceed CHUNK_SIZE_BYTES by >= 1024")
        if self.log_level.upper() not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("LOG_LEVEL is invalid")
        return self

    def grpc_options(self) -> list[tuple[str, int]]:
        return [
            ("grpc.max_send_message_length", self.grpc_max_message_bytes),
            ("grpc.max_receive_message_length", self.grpc_max_message_bytes),
            ("grpc.enable_retries", 0),
            ("grpc.enable_http_proxy", 0),
        ]
