"""V1 validation shared by RPC handlers and filesystem path construction."""

import hashlib
import re
from uuid import UUID

CHECKSUM_PATTERN = re.compile(r"[0-9a-f]{64}")


def validate_chunk_id(chunk_id: str) -> str:
    try:
        canonical = str(UUID(chunk_id))
    except (ValueError, AttributeError, TypeError) as exc:
        raise ValueError("chunk_id must be a canonical lowercase UUID") from exc
    if chunk_id != canonical:
        raise ValueError("chunk_id must be a canonical lowercase UUID")
    return canonical


def validate_store(chunk_id: str, data: bytes, checksum_sha256: str, limit: int) -> str:
    validate_chunk_id(chunk_id)
    if not 1 <= len(data) <= limit:
        raise ValueError("data must contain between 1 and CHUNK_SIZE_BYTES bytes")
    if not CHECKSUM_PATTERN.fullmatch(checksum_sha256):
        raise ValueError("checksum_sha256 must contain 64 lowercase hex characters")
    actual_checksum = hashlib.sha256(data).hexdigest()
    if checksum_sha256 != actual_checksum:
        raise ValueError("checksum_sha256 does not match data")
    return actual_checksum
