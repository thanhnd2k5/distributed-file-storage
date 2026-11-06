"""Shared wire validation and bounded JSON HTTP transport for V2 upload services."""

import asyncio
import json
import logging
import math
import re
import ssl
import unicodedata
from datetime import datetime, timezone
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import h11
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator
from sqlalchemy.exc import SQLAlchemyError
from starlette.exceptions import HTTPException

from metadata.v2.admission import AdmissionBudget, progress_request
from metadata.v2.repository import AllocationEvidence, DomainError

JSON_LIMIT = 16384
MAX_INT = 2**53 - 1
STATUS = {
    400: ("INVALID_REQUEST", "INVALID_RANGE"),
    404: ("UPLOAD_NOT_FOUND", "FILE_NOT_FOUND", "READ_LEASE_NOT_FOUND"),
    409: (
        "IDEMPOTENCY_CONFLICT",
        "PART_CONFLICT",
        "UPLOAD_STATE_CONFLICT",
        "PART_BUSY",
        "PARTS_INCOMPLETE",
        "OUTCOME_PENDING",
        "STALE_CLAIM",
        "FILE_NOT_READY",
        "STALE_READ_LEASE",
    ),
    410: ("UPLOAD_EXPIRED", "UPLOAD_ABORTED"),
    413: ("FILE_TOO_LARGE", "PART_TOO_LARGE"),
    412: ("PRECONDITION_FAILED",),
    422: ("VALIDATION_ERROR", "CHECKSUM_MISMATCH", "PART_SIZE_MISMATCH", "INVALID_STORAGE_ACK"),
    429: ("RESOURCE_LIMIT",),
    503: (
        "CAPACITY_UNAVAILABLE",
        "INSUFFICIENT_DOMAINS",
        "STORAGE_UNAVAILABLE",
        "METADATA_UNAVAILABLE",
        "RECOVERY_REQUIRED",
        "CHUNK_UNAVAILABLE",
    ),
}


def uuid_value(value):
    if not isinstance(value, str) or str(UUID(value)) != value:
        raise ValueError("Canonical UUID required")
    return UUID(value)


def integer(value, *, minimum=0, maximum=MAX_INT):
    if not isinstance(value, str) or not re.fullmatch(r"-?(0|[1-9][0-9]*)", value):
        raise ValueError("Integer required")
    result = int(value)
    if not minimum <= result <= maximum:
        raise ValueError("Integer out of range")
    return result


def wire(value):
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, dict):
        return {k: wire(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [wire(v) for v in value]
    return value


def part_result(p):
    return wire(
        dict(
            index=p["part_index"],
            **{k: p[k] for k in ("size_bytes", "checksum_sha256", "state", "confirmed_at")},
        )
    )


def session_result(s):
    return wire(
        {
            k: s[k]
            for k in (
                "upload_id",
                "file_id",
                "version_id",
                "state",
                "original_name",
                "content_type",
                "size_bytes",
                "expected_sha256",
                "chunk_size_bytes",
                "part_count",
                "replication_factor",
                "min_distinct_domains",
                "max_parallel_parts",
                "confirmed_parts",
                "confirmed_bytes",
                "expires_at",
                "created_at",
                "completed_at",
                "error_code",
                "cleanup_pending",
            )
        }
    )


def error_response(code, status=None, *, request_id=None):
    status = status or next((s for s, codes in STATUS.items() if code in codes), 500)
    retry = status in (429, 503) or code in ("PART_BUSY", "OUTCOME_PENDING", "PARTS_INCOMPLETE")
    return JSONResponse(
        {
            "error": {
                "code": code,
                "message": code.replace("_", " ").capitalize(),
                "retryable": retry,
                "details": {},
            }
        },
        status_code=status,
        headers={
            **({"Retry-After": "1"} if retry else {}),
            **({"X-Request-ID": request_id} if request_id else {}),
        },
    )


class RequestIds:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        request_id = scope.setdefault("request_id", str(uuid4()))

        async def respond(message):
            if message["type"] == "http.response.start":
                message = dict(
                    message,
                    headers=[
                        *(h for h in message.get("headers", []) if h[0].lower() != b"x-request-id"),
                        (b"x-request-id", request_id.encode()),
                    ],
                )
            elif message["type"] == "http.response.body" and scope["method"] == "HEAD":
                message = dict(message, body=b"")
            await send(message)

        await self.app(scope, receive, respond)


def handlers(app: FastAPI, *, internal=False):
    app.add_middleware(RequestIds)

    @app.exception_handler(DomainError)
    async def domain(request, exc):
        code = exc.code
        if code == "INVALID_ALLOCATION_ACK":
            code = "VALIDATION_ERROR"
        if not internal:
            code = {
                "STALE_CLAIM": "UPLOAD_STATE_CONFLICT",
                "INVALID_STORAGE_ACK": "STORAGE_UNAVAILABLE",
            }.get(code, code)
        return error_response(code)

    @app.exception_handler(ValueError)
    @app.exception_handler(RequestValidationError)
    async def invalid(request, exc):
        return error_response("VALIDATION_ERROR")

    @app.exception_handler(SQLAlchemyError)
    async def database(request, exc):
        return error_response("METADATA_UNAVAILABLE")

    @app.exception_handler(HTTPException)
    async def http_error(request, exc):
        return error_response("INVALID_REQUEST", exc.status_code)

    @app.exception_handler(Exception)
    async def unexpected(request, exc):
        request_id = request.scope.get("request_id")
        logging.getLogger(__name__).error(
            "V2 HTTP request failed request_id=%s", request_id, exc_info=exc
        )
        response = error_response("INTERNAL_ERROR", request_id=request_id)
        if request.method == "HEAD":
            response.body = b""
        return response


class StrictDTO(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Empty(StrictDTO):
    pass


class CreateUpload(StrictDTO):
    original_name: StrictStr = Field(min_length=1, max_length=255)
    content_type: StrictStr = Field(
        default="application/octet-stream", min_length=1, max_length=255
    )
    size_bytes: StrictInt = Field(ge=0, le=MAX_INT)
    expected_sha256: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("original_name", "content_type")
    @classmethod
    def no_controls(cls, value):
        if any(unicodedata.category(c) == "Cc" for c in value):
            raise ValueError("Control characters are forbidden")
        return value


class Internal(StrictDTO):
    recovery_epoch: StrictStr

    @field_validator("recovery_epoch")
    @classmethod
    def epoch_uuid(cls, value):
        uuid_value(value)
        return value


class Claim(Internal):
    size_bytes: StrictInt = Field(gt=0, le=MAX_INT)
    checksum_sha256: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    request_id: StrictStr

    @field_validator("request_id")
    @classmethod
    def request_uuid(cls, value):
        uuid_value(value)
        return value


class Lease(Internal):
    claim_token: StrictStr
    revision: StrictInt = Field(gt=0, le=2**63 - 1)

    @field_validator("claim_token")
    @classmethod
    def token_uuid(cls, value):
        uuid_value(value)
        return value


class Ack(StrictDTO):
    namespace_id: StrictStr
    recovery_epoch: StrictStr
    node_id: StrictStr = Field(min_length=1, max_length=64)
    incarnation: StrictStr
    allocation_id: StrictStr
    generation: StrictInt = Field(gt=0, le=2**63 - 1)
    chunk_id: StrictStr
    size_bytes: StrictInt = Field(gt=0, le=2**63 - 1)
    checksum_sha256: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    state: StrictStr = Field(pattern=r"^STORED$")
    actual_size: StrictInt = Field(gt=0, le=2**63 - 1)
    actual_hash: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")

    def evidence(self):
        values = self.model_dump()
        for name in ("namespace_id", "recovery_epoch", "incarnation", "allocation_id", "chunk_id"):
            values[name] = uuid_value(values[name])
        return AllocationEvidence(**values)


class Confirm(Lease):
    allocations: list[Ack] = Field(min_length=1, max_length=16)


class Uncertain(Lease):
    allocation_ids: list[StrictStr] = Field(min_length=1, max_length=16)
    reason_code: StrictStr = Field(pattern=r"^[A-Z_]{1,64}$")


async def body(request: Request, dto):
    if request.headers.get("content-type", "").split(";")[0].strip().lower() != "application/json":
        raise DomainError("INVALID_REQUEST")
    data = bytearray()
    try:
        async with asyncio.timeout(10):
            async for block in request.stream():
                if len(data) + len(block) > JSON_LIMIT:
                    raise DomainError("INVALID_REQUEST")
                data.extend(block)
    except TimeoutError as exc:
        raise DomainError("INVALID_REQUEST") from exc
    try:
        # Reject duplicate keys too: no ambiguous command interpretation.
        def pairs(items):
            result = {}
            for k, v in items:
                if k in result:
                    raise ValueError("Duplicate JSON key")
                result[k] = v
            return result

        parsed = json.loads(data, object_pairs_hook=pairs)
        return dto.model_validate(parsed)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise DomainError("VALIDATION_ERROR") from exc


class JsonClient:
    """One async connection per call; no retries, bounded bytes and total deadline.

    Cancellation closes the transport before returning its slot. A cancelled
    mutation remains an unknown outcome, never an excuse to replay the command.
    """

    def __init__(self, public_url, internal_url, epoch, timeout=5, max_calls=32, reserve=None):
        self.urls = (public_url, internal_url)
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("Finite positive control timeout required")
        self.epoch, self.timeout = str(epoch), timeout
        self.slots = AdmissionBudget(max_calls, reserve)
        for url in self.urls:
            p = urlsplit(url)
            if p.scheme not in ("http", "https") or not p.hostname or p.path not in ("", "/"):
                raise ValueError("API origin required")

    async def call(self, method, path, payload=None, *, internal=False, timeout=None, headers=None):
        progress = progress_request(method, path)
        if not self.slots.acquire(progress):
            raise DomainError("RESOURCE_LIMIT")

        writer = None
        try:
            budget = self.timeout if timeout is None else timeout
            if not math.isfinite(budget) or budget <= 0:
                raise ValueError("Finite positive control timeout required")
            async with asyncio.timeout(budget):
                url = urlsplit(self.urls[internal])
                reader, writer = await asyncio.open_connection(
                    url.hostname,
                    url.port or (443 if url.scheme == "https" else 80),
                    ssl=ssl.create_default_context() if url.scheme == "https" else None,
                )
                data = (
                    b""
                    if payload is None
                    else json.dumps(
                        wire(dict(payload, **({"recovery_epoch": self.epoch} if internal else {})))
                    ).encode()
                )
                host = f"[{url.hostname}]" if ":" in url.hostname else url.hostname
                if url.port is not None:
                    host += f":{url.port}"
                wire_headers = {
                    "Host": host,
                    "Content-Type": "application/json",
                    "Content-Length": str(len(data)),
                    "Connection": "close",
                    **(headers or {}),
                    **({"X-Recovery-Epoch": self.epoch} if internal else {}),
                }
                protocol = h11.Connection(h11.CLIENT, max_incomplete_event_size=16384)
                writer.write(
                    protocol.send(
                        h11.Request(method=method, target=path, headers=list(wire_headers.items()))
                    )
                )
                if data:
                    writer.write(protocol.send(h11.Data(data=data)))
                writer.write(protocol.send(h11.EndOfMessage()))
                await writer.drain()
                status, raw = None, bytearray()
                while True:
                    event = protocol.next_event()
                    if event is h11.NEED_DATA:
                        protocol.receive_data(await reader.read(65536))
                    elif isinstance(event, h11.InformationalResponse):
                        continue
                    elif isinstance(event, h11.Response):
                        status = event.status_code
                    elif isinstance(event, h11.Data):
                        if len(raw) + len(event.data) > 1024 * 1024:
                            raise DomainError("METADATA_UNAVAILABLE")
                        raw.extend(event.data)
                    elif isinstance(event, h11.EndOfMessage):
                        break
                    else:
                        raise DomainError("METADATA_UNAVAILABLE")
                if status == 204 and not raw:
                    return None
                result = json.loads(raw)
                if not isinstance(result, dict):
                    raise DomainError("METADATA_UNAVAILABLE")
                if status is None:
                    raise DomainError("METADATA_UNAVAILABLE")
                if status >= 400:
                    raise DomainError(result["error"]["code"])
                return result
        except (OSError, TimeoutError, ValueError, KeyError, h11.ProtocolError) as exc:
            raise DomainError("METADATA_UNAVAILABLE") from exc
        finally:
            if writer is not None:
                # Close control I/O before returning the slot; no HTTP worker thread.
                writer.transport.abort()
                try:
                    await writer.wait_closed()
                except (OSError, asyncio.CancelledError):
                    pass
            self.slots.release(progress)
