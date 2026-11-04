"""D3 catalog and read commands. Every public DTO is an explicit projection."""

import asyncio
import base64
import hashlib
import json
from datetime import datetime

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from pydantic import Field, StrictInt, StrictStr, field_validator
from starlette.concurrency import run_in_threadpool

from metadata.v2.http import Internal, StrictDTO, body, integer, uuid_value, wire
from metadata.v2.repository import DomainError


class CatalogCursor(StrictDTO):
    v: StrictInt = Field(ge=1, le=1)
    state: StrictStr = Field(pattern="^AVAILABLE$")
    scope: StrictStr = Field(pattern="^[0-9a-f]{64}$")
    created_at: StrictStr
    file_id: StrictStr


class AcquireRead(Internal):
    file_id: StrictStr
    version_id: StrictStr | None = None

    @field_validator("file_id", "version_id")
    @classmethod
    def ids(cls, value):
        if value is not None:
            uuid_value(value)
        return value


class ReadToken(Internal):
    lease_token: StrictStr

    @field_validator("lease_token")
    @classmethod
    def token(cls, value):
        uuid_value(value)
        return value


def file_result(file):
    return wire(
        dict(
            **{
                k: file[k]
                for k in (
                    "file_id",
                    "version_id",
                    "original_name",
                    "content_type",
                    "size_bytes",
                    "state",
                    "created_at",
                    "available_at",
                )
            },
            checksum_sha256=file["actual_sha256"] if file["state"] == "AVAILABLE" else None,
        )
    )


def query(request, allowed):
    pairs = list(request.query_params.multi_items())
    if any(k not in allowed for k, _ in pairs) or len(pairs) != len(dict(pairs)):
        raise ValueError("Unknown or repeated query parameter")
    return dict(pairs)


def page_query(request):
    values = query(request, {"after_index", "limit"})
    return dict(
        after_index=integer(values.get("after_index", "-1"), minimum=-1),
        limit=integer(values.get("limit", "100"), minimum=1, maximum=1000),
    )


def cursor_scope(repo):
    return hashlib.sha256(
        f"catalog:1:AVAILABLE:{repo.namespace_id}:{repo.epoch}".encode()
    ).hexdigest()


def cursor_encode(repo, file):
    value = wire(
        dict(
            v=1,
            state="AVAILABLE",
            scope=cursor_scope(repo),
            created_at=file["created_at"],
            file_id=file["file_id"],
        )
    )
    return (
        base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode())
        .decode()
        .rstrip("=")
    )


def cursor_decode(repo, value):
    if not value or len(value) > 1024:
        raise ValueError("Invalid catalog cursor")
    raw = base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
    if base64.urlsafe_b64encode(raw).decode().rstrip("=") != value:
        raise ValueError("Noncanonical cursor")
    data = CatalogCursor.model_validate_json(raw)
    if data.scope != cursor_scope(repo):
        raise ValueError("Cursor scope mismatch")
    created = datetime.fromisoformat(data.created_at)
    if created.tzinfo is None or wire(created) != data.created_at:
        raise ValueError("UTC timestamp required")
    return created, uuid_value(data.file_id)


def single_header(request, name):
    values = request.headers.getlist(name)
    if len(values) != 1:
        raise ValueError("One canonical internal header required")
    return uuid_value(values[0])


def read_result(read):
    result = {
        k: read[k]
        for k in (
            "read_id",
            "lease_token",
            "file_id",
            "version_id",
            "created_at",
            "expires_at",
            "absolute_expires_at",
            "db_now",
        )
    }
    if "file" in read:
        file = read["file"]
        result.update(
            **{
                k: file[k]
                for k in ("size_bytes", "original_name", "chunk_size_bytes", "part_count")
            },
            checksum_sha256=file["actual_sha256"],
        )
    return wire(result)


def register_routes(app, repo, *, internal):
    if not internal:

        @app.get("/api/v2/files")
        async def catalog(request: Request):
            values = query(request, {"cursor", "limit"})
            after = cursor_decode(repo, values["cursor"]) if "cursor" in values else None
            limit = integer(values.get("limit", "50"), minimum=1, maximum=100)
            items, more = await run_in_threadpool(repo.file_catalog, after=after, limit=limit)
            return dict(
                items=[file_result(f) for f in items],
                next_cursor=cursor_encode(repo, items[-1]) if more else None,
            )

        @app.get("/api/v2/files/{file}")
        async def detail(file: str, request: Request):
            query(request, set())
            return file_result(await run_in_threadpool(repo.file_record, uuid_value(file)))

        @app.get("/api/v2/files/{file}/chunks")
        async def chunks(file: str, request: Request):
            result = await run_in_threadpool(
                repo.catalog_manifest, uuid_value(file), **page_query(request)
            )
            for part in result["items"]:
                part["index"] = part.pop("part_index")
                part.pop("version_id")
            return wire(result)

        @app.delete("/api/v2/files/{file}")
        async def delete(file: str, request: Request):
            query(request, set())
            # This command has no JSON body; reject even chunked unexpected data.
            try:
                async with asyncio.timeout(10):
                    async for block in request.stream():
                        if block:
                            raise DomainError("INVALID_REQUEST")
            except TimeoutError as exc:
                raise DomainError("INVALID_REQUEST") from exc
            result = await run_in_threadpool(repo.delete_file, uuid_value(file))
            return JSONResponse(wire(result), status_code=202 if result["cleanup_pending"] else 200)

    else:

        async def command(request, dto):
            query(request, set())
            data = await body(request, dto)
            if uuid_value(data.recovery_epoch) != repo.epoch:
                raise DomainError("RECOVERY_REQUIRED")
            return data

        @app.post("/internal/v2/downloads")
        async def acquire(request: Request):
            data = await command(request, AcquireRead)
            result = await run_in_threadpool(
                repo.acquire_read,
                uuid_value(data.file_id),
                version_id=uuid_value(data.version_id) if data.version_id is not None else None,
            )
            return read_result(result)

        @app.get("/internal/v2/downloads/{read}/manifest")
        async def manifest(read: str, request: Request):
            if single_header(request, "x-recovery-epoch") != repo.epoch:
                raise DomainError("RECOVERY_REQUIRED")
            result = await run_in_threadpool(
                repo.read_manifest,
                uuid_value(read),
                single_header(request, "x-read-lease-token"),
                **page_query(request),
            )
            return wire(result)

        @app.post("/internal/v2/downloads/{read}/renew")
        async def renew(read: str, request: Request):
            data = await command(request, ReadToken)
            return read_result(
                await run_in_threadpool(
                    repo.renew_read, uuid_value(read), uuid_value(data.lease_token)
                )
            )

        @app.post("/internal/v2/downloads/{read}/release", status_code=204)
        async def release(read: str, request: Request):
            data = await command(request, ReadToken)
            await run_in_threadpool(
                repo.release_read, uuid_value(read), uuid_value(data.lease_token)
            )
            return Response(status_code=204)
