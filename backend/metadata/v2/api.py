"""Upload control plane. Public and internal listeners are separate ASGI apps."""

from contextlib import asynccontextmanager
from dataclasses import asdict
from uuid import UUID

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from metadata.v2.download_api import register_routes
from metadata.v2.http import (
    Claim,
    Confirm,
    CreateUpload,
    Empty,
    Lease,
    Uncertain,
    body,
    handlers,
    integer,
    part_result,
    session_result,
    uuid_value,
    wire,
)
from metadata.v2.repository import DomainError
from metadata.v2.worker import Worker, WorkerConfig


def create_app(repo, *, internal=False, retry_config=None):
    # This instance never runs a background loop. Its claim is handed directly
    # to the waiting HTTP caller; lost responses recover through claim expiry.
    consumer = (
        Worker(repo, config=retry_config or WorkerConfig(job_seconds=15)) if internal else None
    )

    @asynccontextmanager
    async def lifespan(app):
        await run_in_threadpool(repo.ready)
        yield
        if consumer:
            await run_in_threadpool(consumer.close)

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    handlers(app, internal=internal)
    app.state.repo = repo
    register_routes(app, repo, internal=internal)

    @app.get("/health/live")
    async def live():
        return {"live": True}

    @app.get("/health/ready")
    async def ready():
        await run_in_threadpool(repo.ready)
        return {"ready": True}

    async def session(upload):
        return session_result(await run_in_threadpool(repo.upload_status, upload))

    if not internal:

        @app.get("/api/v2/config")
        async def config():
            return dict(
                asdict(repo.policy),
                max_block_bytes=262144,
                max_json_bytes=16384,
                max_job_backlog=repo.max_job_backlog,
            )

        @app.post("/api/v2/uploads")
        async def create(request: Request):
            data = await body(request, CreateUpload)
            key = uuid_value(request.headers.get("idempotency-key"))
            result, replay = await run_in_threadpool(repo.create_upload, key, **data.model_dump())
            upload = result["upload_id"]
            return JSONResponse(
                await session(upload),
                status_code=200 if replay else 201,
                headers={"Location": f"/api/v2/uploads/{upload}"},
            )

        @app.get("/api/v2/uploads/{upload}")
        async def status(upload: str):
            return await session(uuid_value(upload))

        @app.get("/api/v2/uploads/{upload}/parts")
        async def parts(upload: str, request: Request):
            if set(request.query_params) - {"after_index", "limit"}:
                raise ValueError("Unknown query")
            after = integer(request.query_params.get("after_index", "-1"), minimum=-1)
            limit = integer(request.query_params.get("limit", "100"), minimum=1, maximum=1000)
            records, more = await run_in_threadpool(
                repo.part_page, uuid_value(upload), after_index=after, limit=limit
            )
            return dict(
                items=[part_result(p) for p in records],
                has_more=more,
                next_after_index=records[-1]["part_index"] if more else None,
            )

        @app.post("/api/v2/uploads/{upload}/complete")
        async def complete(upload: str, request: Request):
            await body(request, Empty)
            uid = uuid_value(upload)
            observed = await run_in_threadpool(repo.get_session, uid)
            if observed["state"] in ("ABORTED", "EXPIRED"):
                raise DomainError("UPLOAD_" + observed["state"])
            await run_in_threadpool(repo.complete_upload, uid)
            result = await session(uid)
            return JSONResponse(
                result,
                status_code=200 if result["state"] == "COMPLETED" else 202,
                headers={"Location": f"/api/v2/uploads/{uid}"},
            )

        @app.post("/api/v2/uploads/{upload}/abort")
        async def abort(upload: str, request: Request):
            await body(request, Empty)
            uid = uuid_value(upload)
            state = await run_in_threadpool(repo.abort, uid)
            if state == "EXPIRED":
                raise DomainError("UPLOAD_EXPIRED")
            result = await session(uid)
            return JSONResponse(result, status_code=202 if result["cleanup_pending"] else 200)
    else:

        async def command(request, dto):
            data = await body(request, dto)
            if UUID(data.recovery_epoch) != repo.epoch:
                raise DomainError("RECOVERY_REQUIRED")
            return data

        @app.post("/internal/v2/uploads/{upload}/parts/{index}/claim")
        async def claim(upload: str, index: str, request: Request):
            data = await command(request, Claim)
            uid, idx = uuid_value(upload), integer(index)
            owner = uuid_value(data.request_id)
            try:
                result = await run_in_threadpool(
                    repo.claim_part, uid, idx, data.size_bytes, data.checksum_sha256, owner
                )
            except DomainError as exc:
                if exc.code != "OUTCOME_PENDING":
                    raise
                part = await run_in_threadpool(repo.part_record, uid, idx)
                # Worker validates generation/spec again before any handoff.
                # Serialize only this dedicated consumer; never queue waiters.
                if app.state.retry_busy:
                    raise DomainError("OUTCOME_PENDING")
                app.state.retry_busy = True
                try:
                    result = await run_in_threadpool(
                        consumer.retry_for_consumer, part["chunk_id"], owner
                    )
                finally:
                    app.state.retry_busy = False
                if result is None:
                    raise DomainError("OUTCOME_PENDING")
            return wire(result)

        app.state.retry_busy = False

        @app.post("/internal/v2/uploads/{upload}/parts/{index}/renew")
        async def renew(upload: str, index: str, request: Request):
            data = await command(request, Lease)
            expires = await run_in_threadpool(
                repo.renew_part,
                uuid_value(upload),
                integer(index),
                uuid_value(data.claim_token),
                data.revision,
            )
            return {"claim_expires_at": wire(expires)}

        @app.post("/internal/v2/uploads/{upload}/parts/{index}/confirm")
        async def confirm(upload: str, index: str, request: Request):
            data = await command(request, Confirm)
            result = await run_in_threadpool(
                repo.confirm_part,
                uuid_value(upload),
                integer(index),
                uuid_value(data.claim_token),
                data.revision,
                [a.evidence() for a in data.allocations],
            )
            return part_result(result)

        @app.post("/internal/v2/uploads/{upload}/parts/{index}/uncertain")
        async def uncertain(upload: str, index: str, request: Request):
            data = await command(request, Uncertain)
            uid, idx = uuid_value(upload), integer(index)
            part = await run_in_threadpool(repo.part_record, uid, idx)
            # Validate UUIDs; durable repository intent always covers all replicas.
            if not data.allocation_ids or any(not uuid_value(a) for a in data.allocation_ids):
                raise ValueError("Invalid allocations")
            if str(part["claim_token"]) != data.claim_token or part["revision"] != data.revision:
                raise DomainError("STALE_CLAIM")
            await run_in_threadpool(
                repo.uncertain, uid, idx, uuid_value(data.claim_token), data.revision
            )
            return {"pending": True}

    return app
