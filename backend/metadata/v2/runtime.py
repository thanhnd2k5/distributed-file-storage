"""Explicit isolated V2 service entrypoints; no migrations or node provisioning."""

import asyncio
import math
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass, fields, replace
from threading import Thread
from uuid import UUID

from fastapi import FastAPI
from sqlalchemy import create_engine
from starlette.concurrency import run_in_threadpool

from metadata.v2.admission import AdmissionBudget, progress_request
from metadata.v2.http import RequestIds, error_response, handlers
from metadata.v2.repository import Policy, Repository, positive
from metadata.v2.worker import Worker, WorkerConfig


@dataclass(frozen=True)
class Limits:
    requests: int = 32
    streams: int = 8
    block_bytes: int = 262144
    queue_blocks: int = 2
    max_replicas: int = 16
    control_calls: int = 32
    request_reserve: int = 8
    control_reserve: int = 8
    buffered_bytes: int = 128 * 1024**2
    request_seconds: float = 120
    control_seconds: float = 20
    renew_seconds: float = 20
    shutdown_seconds: float = 10
    download_streams: int = 4
    download_chunk_bytes: int = 8388608
    download_buffered_bytes: int = 36 * 1024**2
    download_rpc_seconds: float = 5
    download_seconds: float = 86400
    download_renew_seconds: float = 10

    def __post_init__(self):
        AdmissionBudget(self.requests, self.request_reserve)
        AdmissionBudget(self.control_calls, self.control_reserve)
        for name in (
            "requests",
            "streams",
            "block_bytes",
            "queue_blocks",
            "max_replicas",
            "control_calls",
            "buffered_bytes",
            "download_streams",
            "download_chunk_bytes",
            "download_buffered_bytes",
        ):
            positive(getattr(self, name))
        if self.block_bytes > 262144 or self.max_replicas > 16 or self.queue_blocks > 2:
            raise ValueError("Wire/queue bounds exceeded")
        if self.streams > self.requests or self.renew_seconds > 20:
            raise ValueError("Invalid stream/renew budget")
        # Includes one producer and one serialization block per destination.
        required = (
            self.streams * self.block_bytes * (self.max_replicas * (self.queue_blocks + 1) + 1)
        )
        if self.buffered_bytes < required:
            raise ValueError("Insufficient configured application buffer budget")
        # One verified chunk and two wire/serialization blocks per download.
        if self.download_buffered_bytes < self.download_streams * (
            self.download_chunk_bytes + 2 * 262144
        ):
            raise ValueError("Insufficient download buffer reservation")
        if (
            self.download_streams > self.requests
            or self.download_renew_seconds > 10
            or self.download_rpc_seconds > 120
            or self.download_seconds > 86400
        ):
            raise ValueError("Invalid download budget")
        for name in (
            "request_seconds",
            "control_seconds",
            "renew_seconds",
            "shutdown_seconds",
            "download_rpc_seconds",
            "download_seconds",
            "download_renew_seconds",
        ):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError("Finite positive durations required")

    @classmethod
    def from_env(cls):
        return cls(
            **{
                f.name: f.type(os.environ["V2_" + f.name.upper()])
                for f in fields(cls)
                if "V2_" + f.name.upper() in os.environ
            }
        )


class Admission:
    """ASGI admission before body consumption, no waiting queue; one event loop."""

    def __init__(self, app, limit, state=None, *, reserve=None):
        self.app, self.limit = app, limit
        self.budget = AdmissionBudget(limit, reserve)
        self.state, self.health = state, 0
        self.traced = RequestIds(self.admit)

    async def __call__(self, scope, receive, send):
        return await self.traced(scope, receive, send)

    async def admit(self, scope, receive, send):
        if scope["type"] != "http" or scope["path"] == "/health/live":
            return await self.app(scope, receive, send)
        if self.state is not None and self.state.closing:
            return await error_response("METADATA_UNAVAILABLE")(scope, receive, send)
        if scope["path"] == "/health/ready":
            if self.health >= 2:
                return await error_response("RESOURCE_LIMIT")(scope, receive, send)
            self.health += 1
            try:
                return await self.app(scope, receive, send)
            finally:
                self.health -= 1
        progress = progress_request(scope["method"], scope["path"])
        if not self.budget.acquire(progress):
            return await error_response("RESOURCE_LIMIT")(scope, receive, send)
        try:
            await self.app(scope, receive, send)
        finally:
            self.budget.release(progress)

    @property
    def active(self):
        return self.budget.active


def worker_config():
    return WorkerConfig(
        **{
            f.name: f.type(os.environ["V2_WORKER_" + f.name.upper()])
            for f in fields(WorkerConfig)
            if "V2_WORKER_" + f.name.upper() in os.environ
        }
    )


def worker_app(repo, config):
    worker = Worker(repo, config=config)

    @asynccontextmanager
    async def lifespan(app):
        thread = Thread(target=worker.run, name="v2-worker", daemon=True)
        thread.start()
        try:
            for _ in range(100):
                if worker.ready():
                    break
                if not thread.is_alive():
                    raise RuntimeError("Worker startup failed")
                await asyncio.sleep(0.05)
            else:
                raise RuntimeError("Worker startup timed out")
            yield
        finally:
            worker.stopping.set()
            await run_in_threadpool(thread.join, config.shutdown_seconds)
            if thread.is_alive():
                raise RuntimeError("Worker failed to drain")

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.worker = worker
    handlers(app)

    @app.get("/health/ready")
    async def ready():
        if not await run_in_threadpool(worker.ready):
            return error_response("METADATA_UNAVAILABLE")
        return {"ready": True}

    @app.get("/health/live")
    async def live():
        return {"live": True}

    return app


def app_factory():
    limits = Limits.from_env()
    role = os.environ["V2_SERVICE_ROLE"]
    engine = None
    if role == "transfer":
        from metadata.v2.http import JsonClient
        from transfer.v2.app import create_app

        app = create_app(
            JsonClient(
                os.environ["V2_API_URL"],
                os.environ["V2_INTERNAL_URL"],
                UUID(os.environ["V2_RECOVERY_EPOCH"]),
                timeout=limits.control_seconds,
                max_calls=limits.control_calls,
                reserve=limits.control_reserve,
            ),
            limits=limits,
        )
    else:
        engine = create_engine(
            os.environ["V2_DATABASE_URL"],
            pool_size=8,
            max_overflow=0,
            pool_timeout=5,
            connect_args={"connect_timeout": 5},
        )
        repo = Repository(
            engine,
            UUID(os.environ["V2_NAMESPACE_ID"]),
            UUID(os.environ["V2_RECOVERY_EPOCH"]),
            schema=os.environ.get("V2_DATABASE_SCHEMA", "public"),
            policy=Policy(
                **{
                    f.name: int(os.environ["V2_POLICY_" + f.name.upper()])
                    for f in fields(Policy)
                    if "V2_POLICY_" + f.name.upper() in os.environ
                }
            ),
            max_job_backlog=int(os.environ.get("V2_JOB_BACKLOG_LIMIT", "1024")),
        )
        if repo.policy.replication_factor > limits.max_replicas:
            engine.dispose()
            raise ValueError("Policy exceeds wire ACK and Transfer replica bounds")
        if role == "worker":
            app = worker_app(repo, worker_config())
        elif role in ("api", "internal"):
            from metadata.v2.api import create_app

            app = create_app(
                repo,
                internal=role == "internal",
                retry_config=replace(
                    worker_config(),
                    job_seconds=limits.control_seconds * 0.75,
                    shutdown_seconds=limits.shutdown_seconds,
                ),
            )
        else:
            engine.dispose()
            raise ValueError("Unknown V2_SERVICE_ROLE")
        original = app.router.lifespan_context

        @asynccontextmanager
        async def lifespan(app):
            try:
                async with original(app):
                    yield
            finally:
                engine.dispose()

        app.router.lifespan_context = lifespan
    app.state.closing = False
    app.add_middleware(
        Admission, limit=limits.requests, state=app.state, reserve=limits.request_reserve
    )
    return app


def main():
    import uvicorn

    limits = Limits.from_env()
    app = app_factory()

    class Server(uvicorn.Server):
        def handle_exit(self, sig, frame):
            app.state.closing = True
            if hasattr(app.state, "transfer"):
                app.state.transfer.stopping = True
            if hasattr(app.state, "worker"):
                app.state.worker.stopping.set()
            super().handle_exit(sig, frame)

    server = Server(
        uvicorn.Config(
            app,
            host=os.environ.get("V2_BIND_HOST", "0.0.0.0"),
            port=int(os.environ.get("V2_PORT", "8080")),
            timeout_graceful_shutdown=limits.shutdown_seconds,
            timeout_keep_alive=5,
        )
    )
    server.run()


if __name__ == "__main__":
    main()
