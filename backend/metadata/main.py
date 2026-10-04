import logging
from contextlib import asynccontextmanager
from threading import Lock

from fastapi import FastAPI
from fastapi.concurrency import run_in_threadpool
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from starlette.exceptions import HTTPException

from common.logging import configure_logging
from metadata.bootstrap import initialize_metadata
from metadata.config import MetadataSettings
from metadata.db import create_database
from metadata.download_temp import initialize_download_temp
from metadata.errors import error_response, http_error, operation_error, validation_error
from metadata.operations import OperationError
from metadata.routes.cluster import router as cluster_router
from metadata.routes.files import router as files_router
from metadata.storage_client import StorageClient
from metadata.worker import MetadataWorker

logger = logging.getLogger(__name__)


def create_app(settings: MetadataSettings | None = None) -> FastAPI:
    settings = settings or MetadataSettings()
    configure_logging(settings.log_level)
    engine, session_factory = create_database(settings.database_url)

    def initialize_under_lock():
        with app.state.operation_lock:
            initialize_metadata(session_factory, settings)
            initialize_download_temp(settings.download_temp_dir)

    @asynccontextmanager
    async def lifespan(app):
        worker = None
        storage_client = None
        app.state.initialized = False
        app.state.health_worker = None
        app.state.storage_client = None
        try:
            await run_in_threadpool(initialize_under_lock)
            worker = MetadataWorker(session_factory, settings)
            app.state.health_worker = worker
            await run_in_threadpool(worker.start)
            storage_client = StorageClient(settings)
            app.state.storage_client = storage_client
            await run_in_threadpool(storage_client.start)
            app.state.initialized = True
            logger.info("Metadata registry and startup recovery initialized")
        except Exception:
            logger.exception("Metadata startup failed; readiness remains unavailable")
            if storage_client is not None:
                await run_in_threadpool(storage_client.close)
            if worker is not None:
                await run_in_threadpool(worker.stop)
        try:
            yield
        finally:
            app.state.initialized = False

            def drain_data_operations():
                # Shutdown admission is closed; accepted operations may still need DB commits.
                with app.state.operation_lock:
                    if storage_client is not None:
                        storage_client.close()

            try:
                await run_in_threadpool(drain_data_operations)
            finally:
                try:
                    if worker is not None:
                        await run_in_threadpool(worker.stop)
                finally:
                    await run_in_threadpool(engine.dispose)

    app = FastAPI(title="Distributed File Storage", version="0.1.0", lifespan=lifespan)
    app.state.initialized = False
    app.state.health_worker = None
    app.state.storage_client = None
    app.state.settings = settings
    app.state.engine = engine
    app.state.session_factory = session_factory
    app.state.operation_lock = Lock()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_methods=["GET", "POST", "DELETE"],
        allow_headers=["Content-Type"],
        expose_headers=["Content-Disposition", "X-File-Checksum-SHA256"],
    )
    app.add_exception_handler(RequestValidationError, validation_error)
    app.add_exception_handler(HTTPException, http_error)
    app.add_exception_handler(OperationError, operation_error)
    app.include_router(cluster_router)
    app.include_router(files_router)

    @app.get("/api/v1/health/live", tags=["health"])
    def live():
        return {"status": "LIVE"}

    @app.get("/api/v1/health/ready", tags=["health"])
    def ready():
        try:
            if not app.state.initialized:
                raise RuntimeError("Startup initialization incomplete")
            worker = app.state.health_worker
            if worker is None or not worker.running:
                raise RuntimeError("Health scheduler is not running")
            if app.state.storage_client is None or not app.state.storage_client.running:
                raise RuntimeError("Data RPC client is not running")
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
        except Exception:
            return error_response(503, "METADATA_UNAVAILABLE", "Metadata chưa sẵn sàng.")
        return {"status": "READY"}

    return app
