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
from metadata.errors import error_response, http_error, validation_error

logger = logging.getLogger(__name__)


def create_app(settings: MetadataSettings | None = None) -> FastAPI:
    settings = settings or MetadataSettings()
    configure_logging(settings.log_level)
    engine, session_factory = create_database(settings.database_url)

    @asynccontextmanager
    async def lifespan(app):
        try:
            await run_in_threadpool(initialize_metadata, session_factory, settings)
            app.state.initialized = True
            logger.info("Metadata initialized; data operations are not implemented in M0")
        except Exception:
            logger.exception("Metadata startup failed; readiness remains unavailable")
        try:
            yield
        finally:
            app.state.initialized = False
            engine.dispose()

    app = FastAPI(title="Distributed File Storage", version="0.1.0", lifespan=lifespan)
    app.state.initialized = False
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

    @app.get("/api/v1/health/live", tags=["health"])
    def live():
        return {"status": "LIVE"}

    @app.get("/api/v1/health/ready", tags=["health"])
    def ready():
        try:
            if not app.state.initialized:
                raise RuntimeError("Startup initialization incomplete")
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
        except Exception:
            return error_response(503, "METADATA_UNAVAILABLE", "Metadata chưa sẵn sàng.")
        return {"status": "READY"}

    return app
