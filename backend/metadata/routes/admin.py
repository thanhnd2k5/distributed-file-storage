import asyncio
from contextlib import suppress
from threading import Event

from fastapi import APIRouter, Request
from starlette.requests import ClientDisconnect

from metadata.errors import error_response
from metadata.repair import repair_files
from metadata.routes.files import watch_disconnect
from metadata.schemas import RepairRequest, RepairResult
from metadata.transfers import finish_cancelled, run_owned

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])


@router.post("/repair", response_model=RepairResult)
async def repair(request: Request, body: RepairRequest):
    cancel = Event()
    task = monitor = None
    try:
        task = asyncio.create_task(run_owned(repair_files, request.app.state, body, cancel))
        monitor = asyncio.create_task(watch_disconnect(request, cancel))
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        cancel.set()
        if task is not None:
            await finish_cancelled(task)
        raise
    except ClientDisconnect:
        return error_response(400, "INVALID_REQUEST", "Yêu cầu repair bị ngắt.")
    finally:
        if monitor is not None:
            monitor.cancel()
            with suppress(asyncio.CancelledError):
                await monitor
