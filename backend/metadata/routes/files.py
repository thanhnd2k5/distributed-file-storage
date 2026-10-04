import asyncio
import logging
from contextlib import suppress
from threading import Event
from typing import Annotated
from uuid import UUID

import anyio
from fastapi import APIRouter, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, Response
from sqlalchemy.exc import SQLAlchemyError
from starlette.datastructures import UploadFile
from starlette.formparsers import MultiPartException, MultiPartParser
from starlette.requests import ClientDisconnect

from metadata.download import DownloadError, prepare_download
from metadata.download import check_cancel as check_download_cancel
from metadata.download_response import DownloadResponse
from metadata.errors import error_response
from metadata.file_input import FileInputError, invalid_request, temp_storage_unavailable
from metadata.files import FileNotFound, file_chunks, file_detail, file_snapshot, list_files
from metadata.operations import OperationError
from metadata.schemas import ChunkList, FileDetail, FileList, FileSummary
from metadata.transfers import finish_cancelled, run_owned
from metadata.upload import upload_file

router = APIRouter(prefix="/api/v1", tags=["files"])
logger = logging.getLogger(__name__)


def read_view(state, view, *args):
    if not state.initialized:
        return error_response(503, "METADATA_UNAVAILABLE", "Metadata chưa sẵn sàng.")
    try:
        with file_snapshot(state.session_factory) as session:
            return view(session, *args)
    except FileNotFound:
        return error_response(404, "FILE_NOT_FOUND", "Không tìm thấy file.")
    except SQLAlchemyError as exc:
        logger.warning("Cannot read file snapshot (%s)", type(exc).__name__)
        return error_response(503, "METADATA_UNAVAILABLE", "Metadata chưa sẵn sàng.")


@router.get("/files", response_model=FileList)
def files(
    request: Request,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    include_inactive: bool = False,
):
    return read_view(request.app.state, list_files, limit, offset, include_inactive)


@router.get("/files/{file_id}", response_model=FileDetail)
def detail(request: Request, file_id: UUID):
    return read_view(request.app.state, file_detail, file_id)


@router.get("/files/{file_id}/chunks", response_model=ChunkList)
def chunks(request: Request, file_id: UUID):
    return read_view(request.app.state, file_chunks, file_id)


class OwnedUploadFile(UploadFile):
    """Cancellation cannot leave parser I/O running against a closed spool."""

    async def _io(self, function, *args):
        task = asyncio.create_task(run_owned(function, *args))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await finish_cancelled(task)
            raise

    async def write(self, data):
        if self.size is not None:
            self.size += len(data)
        await self._io(self.file.write, data)

    async def seek(self, offset):
        await self._io(self.file.seek, offset)

    async def close(self):
        await self._io(self.file.close)


class CompleteMultipartParser(MultiPartParser):
    """The pinned multipart parser's finalize() does not reject truncated bodies."""

    complete = False

    def __init__(self, *args, file_size_limit, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_size_limit = file_size_limit
        self.file_bytes = 0

    def on_part_begin(self):
        super().on_part_begin()
        self.file_bytes = 0

    def on_headers_finished(self):
        try:
            # Validate a text codec before decoding fields or creating a spool.
            # Empty bytes can bypass codec lookup in Python. Some valid text
            # codecs need more than one byte, so only lookup errors are invalid.
            b"\0".decode(self._charset)
        except UnicodeError:
            pass
        except LookupError:
            raise MultiPartException("Invalid multipart charset") from None
        super().on_headers_finished()
        original = self._current_part.file
        if original is not None:
            self._current_part.file = OwnedUploadFile(
                original.file,
                size=original.size,
                filename=original.filename,
                headers=original.headers,
            )

    def on_part_data(self, data, start, end):
        if self._current_part.file is not None:
            self.file_bytes += end - start
            if self.file_bytes > self.file_size_limit:
                raise FileInputError(413, "FILE_TOO_LARGE", "File vượt quá dung lượng cho phép.")
        super().on_part_data(data, start, end)

    def on_end(self):
        self.complete = True

    async def parse(self):
        try:
            form = await super().parse()
            if not self.complete:
                raise MultiPartException("Multipart body is incomplete")
            return form
        except BaseException as exc:
            # Includes unfinished parts not present in FormData.items. This is
            # the cleanup registry of the pinned Starlette parser.
            for spool in self._files_to_close_on_error:
                spool.close()
            if isinstance(exc, (LookupError, UnicodeError)):
                raise MultiPartException("Invalid multipart text encoding") from None
            raise


async def transfer(request, parts, cancel):
    # Coroutine cancellation must not abandon the owning thread with live spools.
    with anyio.CancelScope(shield=True):
        return await run_in_threadpool(upload_file, request.app.state, parts, cancel)


async def watch_disconnect(request, cancel):
    # Request body has been consumed; only this task owns receive now.
    while True:
        if (await request.receive())["type"] == "http.disconnect":
            cancel.set()
            return


@router.get(
    "/files/{file_id}/download",
    response_class=Response,
    responses={
        200: {
            "content": {
                "application/octet-stream": {"schema": {"type": "string", "format": "binary"}}
            }
        }
    },
)
async def download(request: Request, file_id: UUID):
    cancel = Event()
    task = monitor = temp = None
    try:
        # Consume a possible GET body before assigning receive to the monitor.
        async for _ in request.stream():
            pass
        task = asyncio.create_task(run_owned(prepare_download, request.app.state, file_id, cancel))
        monitor = asyncio.create_task(watch_disconnect(request, cancel))
        temp = await asyncio.shield(task)
        monitor.cancel()
        with suppress(asyncio.CancelledError):
            await monitor
        monitor = None
        check_download_cancel(cancel, file_id)
        response = DownloadResponse(temp)
        temp = None  # Ownership moves to the response, with no await before returning it.
        return response
    except asyncio.CancelledError:
        cancel.set()
        if task is not None:
            await finish_cancelled(task)
            if temp is None and not task.cancelled() and task.exception() is None:
                temp = task.result()
        raise
    except DownloadError as exc:
        return error_response(exc.status_code, exc.code, str(exc), exc.details)
    except ClientDisconnect:
        return error_response(400, "INVALID_REQUEST", "Download bị ngắt khi nhận yêu cầu.")
    finally:
        with anyio.CancelScope(shield=True):
            if monitor is not None:
                monitor.cancel()
                with suppress(asyncio.CancelledError):
                    await monitor
            if temp is not None:
                close = asyncio.create_task(run_owned(temp.close))
                try:
                    await asyncio.shield(close)
                except asyncio.CancelledError:
                    await finish_cancelled(close)
                    raise


@router.post(
    "/files",
    response_model=FileSummary,
    status_code=201,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "multipart/form-data": {
                    "schema": {
                        "type": "object",
                        "required": ["file"],
                        "properties": {"file": {"type": "string", "format": "binary"}},
                        "additionalProperties": False,
                    }
                }
            },
        }
    },
)
async def upload(request: Request):
    form = None
    task = monitor = None
    cancel = Event()
    try:
        if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != (
            "multipart/form-data"
        ):
            raise invalid_request("Upload phải dùng multipart/form-data.")
        form = await CompleteMultipartParser(
            request.headers,
            request.stream(),
            max_files=2,
            max_fields=1,
            file_size_limit=request.app.state.settings.max_file_size_bytes,
        ).parse()
        task = asyncio.create_task(transfer(request, form.multi_items(), cancel))
        monitor = asyncio.create_task(watch_disconnect(request, cancel))
        try:
            result = await asyncio.shield(task)
        except asyncio.CancelledError:
            cancel.set()
            await finish_cancelled(task)
            raise
        return JSONResponse(
            status_code=201,
            content=result.model_dump(mode="json"),
            headers={"Location": f"/api/v1/files/{result.file_id}"},
        )
    except (FileInputError, OperationError) as exc:
        file_id = getattr(exc, "file_id", None)
        details = {"file_id": str(file_id)} if file_id is not None else None
        return error_response(exc.status_code, exc.code, str(exc), details)
    except OSError:
        exc = temp_storage_unavailable()
        return error_response(exc.status_code, exc.code, str(exc))
    except MultiPartException:
        return error_response(400, "INVALID_REQUEST", "Multipart không hợp lệ.")
    except ClientDisconnect:
        # No coordinator has started when parsing disconnects.
        return error_response(400, "INVALID_REQUEST", "Upload bị ngắt khi nhận dữ liệu.")
    finally:
        if monitor is not None:
            monitor.cancel()
            with suppress(asyncio.CancelledError):
                await monitor
        if form is not None:
            with anyio.CancelScope(shield=True):
                await form.close()
