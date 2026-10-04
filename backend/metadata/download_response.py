"""HTTP owns the verified disk snapshot; finalization covers every send outcome."""

import asyncio
from urllib.parse import quote

import anyio
from starlette.responses import Response

from metadata.file_input import FileInputError, normalize_filename
from metadata.transfers import finish_cancelled, run_owned


class DownloadResponse(Response):
    chunk_size = 64 * 1024

    def __init__(self, temp):
        self.temp = temp
        try:
            filename = normalize_filename(temp.original_name)
        except FileInputError:
            filename = "download.bin"
        super().__init__(
            media_type="application/octet-stream",
            headers={
                "Content-Length": str(temp.size_bytes),
                "Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename, safe='')}",
                "X-File-Checksum-SHA256": temp.checksum_sha256,
            },
        )

    async def stream(self, send):
        await send({"type": "http.response.start", "status": 200, "headers": self.raw_headers})
        while True:
            read = asyncio.create_task(run_owned(self.temp.file.read, self.chunk_size))
            try:
                block = await asyncio.shield(read)
            except asyncio.CancelledError:
                await finish_cancelled(read)
                raise
            if not block:
                break
            await send({"type": "http.response.body", "body": block, "more_body": True})
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    async def __call__(self, scope, receive, send):
        try:
            # Monitor disconnect on every ASGI version, including spec >=2.4.
            async with anyio.create_task_group() as tasks:

                async def send_and_stop():
                    await self.stream(send)
                    tasks.cancel_scope.cancel()

                tasks.start_soon(send_and_stop)
                while True:
                    if (await receive())["type"] == "http.disconnect":
                        tasks.cancel_scope.cancel()
                        break
        finally:
            close = asyncio.create_task(run_owned(self.temp.close))
            try:
                await asyncio.shield(close)
            except asyncio.CancelledError:
                await finish_cancelled(close)
                raise
