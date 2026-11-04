"""D5-only observation/fault entrypoint around unchanged production services."""

import asyncio
import json
import os
import time
from pathlib import Path
from uuid import uuid4

import uvicorn

ROOT = Path(os.environ["V2_D5_CONTROL"])
LABEL = os.environ["V2_D5_LABEL"]


def event(kind, **values):
    with (ROOT / f"{LABEL}.jsonl").open("a") as log:
        log.write(json.dumps(dict(kind=kind, at=time.monotonic(), **values)) + "\n")


def storage():
    from storage.v2.server import main
    from storage.v2.service import StorageService

    original = StorageService.GetChunk

    def read(self, request, context):
        chunk, call = request.expected_chunk.chunk_id, str(uuid4())
        path = ROOT / f"{LABEL}-fault.json"
        fault = json.loads(path.read_text()) if path.exists() else {}
        if fault.get("chunk_id", chunk) != chunk:
            fault = {}
        total = 0
        event("read_start", call=call, chunk=chunk)
        try:
            for frame in original(self, request, context):
                kind = frame.WhichOneof("payload")
                if kind == "block":
                    total += len(frame.block.data)
                    if delay := fault.get("delay", 0):
                        time.sleep(delay)
                if kind == "footer":
                    if fault.get("mode") == "missing_footer":
                        continue
                    if fault.get("mode") == "hold_eof":
                        event("held_eof", call=call, chunk=chunk)
                        while (ROOT / "hold-eof").exists() and context.is_active():
                            time.sleep(0.01)
                yield frame
            if fault.get("mode") == "eof_error":
                import grpc

                context.abort(grpc.StatusCode.UNAVAILABLE, "D5 fault after footer")
        finally:
            event(
                "read_end",
                call=call,
                chunk=chunk,
                payload_bytes=total,
                cancelled=not context.is_active(),
            )

    StorageService.GetChunk = read
    main()


def http():
    from metadata.v2.runtime import app_factory

    app = app_factory()
    stats = {
        "read_chunks": 0,
        "max_buffers": 0,
        "max_downloads": 0,
        "max_uploads": 0,
        "max_send_seconds": 0,
        "body_bytes": 0,
    }
    buffers = {}
    if hasattr(app.state, "transfer"):
        transfer = app.state.transfer
        original_read = transfer.downloads.read_chunk
        original_api = transfer.api.call

        async def read(part, buffer):
            task = asyncio.current_task()
            if task not in buffers:
                buffers[task] = id(buffer)
                task.add_done_callback(lambda done: buffers.pop(done, None))
            assert buffers[task] == id(buffer), "More than one chunk buffer for a stream"
            stats["read_chunks"] += 1
            stats["max_buffers"] = max(stats["max_buffers"], len(buffers))
            stats["max_downloads"] = max(stats["max_downloads"], len(transfer.downloads.active))
            stats["max_uploads"] = max(stats["max_uploads"], len(transfer.active))
            await original_read(part, buffer)

        async def call(method, path, *args, **kwargs):
            if path == "/internal/v2/downloads" and (ROOT / "hold-acquire").exists():
                event("held_acquire")
                while (ROOT / "hold-acquire").exists():
                    await asyncio.sleep(0.01)
            result = await original_api(method, path, *args, **kwargs)
            if path.endswith("/renew") and "/downloads/" in path:
                event("renewed")
            return result

        transfer.downloads.read_chunk = read
        transfer.api.call = call

    @app.get("/_d5/metrics")
    async def metrics():
        return dict(
            stats,
            buffers=len(buffers),
            downloads=len(app.state.transfer.downloads.active),
            uploads=len(app.state.transfer.active),
        )

    class Observe:
        async def __call__(self, scope, receive, send):
            async def observed(message):
                started = time.monotonic()
                await send(message)
                if message["type"] == "http.response.body":
                    stats["body_bytes"] += len(message.get("body", b""))
                    stats["max_send_seconds"] = max(
                        stats["max_send_seconds"], time.monotonic() - started
                    )

            await app(
                scope, receive, observed if scope.get("path", "").endswith("/download") else send
            )

    uvicorn.run(
        Observe(),
        host="127.0.0.1",
        port=int(os.environ["V2_PORT"]),
        log_level="warning",
        timeout_graceful_shutdown=3,
    )


if __name__ == "__main__":
    storage() if os.environ["V2_SERVICE_ROLE"] == "storage" else http()
