"""Test-only probes/faults around the real runtime. Never a deployment entrypoint."""

import asyncio
import json
import os
import time
from pathlib import Path
from uuid import uuid4

import uvicorn

from metadata.v2.runtime import app_factory


def main():
    root = Path(os.environ["V2_C5_CONTROL"])
    role, label = os.environ["V2_SERVICE_ROLE"], os.environ["V2_C5_LABEL"]
    app = app_factory()
    stats = {
        "queues": 0,
        "queue_high_water": 0,
        "blocked_puts": 0,
        "max_streams": 0,
        "queue_limit": 2,
        "block_bytes": 262144,
    }
    if role == "transfer":
        import transfer.v2.app as transfer_module

        class Queue(asyncio.Queue):
            def __init__(self, maxsize=0):
                super().__init__(maxsize)
                stats["queues"] += 1

            async def put(self, item):
                stats["blocked_puts"] += self.full()
                await super().put(item)
                stats["queue_high_water"] = max(stats["queue_high_water"], self.qsize())
                stats["max_streams"] = max(stats["max_streams"], len(app.state.transfer.active))

        class AsyncioProbe:
            def __getattr__(self, name):
                return Queue if name == "Queue" else getattr(asyncio, name)

        transfer_module.asyncio = AsyncioProbe()
        original_call = app.state.transfer.api.call

        async def call(method, path, *args, **kwargs):
            if path.endswith("/confirm") and (root / "hold-confirm").exists():
                (root / "transfer-event.json").write_text(
                    json.dumps(
                        {
                            "pid": os.getpid(),
                            "label": label,
                            "path": path,
                        }
                    )
                )
                while (root / "hold-confirm").exists():
                    await asyncio.sleep(0.02)
            return await original_call(method, path, *args, **kwargs)

        app.state.transfer.api.call = call
    if role == "worker":

        def hook(point):
            with (root / "worker-events.jsonl").open("a") as log:
                log.write(
                    json.dumps(
                        {
                            "point": point,
                            "pid": os.getpid(),
                            "at": time.time(),
                            "phase": (root / "phase").read_text(),
                        }
                    )
                    + "\n"
                )
            if (root / "hold-worker").read_text() == point:
                (root / "worker-event.json").write_text(
                    json.dumps(
                        {
                            "point": point,
                            "pid": os.getpid(),
                            "at": time.time(),
                        }
                    )
                )
                while (root / "hold-worker").read_text() == point:
                    time.sleep(0.02)

        app.state.worker.hook = hook

    @app.get("/_c5/metrics")
    async def metrics():
        return stats

    class Fault:
        async def __call__(self, scope, receive, send):
            if scope["type"] != "http":
                return await app(scope, receive, send)
            path, method = scope["path"], scope["method"]
            kind = (
                "create"
                if method == "POST" and path == "/api/v2/uploads"
                else "part"
                if method == "PUT"
                else "complete"
                if path.endswith("/complete")
                else None
            )
            flag = root / "lose-response"
            capture = False
            try:
                if flag.read_text() == kind:
                    flag.rename(root / f"consumed-{uuid4()}")
                    capture = True
            except FileNotFoundError:
                pass
            if not capture:
                return await app(scope, receive, send)
            messages = []

            async def collect(message):
                messages.append(message)

            await app(scope, receive, collect)
            if messages[0]["status"] < 300:
                from metadata.v2.http import error_response

                (root / f"lost-{kind}-{label}.json").write_text(
                    json.dumps(
                        {
                            "path": path,
                            "status": messages[0]["status"],
                            "pid": os.getpid(),
                        }
                    )
                )
                return await error_response("METADATA_UNAVAILABLE")(scope, receive, send)
            for message in messages:
                await send(message)

    uvicorn.run(Fault(), host="127.0.0.1", port=int(os.environ["V2_PORT"]), log_level="warning")


if __name__ == "__main__":
    main()
