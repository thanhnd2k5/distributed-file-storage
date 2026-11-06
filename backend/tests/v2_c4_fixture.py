"""Disposable C4 browser fixture; never imported by production entrypoints.

Owns a unique PostgreSQL schema, two temporary Storage ledgers, local HTTP ports,
and Nginx. Control/fault routes exist only in this test process.
"""

import asyncio
import hashlib
import json
import signal
import subprocess
import tempfile
import time
from contextlib import ExitStack, contextmanager
from dataclasses import replace
from pathlib import Path
from threading import Event, Lock, Thread

from fastapi import Request
from test_v2_c2 import cluster
from test_v2_c3 import serve
from test_v2_metadata import foundation, sql

from metadata.v2.api import create_app
from metadata.v2.http import JsonClient
from metadata.v2.worker import Worker, WorkerConfig
from transfer.v2.app import create_app as transfer_app


class Probe:
    def __init__(self):
        self.fault, self.requests, self.report = None, [], None
        self.lock = Lock()

    def wrap(self, app):
        probe = self

        async def middleware(scope, receive, send):
            if scope["type"] != "http":
                return await app(scope, receive, send)
            path, method = scope["path"], scope["method"]
            if "/_c4/" not in path:
                with probe.lock:
                    probe.requests.append(
                        dict(
                            path=path,
                            method=method,
                            at=time.monotonic(),
                            authorization=any(
                                key.lower() == b"authorization" for key, _ in scope["headers"]
                            ),
                        )
                    )
                    probe.requests = probe.requests[-1000:]
            kind = (
                "create"
                if method == "POST" and path == "/api/v2/uploads"
                else "part"
                if method == "PUT"
                else "complete"
                if path.endswith("/complete")
                else "abort"
                if path.endswith("/abort")
                else None
            )
            with probe.lock:
                fault = probe.fault if probe.fault in (f"after_{kind}", f"before_{kind}") else None
                if fault:
                    probe.fault = None
            if fault and fault.startswith("before_"):
                return await unavailable(scope, receive, send)
            if not fault:
                return await app(scope, receive, send)
            messages = []

            async def capture(message):
                messages.append(message)

            await app(scope, receive, capture)
            # The real handler already committed; drop its result and deliver
            # a deterministic proxy 503, not a simulated successful mutation.
            return await unavailable(scope, receive, send)

        return middleware


async def unavailable(scope, receive, send):
    from fastapi.responses import JSONResponse

    response = JSONResponse(
        {
            "error": {
                "code": "METADATA_UNAVAILABLE",
                "message": "C4 injected response loss",
                "retryable": True,
                "details": {},
            }
        },
        status_code=503,
        headers={"Retry-After": "1"},
    )
    await response(scope, receive, send)


def disk_evidence(repo, resources):
    result = []
    ledgers = {ledger.scope.node_id: ledger for ledger, _, _ in resources}
    for file in sql(
        repo, "SELECT file_id,state,expected_sha256,actual_sha256,size_bytes FROM files"
    ):
        entry = {k: str(v) if k == "file_id" else v for k, v in file.items()}
        if file["state"] == "AVAILABLE":
            digest, total, replicas = hashlib.sha256(), 0, []
            parts = sql(
                repo,
                "SELECT p.* FROM manifest_parts p JOIN files f USING(version_id) "
                "WHERE f.file_id=:id ORDER BY part_index",
                id=file["file_id"],
            )
            for part in parts:
                copies = sql(
                    repo,
                    "SELECT * FROM allocations WHERE chunk_id=:id AND state='STORED'",
                    id=part["chunk_id"],
                )
                assert len(copies) == 2
                assert len({c["failure_domain_snapshot"] for c in copies}) == 2
                for copy in copies:
                    path = ledgers[copy["node_id"]].root / "chunks" / str(copy["allocation_id"])
                    local, count = hashlib.sha256(), 0
                    with path.open("rb") as handle:
                        while block := handle.read(262144):
                            local.update(block)
                            count += len(block)
                            if copy == copies[0]:
                                digest.update(block)
                                total += len(block)
                    assert (count, local.hexdigest()) == (
                        part["size_bytes"],
                        part["checksum_sha256"],
                    )
                replicas.append(len(copies))
            assert total == file["size_bytes"]
            assert digest.hexdigest() == file["expected_sha256"] == file["actual_sha256"]
            entry.update(disk_sha256=digest.hexdigest(), disk_size=total, replicas=replicas)
        result.append(entry)
    return result


def main():
    stopping, probe = Event(), Probe()
    worker, thread = None, None
    with ExitStack() as stack:
        repo = stack.enter_context(contextmanager(foundation.__wrapped__)())
        directory = Path(stack.enter_context(tempfile.TemporaryDirectory()))
        _, resources = stack.enter_context(contextmanager(cluster.__wrapped__)(repo, directory))
        repo.policy = replace(repo.policy, chunk_size_bytes=65536)
        for ledger, _, _ in resources:
            ledger.chunk_limit = 65536
            from uuid import UUID

            repo.set_budget(ledger.scope.node_id, UUID(ledger.scope.node_incarnation), 64 * 1024**2)
        public_app = create_app(repo)

        @public_app.post("/api/v2/_c4/control")
        async def control(request: Request):
            nonlocal worker, thread
            data = await request.json()
            if "fault" in data:
                assert data["fault"] in (
                    None,
                    "before_part",
                    "after_create",
                    "after_part",
                    "after_complete",
                    "after_abort",
                )
                with probe.lock:
                    probe.fault = data["fault"]
            if data.get("worker") is False and worker:
                worker.stopping.set()
                await asyncio.to_thread(thread.join, 5)
                assert not thread.is_alive()
                worker, thread = None, None
            if data.get("worker") is True and worker is None:
                worker = Worker(repo, config=WorkerConfig(poll_seconds=0.02))
                thread = Thread(target=worker.run, daemon=True)
                thread.start()
                for _ in range(250):
                    if worker.ready():
                        break
                    await asyncio.sleep(0.02)
                assert worker.ready()
            if data.get("clear_audit"):
                with probe.lock:
                    probe.requests.clear()
            if data.get("finish"):
                stopping.set()
            return {"ok": True}

        @public_app.get("/api/v2/_c4/status")
        async def status():
            return {
                "requests": probe.requests,
                "report": probe.report,
                "files": disk_evidence(repo, resources),
                "allocations": len(sql(repo, "SELECT 1 FROM allocations")),
                "sessions": len(sql(repo, "SELECT 1 FROM upload_sessions")),
            }

        @public_app.post("/api/v2/_c4/report")
        async def report(request: Request):
            probe.report = await request.json()
            return {"ok": True}

        public = stack.enter_context(serve(probe.wrap(public_app)))
        internal = stack.enter_context(serve(create_app(repo, internal=True)))
        transfer = stack.enter_context(
            serve(probe.wrap(transfer_app(JsonClient(public, internal, repo.epoch))))
        )
        config = Path("/app/v2-ingress.conf").read_text()
        config = config.replace("api:8080", public.removeprefix("http://"))
        config = config.replace("transfer:8080", transfer.removeprefix("http://"))
        config = config.replace("listen 8080", "listen 18080")
        config = config.replace("/tmp/v2-nginx.pid", str(directory / "nginx.pid"))
        path = directory / "nginx.conf"
        path.write_text(config)
        process = subprocess.Popen(["nginx", "-c", str(path), "-g", "daemon off;"])
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda *_: stopping.set())
        print(json.dumps({"ready": True, "port": 18080, "schema": repo.schema}), flush=True)
        try:
            while not stopping.wait(0.25):
                assert process.poll() is None
        finally:
            if worker:
                worker.stopping.set()
                thread.join(5)
            process.terminate()
            process.wait(timeout=5)


if __name__ == "__main__":
    main()
