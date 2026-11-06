"""Pre-E regressions: typed configuration, request tracing and control I/O budgets."""

import asyncio
import json
import ssl
import time
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI, Request

from metadata.v2.http import JsonClient, handlers
from metadata.v2.repository import DomainError
from metadata.v2.runtime import Admission, Limits, worker_config


@pytest.mark.parametrize(
    "field",
    [
        "request_seconds",
        "control_seconds",
        "renew_seconds",
        "shutdown_seconds",
        "download_rpc_seconds",
        "download_seconds",
        "download_renew_seconds",
    ],
)
def test_fractional_limits_environment(monkeypatch, field):
    monkeypatch.setenv("V2_" + field.upper(), "0.5")
    assert getattr(Limits.from_env(), field) == 0.5


@pytest.mark.parametrize(
    "field", ["poll_seconds", "renew_seconds", "job_seconds", "shutdown_seconds"]
)
def test_fractional_worker_environment(monkeypatch, field):
    monkeypatch.setenv("V2_WORKER_" + field.upper(), "0.5")
    assert getattr(worker_config(), field) == 0.5


@pytest.mark.parametrize("value", ["nan", "inf", "0", "-1", "bad"])
def test_invalid_duration_environment_still_fails(monkeypatch, value):
    monkeypatch.setenv("V2_DOWNLOAD_RPC_SECONDS", value)
    with pytest.raises(ValueError):
        Limits.from_env()
    monkeypatch.setenv("V2_WORKER_JOB_SECONDS", value)
    with pytest.raises(ValueError):
        worker_config()


def test_counts_still_require_integers(monkeypatch):
    monkeypatch.setenv("V2_DOWNLOAD_STREAMS", "1.5")
    with pytest.raises(ValueError):
        Limits.from_env()


@pytest.mark.parametrize("mode,status", [("requests", 429), ("health", 429), ("closing", 503)])
def test_trace_early_admission_and_head(mode, status):
    async def exercise():
        app = FastAPI()
        handlers(app)
        state = SimpleNamespace(closing=False)
        app.add_middleware(Admission, limit=1)
        app.add_middleware(Admission, limit=1, state=state)
        entered, release = 0, asyncio.Event()

        @app.api_route("/work", methods=["GET", "HEAD"])
        @app.api_route("/health/ready", methods=["GET", "HEAD"])
        async def work(request: Request):
            nonlocal entered
            entered += 1
            await release.wait()
            return {"request_id": request.scope["request_id"]}

        async def invoke(method, path):
            messages = []

            async def receive():
                await asyncio.Future()

            async def send(message):
                messages.append(message)

            await app(
                dict(
                    type="http",
                    asgi={"version": "3.0"},
                    http_version="1.1",
                    method=method,
                    scheme="http",
                    path=path,
                    query_string=b"",
                    headers=[],
                ),
                receive,
                send,
            )
            start = next(m for m in messages if m["type"] == "http.response.start")
            ids = [value.decode() for key, value in start["headers"] if key == b"x-request-id"]
            assert len(ids) == 1 and str(UUID(ids[0])) == ids[0]
            body = b"".join(
                m.get("body", b"") for m in messages if m["type"] == "http.response.body"
            )
            return start["status"], ids[0], body

        path = "/health/ready" if mode == "health" else "/work"
        tasks = []
        try:
            if mode == "closing":
                state.closing = True
            else:
                tasks = [
                    asyncio.create_task(invoke("GET", path))
                    for _ in range(2 if mode == "health" else 1)
                ]
                async with asyncio.timeout(2):
                    while entered != len(tasks):
                        await asyncio.sleep(0)
            for method in ("GET", "HEAD"):
                code, _, body = await asyncio.wait_for(invoke(method, path), 2)
                assert code == status
                if method == "HEAD":
                    assert body == b""
                else:
                    assert json.loads(body)["error"]["code"] in (
                        "RESOURCE_LIMIT",
                        "METADATA_UNAVAILABLE",
                    )
        finally:
            release.set()
            for task in tasks:
                code, request_id, body = await asyncio.wait_for(task, 2)
                assert code == 200 and json.loads(body)["request_id"] == request_id

    asyncio.run(exercise())


@asynccontextmanager
async def peer(*, tls=None):
    tasks, mutations, seen, closed = set(), [], asyncio.Event(), asyncio.Event()

    async def connected(reader, writer):
        task = asyncio.current_task()
        tasks.add(task)
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            method, path, _ = header.split(b"\r\n", 1)[0].decode().split(" ")
            fields = {
                k.lower(): v.strip()
                for k, v in (line.decode().split(":", 1) for line in header.split(b"\r\n")[1:-2])
            }
            body = await reader.readexactly(int(fields.get("content-length", 0)))
            if method == "POST":
                mutations.append(json.loads(body))
            if path in ("/stall", "/mutation"):
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{")
                await writer.drain()
                seen.set()
                assert await reader.read() == b""
                closed.set()
                return
            if path in ("/headers", "/body"):
                data = (
                    b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\n\r\n"
                    if path == "/headers"
                    else b'{"ready":true}'
                )
                if path == "/body":
                    writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 14\r\n\r\n")
                seen.set()
                for byte in data:
                    writer.write(bytes([byte]))
                    await writer.drain()
                    await asyncio.sleep(0.04)
                return
            if path == "/large":
                data = b" " * (1024 * 1024 + 1)
                writer.write(
                    f"HTTP/1.1 200 OK\r\nContent-Length: {len(data)}\r\n\r\n".encode() + data
                )
            elif path == "/truncated":
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 99\r\n\r\n{")
            elif path == "/empty":
                writer.write(b"HTTP/1.1 204 No Content\r\n\r\n")
            elif path == "/error":
                data = b'{"error":{"code":"RECOVERY_REQUIRED"}}'
                writer.write(
                    f"HTTP/1.1 503 Unavailable\r\nContent-Length: {len(data)}\r\n\r\n".encode()
                    + data
                )
            else:
                data = json.dumps(
                    dict(
                        ready=True,
                        epoch=fields.get("x-recovery-epoch"),
                        token=fields.get("x-read-lease-token"),
                    )
                ).encode()
                writer.write(
                    b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\n"
                    b"Transfer-Encoding: chunked\r\n\r\n"
                )
                for block in (data[:1], data[1:]):
                    writer.write(f"{len(block):x}\r\n".encode() + block + b"\r\n")
                writer.write(b"0\r\n\r\n")
            await writer.drain()
        except (OSError, asyncio.IncompleteReadError):
            closed.set()
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
            tasks.discard(task)

    server = await asyncio.start_server(connected, "127.0.0.1", 0, ssl=tls)
    try:
        yield SimpleNamespace(
            origin=f"{'https' if tls else 'http'}://127.0.0.1:{server.sockets[0].getsockname()[1]}",
            mutations=mutations,
            seen=seen,
            closed=closed,
        )
    finally:
        server.close()
        await server.wait_closed()
        for task in list(tasks):
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("path", ["/headers", "/body", "/stall"])
def test_total_control_deadline_and_slot_recovery(path):
    async def exercise():
        async with peer() as server:
            client = JsonClient(server.origin, server.origin, uuid4(), timeout=0.15, max_calls=1)
            started = time.monotonic()
            with pytest.raises(DomainError, match="METADATA_UNAVAILABLE"):
                await client.call("GET", path)
            assert 0.1 <= time.monotonic() - started < 0.7
            if path == "/stall":
                await asyncio.wait_for(server.closed.wait(), 0.5)
            assert (await client.call("GET", "/ok"))["ready"]

    asyncio.run(exercise())


def test_cancel_closes_socket_and_does_not_replay_mutation():
    async def exercise():
        async with peer() as server:
            epoch = uuid4()
            client = JsonClient(server.origin, server.origin, epoch, timeout=2, max_calls=1)
            task = asyncio.create_task(
                client.call("POST", "/mutation", {"file_id": "example"}, internal=True)
            )
            await asyncio.wait_for(server.seen.wait(), 1)
            with pytest.raises(DomainError, match="RESOURCE_LIMIT"):
                await client.call("GET", "/ok")
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await asyncio.wait_for(server.closed.wait(), 0.5)
            result = await client.call(
                "GET", "/ok", internal=True, headers={"X-Read-Lease-Token": "read-token"}
            )
            assert result == dict(ready=True, epoch=str(epoch), token="read-token")
            assert server.mutations == [{"file_id": "example", "recovery_epoch": str(epoch)}]

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "path,code",
    [
        ("/large", "METADATA_UNAVAILABLE"),
        ("/truncated", "METADATA_UNAVAILABLE"),
        ("/error", "RECOVERY_REQUIRED"),
    ],
)
def test_control_errors_and_size_bound_return_slot(path, code):
    async def exercise():
        async with peer() as server:
            client = JsonClient(server.origin, server.origin, uuid4(), timeout=1, max_calls=1)
            with pytest.raises(DomainError, match=code):
                await client.call("GET", path)
            assert await client.call("POST", "/empty", {}, internal=True) is None
            assert (await client.call("GET", "/ok"))["ready"]

    asyncio.run(exercise())


def test_https_verifies_peer_and_supports_trusted_connection(monkeypatch):
    async def exercise():
        directory = Path(__file__).parent / "fixtures"
        certificate = directory / "control-test-cert.pem"
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(certificate, directory / "control-test-key.pem")
        original = ssl.create_default_context
        async with peer(tls=context) as server:
            client = JsonClient(server.origin, server.origin, uuid4(), timeout=2, max_calls=1)
            with pytest.raises(DomainError, match="METADATA_UNAVAILABLE"):
                await client.call("GET", "/ok")
            monkeypatch.setattr(ssl, "create_default_context", lambda: original(cafile=certificate))
            assert (await client.call("GET", "/ok"))["ready"]

    asyncio.run(exercise())


def test_tls_handshake_is_inside_control_deadline():
    async def exercise():
        async with peer() as server:
            origin = server.origin.replace("http:", "https:")
            client = JsonClient(origin, origin, uuid4(), timeout=0.15, max_calls=1)
            started = time.monotonic()
            with pytest.raises(DomainError, match="METADATA_UNAVAILABLE"):
                await client.call("GET", "/ok")
            assert time.monotonic() - started < 0.7
            await asyncio.wait_for(server.closed.wait(), 0.5)

    asyncio.run(exercise())
