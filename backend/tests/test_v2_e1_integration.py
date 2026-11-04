"""Incoming HTTP and outgoing control saturation, with real TCP and PostgreSQL."""

import asyncio
from contextlib import ExitStack, asynccontextmanager
from threading import Event, Lock
from uuid import UUID, uuid4

import httpx
import pytest
from test_v2_c3 import serve
from test_v2_d2 import available
from test_v2_metadata import foundation as foundation
from test_v2_metadata import sql

from metadata.v2.api import create_app
from metadata.v2.http import JsonClient
from metadata.v2.repository import DomainError
from metadata.v2.runtime import Admission


@asynccontextmanager
async def control_peer(block_progress):
    tasks, paths, closed = set(), [], asyncio.Event()

    async def handle(reader, writer):
        task = asyncio.current_task()
        tasks.add(task)
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            path = header.split(b" ")[1].decode()
            # Both GET and POST carry an explicit bounded Content-Length.
            length = next(
                int(line.split(b":", 1)[1])
                for line in header.split(b"\r\n")
                if line.lower().startswith(b"content-length:")
            )
            await reader.readexactly(length)
            paths.append(path)
            if (path.endswith("/renew")) == block_progress:
                await reader.read()
                closed.set()
                return
            body = b'{"ok":true}'
            writer.write(f"HTTP/1.1 200 OK\r\nContent-Length: {len(body)}\r\n\r\n".encode() + body)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            tasks.discard(task)

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    try:
        yield f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}", paths, closed
    finally:
        server.close()
        await server.wait_closed()
        pending = list(tasks)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


@pytest.mark.parametrize("flood_progress", [False, True])
def test_control_flood_preserves_other_class_and_cancellation_reclaims_slot(flood_progress):
    async def exercise():
        async with control_peer(flood_progress) as (origin, paths, closed):
            client = JsonClient(origin, origin, uuid4(), timeout=2, max_calls=4, reserve=1)
            progress = ("POST", f"/internal/v2/downloads/{uuid4()}/renew")
            ordinary = ("GET", "/api/v2/config")
            flood, other = (progress, ordinary) if flood_progress else (ordinary, progress)
            tasks = [asyncio.create_task(client.call(*flood)) for _ in range(3)]
            try:
                async with asyncio.timeout(1):
                    while len(paths) != 3:
                        await asyncio.sleep(0.01)
                with pytest.raises(DomainError, match="RESOURCE_LIMIT"):
                    await client.call(*flood)
                assert len(paths) == 3  # Rejected before opening/sending another request.
                assert (await client.call(*other))["ok"]
                assert client.slots.active == 3
                tasks[0].cancel()
                with pytest.raises(asyncio.CancelledError):
                    await tasks[0]
                await asyncio.wait_for(closed.wait(), 0.5)
                replacement = asyncio.create_task(client.call(*flood))
                tasks.append(replacement)
                async with asyncio.timeout(1):
                    while len(paths) != 5:
                        await asyncio.sleep(0.01)
                assert client.slots.active == 3
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            assert client.slots.active == 0

    asyncio.run(exercise())


@pytest.mark.parametrize("flood_progress", [False, True])
def test_api_and_client_reservations_preserve_lease_progress_and_new_work(
    foundation, monkeypatch, flood_progress
):
    repo = foundation
    session = available(repo, b"")
    app = create_app(repo, internal=True)
    app.add_middleware(Admission, limit=4, reserve=1)
    with ExitStack() as stack:
        internal = stack.enter_context(serve(app))
        # Public origin is unused for these internal commands.
        client = JsonClient(internal, internal, repo.epoch, timeout=3, max_calls=4, reserve=1)

        async def exercise():
            acquire_path = "/internal/v2/downloads"
            initial = await client.call(
                "POST", acquire_path, {"file_id": str(session["file_id"])}, internal=True
            )
            renew_path = f"{acquire_path}/{initial['read_id']}/renew"
            operation = "renew_read" if flood_progress else "acquire_read"
            original = getattr(repo, operation)
            arrived, release, mutex = [], Event(), Lock()

            def held(*args, **kwargs):
                with mutex:
                    arrived.append(True)
                assert release.wait(5)
                return original(*args, **kwargs)

            monkeypatch.setattr(repo, operation, held)
            path = renew_path if flood_progress else acquire_path
            payload = (
                {"lease_token": initial["lease_token"]}
                if flood_progress
                else {"file_id": str(session["file_id"])}
            )
            tasks = [
                asyncio.create_task(client.call("POST", path, payload, internal=True))
                for _ in range(3)
            ]
            leases = {initial["read_id"]: initial}
            try:
                async with asyncio.timeout(2):
                    while len(arrived) != 3:
                        await asyncio.sleep(0.01)
                with pytest.raises(DomainError, match="RESOURCE_LIMIT"):
                    await client.call("POST", path, payload, internal=True)
                async with httpx.AsyncClient(timeout=2) as raw:
                    # Bypass client admission to independently exercise API admission.
                    denied = await raw.post(
                        internal + path, json=dict(payload, recovery_epoch=str(repo.epoch))
                    )
                    assert denied.status_code == 429 and denied.headers["Retry-After"] == "1"
                    assert len(denied.headers.get_list("X-Request-ID")) == 1
                    assert (
                        str(UUID(denied.headers["X-Request-ID"])) == denied.headers["X-Request-ID"]
                    )
                    assert len(arrived) == 3
                    if flood_progress:
                        new = await client.call(
                            "POST",
                            acquire_path,
                            {"file_id": str(session["file_id"])},
                            internal=True,
                        )
                        leases[new["read_id"]] = new
                    else:
                        invalid = await raw.post(
                            internal + renew_path,
                            json=dict(
                                lease_token=initial["lease_token"], recovery_epoch=str(uuid4())
                            ),
                        )
                        assert invalid.status_code == 503
                        assert invalid.json()["error"]["code"] == "RECOVERY_REQUIRED"
                        renewed = await client.call(
                            "POST",
                            renew_path,
                            {"lease_token": initial["lease_token"]},
                            internal=True,
                        )
                        assert renewed["expires_at"] != initial["expires_at"]
                        assert (
                            await client.call(
                                "POST",
                                f"{acquire_path}/{initial['read_id']}/release",
                                {"lease_token": initial["lease_token"]},
                                internal=True,
                            )
                            is None
                        )
            finally:
                release.set()
                replies = await asyncio.gather(*tasks, return_exceptions=True)
            for reply in replies:
                assert isinstance(reply, dict), reply
                leases[reply["read_id"]] = reply
            for lease in leases.values():
                await client.call(
                    "POST",
                    f"{acquire_path}/{lease['read_id']}/release",
                    {"lease_token": lease["lease_token"]},
                    internal=True,
                )
            assert client.slots.active == 0
            assert not sql(repo, "SELECT 1 FROM download_leases WHERE state='ACTIVE'")
            assert sql(repo, "SELECT state FROM files")[0]["state"] == "AVAILABLE"

        asyncio.run(exercise())
