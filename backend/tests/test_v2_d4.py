"""D4: real HTTP/PostgreSQL/gRPC/filesystem, plus deterministic ASGI backpressure."""

import asyncio
import hashlib
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from dataclasses import replace
from threading import Event
from uuid import UUID, uuid4

import grpc
import httpx
import pytest
import v2_storage_pb2 as pb
import v2_storage_pb2_grpc as rpc
from test_v2_c2 import cluster as cluster
from test_v2_c2 import due, run_job
from test_v2_c3 import create, error, put, until
from test_v2_c3 import http_cluster as http_cluster
from test_v2_d3 import ingress
from test_v2_metadata import foundation as foundation
from test_v2_metadata import sql

from metadata.v2.repository import DomainError
from metadata.v2.runtime import Limits
from metadata.v2.worker import Worker
from storage.v2.service import StorageService
from storage.v2.streaming import StreamingStore
from transfer.v2.app import create_app as transfer_app


def published(env, data=b"abcdefg"):
    repo, _, public, _, transfer, _, _ = env
    session = create(public, data, original_name='tệp "x";.bin')
    chunk = session["chunk_size_bytes"]
    for i in range(session["part_count"]):
        assert (
            put(transfer, session["upload_id"], i, data[i * chunk : (i + 1) * chunk]).status_code
            == 200
        )
    assert (
        httpx.post(public + f"/api/v2/uploads/{session['upload_id']}/complete", json={}).status_code
        == 202
    )
    worker = Worker(repo)
    try:
        assert run_job(worker, "FINALIZE")["state"] == "COMPLETED"
    finally:
        worker.close()
    return session, f"/api/v2/files/{session['file_id']}/download"


def released(repo):
    until(lambda: not sql(repo, "SELECT 1 FROM download_leases WHERE state='ACTIVE'"))


@contextmanager
def readers(env, modes, *, part=None):
    """Replace registry read endpoints only; proxies read the real ledger/bytes."""
    repo, resources, *_ = env
    servers, calls, drained = [], [], []
    target = (
        None
        if part is None
        else str(
            sql(repo, "SELECT chunk_id FROM manifest_parts WHERE part_index=:i", i=part)[0][
                "chunk_id"
            ]
        )
    )

    class Reader(rpc.StorageServiceServicer):
        def __init__(self, ledger, mode):
            self.real = StorageService(StreamingStore(ledger), "test")
            self.mode = mode

        def GetChunk(self, request, context):
            calls.append((self.real.ledger.scope.node_id, request.expected_chunk.chunk_id))
            mode = (
                self.mode if target is None or target == request.expected_chunk.chunk_id else "ok"
            )
            try:
                if mode == "slow":
                    while context.is_active():
                        time.sleep(0.01)
                    return
                for frame in self.real.GetChunk(request, context):
                    kind = frame.WhichOneof("payload")
                    if kind == "header":
                        if mode == "identity":
                            frame.header.key.allocation_id = str(uuid4())
                        elif mode == "spec":
                            frame.header.chunk.checksum_sha256 = "a" * 64
                        elif mode == "generation":
                            frame.header.generation += 1
                        elif mode == "duplicate_header":
                            yield frame
                    elif kind == "block":
                        if mode == "corrupt":
                            frame.block.data = b"x" * len(frame.block.data)
                        elif mode == "offset":
                            frame.block.offset += 1
                        elif mode == "empty_block":
                            frame.block.data = b""
                        elif mode == "extra_bytes":
                            frame.block.data += b"!"
                    elif kind == "footer":
                        if mode == "missing_footer":
                            continue
                        if mode == "footer":
                            frame.footer.actual_size_bytes += 1
                    yield frame
                if mode == "trailing":
                    yield pb.GetChunkFrame(block=pb.DataBlock(offset=0, data=b"!"))
                elif mode == "eof_error":
                    context.abort(grpc.StatusCode.UNAVAILABLE, "failure after footer")
                elif mode == "missing_eof":
                    while context.is_active():
                        time.sleep(0.01)
            finally:
                drained.append(self.real.ledger.scope.node_id)

    try:
        for (ledger, _, _), mode in zip(resources, modes):
            executor = ThreadPoolExecutor(4)
            server = grpc.server(executor)
            rpc.add_StorageServiceServicer_to_server(Reader(ledger, mode), server)
            port = server.add_insecure_port("127.0.0.1:0")
            server.start()
            servers.append((server, executor))
            sql(
                repo,
                "UPDATE storage_nodes SET port=:port WHERE node_id=:node",
                port=port,
                node=ledger.scope.node_id,
            )
        yield calls, drained
    finally:
        for server, executor in servers:
            server.stop(0).wait()
            executor.shutdown(wait=True)


@pytest.mark.parametrize(
    "data,headers,status,body,content_range",
    [
        (b"", {}, 200, b"", None),
        (b"abc", {}, 200, b"abc", None),
        (b"abcdefg", {}, 200, b"abcdefg", None),
        (b"abcdefg", {"Range": "bytes=1-4"}, 206, b"bcde", "bytes 1-4/7"),
        (b"abcdefg", {"Range": "bytes=3-"}, 206, b"defg", "bytes 3-6/7"),
        (b"abcdefg", {"Range": "bytes=-2"}, 206, b"fg", "bytes 5-6/7"),
        (b"abcdefg", {"Range": "bytes=2-99999"}, 206, b"cdefg", "bytes 2-6/7"),
        (b"abcdefg", {"Range": "bytes=0-6"}, 206, b"abcdefg", "bytes 0-6/7"),
        (b"abcdefg", {"Range": "bytes=0-1,4-5"}, 200, b"abcdefg", None),
        (b"abcdefg", {"Range": "items=0-1"}, 200, b"abcdefg", None),
        (b"abcdefg", {"Range": "bytes=0-1", "If-Range": 'W/"other"'}, 200, b"abcdefg", None),
        (b"", {"Range": "bytes=-4"}, 200, b"", None),
    ],
)
def test_get_and_range_real_bytes(http_cluster, data, headers, status, body, content_range):
    repo, _, _, _, transfer, app, _ = http_cluster
    session, path = published(http_cluster, data)
    with readers(http_cluster, ["ok", "ok"]) as (calls, _):
        response = httpx.get(transfer + path, headers=headers)
        assert response.status_code == status, response.text
        assert response.content == body
        assert response.headers["Content-Length"] == str(len(body))
        assert response.headers.get("Content-Range") == content_range
        assert response.headers["ETag"] == '"sha256-' + hashlib.sha256(data).hexdigest() + '"'
        assert response.headers["Accept-Ranges"] == "bytes"
        assert response.headers["Content-Type"] == "application/octet-stream"
        assert "%E1%BB%87" in response.headers["Content-Disposition"]
        assert "Content-Encoding" not in response.headers
        assert response.headers["X-Request-ID"]
        if headers.get("Range") == "bytes=-2":
            assert len(calls) == 2  # Only intersecting chunks, not the prefix.
        if not data:
            assert not calls
    released(repo)
    leases = sql(repo, "SELECT file_id,version_id,state FROM download_leases")
    assert len(leases) == 1 and leases[0]["version_id"] == UUID(session["version_id"])
    assert not app.state.transfer.downloads.active


@pytest.mark.parametrize(
    "headers,status,code",
    [
        ({"Range": "bytes=9-"}, 416, "RANGE_NOT_SATISFIABLE"),
        ({"Range": "bytes=-0"}, 416, "RANGE_NOT_SATISFIABLE"),
        ({"Range": "bytes=4-1"}, 400, "INVALID_RANGE"),
        ({"If-Match": '"other"', "Range": "bad"}, 412, "PRECONDITION_FAILED"),
        ({"If-None-Match": "*", "Range": "bad"}, 304, None),
        ({"If-Match": "bad"}, 400, "INVALID_REQUEST"),
    ],
)
def test_unsuccessful_selection_has_no_lease_or_storage(http_cluster, headers, status, code):
    repo, _, _, _, transfer, _, _ = http_cluster
    _, path = published(http_cluster)
    with readers(http_cluster, ["ok", "ok"]) as (calls, _):
        response = httpx.get(transfer + path, headers=headers)
        assert response.status_code == status, response.text
        if code:
            assert response.json()["error"]["code"] == code
        else:
            assert response.content == b"" and "ETag" in response.headers
        if status == 416:
            assert response.headers["Content-Range"] == "bytes */7"
        assert not calls and not sql(repo, "SELECT * FROM download_leases")


@pytest.mark.parametrize(
    "mode",
    [
        "identity",
        "spec",
        "generation",
        "duplicate_header",
        "corrupt",
        "offset",
        "empty_block",
        "extra_bytes",
        "missing_footer",
        "footer",
        "trailing",
        "eof_error",
        "missing_eof",
        "slow",
    ],
)
def test_invalid_replica_falls_back_before_bytes(http_cluster, mode):
    repo, _, _, _, transfer, app, _ = http_cluster
    _, path = published(http_cluster, b"abc")
    app.state.transfer.downloads.limits = replace(
        app.state.transfer.limits, download_rpc_seconds=0.15
    )
    with readers(http_cluster, [mode, "ok"]) as (calls, drained):
        response = httpx.get(transfer + path)
        assert response.status_code == 200 and response.content == b"abc"
        assert [node for node, _ in calls] == ["node-0", "node-1"]
        until(lambda: len(drained) == 2)
    released(repo)


@pytest.mark.parametrize("part", [0, 1])
def test_exhausted_replicas_before_and_after_headers(http_cluster, part):
    repo, _, _, _, transfer, _, _ = http_cluster
    _, path = published(http_cluster)
    with readers(http_cluster, ["corrupt", "missing_footer"], part=part) as (calls, _):
        if part == 0:
            response = httpx.get(transfer + path)
            error(response, 503, "CHUNK_UNAVAILABLE")
            assert response.headers["Content-Type"] == "application/json"
            assert "node-" not in response.text and "lease_token" not in response.text
        else:
            received = bytearray()
            with httpx.stream("GET", transfer + path) as response:
                assert response.status_code == 200
                assert response.headers["Content-Length"] == "7"
                with pytest.raises(httpx.RemoteProtocolError):
                    for block in response.iter_raw():
                        received.extend(block)
            assert received == b"abc"  # No unverified second chunk, JSON or padding.
        assert len(calls) == part + 2
    released(repo)


def test_disk_corruption_marks_ledger_and_uses_other_replica(http_cluster):
    repo, resources, _, _, transfer, _, _ = http_cluster
    _, path = published(http_cluster, b"abc")
    ledger = resources[0][0]
    record = ledger.inventory(ledger.scope)[0][0]
    (ledger.root / "chunks" / record["allocation_id"]).write_bytes(b"bad")
    response = httpx.get(transfer + path)
    assert response.status_code == 200 and response.content == b"abc"
    assert ledger.lookup(record["allocation_id"])["state"] == "CORRUPT"
    assert resources[1][0].inventory(resources[1][0].scope)[0][0]["state"] == "STORED"
    released(repo)


def test_delete_between_resolve_and_acquire_wins_without_binary_headers(http_cluster):
    repo, _, _, _, transfer, _, api = http_cluster
    session, path = published(http_cluster)
    original = api.call

    async def delete_first(method, path, *args, **kwargs):
        if path == "/internal/v2/downloads":
            await asyncio.to_thread(repo.delete_file, UUID(session["file_id"]))
        return await original(method, path, *args, **kwargs)

    api.call = delete_first
    error(httpx.get(transfer + path), 404, "FILE_NOT_FOUND")
    assert not sql(repo, "SELECT * FROM download_leases")


def test_client_disconnect_cancels_grpc_and_releases_admission(http_cluster):
    repo, _, _, _, transfer, app, _ = http_cluster
    _, path = published(http_cluster)
    with readers(http_cluster, ["slow", "ok"]) as (calls, drained):
        parsed = httpx.URL(transfer)
        with socket.create_connection((parsed.host, parsed.port), timeout=3) as connection:
            connection.sendall(f"GET {path} HTTP/1.1\r\nHost: local\r\n\r\n".encode())
            until(lambda: bool(calls))
        until(lambda: bool(drained))
        until(lambda: not app.state.transfer.downloads.active)
        assert len(calls) == 1  # Cancellation does not initiate fallback.
    released(repo)


async def asgi_download(app, path, send, queue=None):
    queue = queue or asyncio.Queue()
    await queue.put({"type": "http.request", "body": b"", "more_body": False})
    scope = dict(
        type="http",
        asgi={"version": "3.0"},
        http_version="1.1",
        method="GET",
        scheme="http",
        path=path,
        raw_path=path.encode(),
        query_string=b"",
        headers=[],
        client=("127.0.0.1", 1),
        server=("local", 80),
    )
    await app(scope, queue.get, send)


@pytest.mark.parametrize(
    "failure", ["renew_error", "renew_stall", "expiry", "total_timeout", "shutdown"]
)
def test_blocked_send_stops_and_drains_on_lease_or_request_end(http_cluster, failure):
    repo, _, _, _, _, app, api = http_cluster
    _, path = published(http_cluster)
    downloads = app.state.transfer.downloads
    downloads.limits = replace(
        downloads.limits,
        download_renew_seconds=0.1,
        download_seconds=0.3 if failure == "total_timeout" else 3,
    )
    original = api.call

    async def short_lease(method, path, *args, **kwargs):
        if path.endswith("/renew"):
            if failure == "renew_error":
                raise DomainError("STALE_READ_LEASE")
            if failure == "renew_stall":
                await asyncio.sleep(10)
        result = await original(method, path, *args, **kwargs)
        if path == "/internal/v2/downloads" and failure in ("expiry", "renew_stall"):
            # Genuine DB lease has a shorter TTL; no host wall-clock assumption.
            sql(repo, "UPDATE download_leases SET expires_at=created_at+interval '0.5 seconds'")
            result["expires_at"] = sql(repo, "SELECT expires_at FROM download_leases")[0][
                "expires_at"
            ].isoformat()
        return result

    api.call = short_lease
    if failure == "expiry":
        downloads.limits = replace(downloads.limits, download_renew_seconds=1)

    async def exercise(calls):
        blocked = asyncio.Event()
        cancelled = asyncio.Event()
        messages = []

        async def send(message):
            messages.append(dict(message, body=bytes(message.get("body", b""))))
            if message["type"] == "http.response.body" and message.get("more_body"):
                blocked.set()
                try:
                    await asyncio.Future()
                finally:
                    cancelled.set()

        task = asyncio.create_task(asgi_download(app, path, send))
        await asyncio.wait_for(blocked.wait(), 2)
        assert len(calls) == 1  # No next-chunk prefetch while send is blocked.
        if failure == "shutdown":
            await app.state.transfer.close()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(RuntimeError, match="Download interrupted"):
                await asyncio.wait_for(task, 2)
        assert cancelled.is_set()
        assert [m["status"] for m in messages if m["type"] == "http.response.start"] == [200]
        assert len([m for m in messages if m["type"] == "http.response.body"]) == 1
        assert not downloads.active

    with readers(http_cluster, ["ok", "ok"]) as (calls, _):
        asyncio.run(exercise(calls))
    released(repo)


def test_slow_send_renews_after_tombstone_then_cleanup(http_cluster):
    repo, resources, _, _, _, app, api = http_cluster
    session, path = published(http_cluster)
    downloads = app.state.transfer.downloads
    downloads.limits = replace(downloads.limits, download_renew_seconds=0.1)
    original = api.call
    renewals = Event()

    async def observe(method, path, *args, **kwargs):
        value = await original(method, path, *args, **kwargs)
        if path.endswith("/renew"):
            renewals.set()
        return value

    api.call = observe

    async def exercise():
        bodies = []
        deleted = False

        async def send(message):
            nonlocal deleted
            if message["type"] == "http.response.body":
                bodies.append(bytes(message["body"]))
                if not deleted:
                    deleted = True
                    await asyncio.to_thread(repo.delete_file, UUID(session["file_id"]))
                    worker = Worker(repo)
                    try:
                        await asyncio.to_thread(run_job, worker, "CLEANUP")
                    finally:
                        worker.close()
                    assert (
                        sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 14
                    )
                    for ledger, _, _ in resources:
                        assert list((ledger.root / "chunks").iterdir())
                    assert await asyncio.to_thread(renewals.wait, 2)

        await asyncio.wait_for(asgi_download(app, path, send), 5)
        assert b"".join(bodies) == b"abcdefg"

    asyncio.run(exercise())
    released(repo)
    worker = Worker(repo)
    try:
        due(repo)
        run_job(worker, "CLEANUP")
    finally:
        worker.close()
    assert sql(repo, "SELECT state FROM files")[0]["state"] == "DELETED"
    assert sql(repo, "SELECT state FROM jobs WHERE kind='CLEANUP'")[0]["state"] == "DONE"
    assert sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 0
    for ledger, _, _ in resources:
        assert not list((ledger.root / "chunks").iterdir())
        assert all(r["cleanup_complete"] for r in ledger.inventory(ledger.scope)[0])


def test_download_admission_precedes_acquire_or_storage(http_cluster):
    repo, _, _, _, transfer, app, _ = http_cluster
    _, path = published(http_cluster)
    parsed = httpx.URL(transfer)
    with readers(http_cluster, ["slow", "ok"]) as (calls, drained):
        with ExitStack() as sockets:
            for i in range(4):
                connection = sockets.enter_context(
                    socket.create_connection((parsed.host, parsed.port), timeout=3)
                )
                connection.sendall(f"GET {path} HTTP/1.1\r\nHost: local\r\n\r\n".encode())
                until(lambda: len(calls) == i + 1)
            error(httpx.get(transfer + path), 429, "RESOURCE_LIMIT")
            assert len(calls) == 4 and len(sql(repo, "SELECT * FROM download_leases")) == 4
        until(lambda: len(drained) == 4)
        until(lambda: not app.state.transfer.downloads.active)
    released(repo)


@pytest.mark.parametrize(
    "values",
    [
        dict(download_buffered_bytes=1),
        dict(download_renew_seconds=11),
        dict(download_rpc_seconds=121),
        dict(download_seconds=86401),
        dict(download_seconds=float("nan")),
        dict(download_streams=33),
    ],
)
def test_invalid_download_budgets(values):
    with pytest.raises(ValueError):
        Limits(**values)


def test_ingress_get_range_and_if_range(http_cluster, tmp_path):
    repo, _, public, _, transfer, _, _ = http_cluster
    _, path = published(http_cluster)
    with ingress(public, transfer, tmp_path) as origin:
        etag = httpx.head(origin + path).headers["ETag"]
        response = httpx.get(origin + path, headers={"Range": "bytes=2-4", "If-Range": etag})
        assert response.status_code == 206 and response.content == b"cde"
        response = httpx.get(origin + path, headers={"Range": "bad", "If-Range": '"different"'})
        assert response.status_code == 200 and response.content == b"abcdefg"
    released(repo)


def test_readiness_rejects_chunk_policy_larger_than_reservation(http_cluster):
    _, _, _, _, _, _, api = http_cluster
    app = transfer_app(api, limits=Limits(download_chunk_bytes=2))

    async def startup():
        with pytest.raises(ValueError, match="Chunk policy exceeds"):
            async with app.router.lifespan_context(app):
                pytest.fail("Readiness must reject the incompatible chunk reservation")

    asyncio.run(startup())


def test_multiple_wire_blocks_and_http_slices_reuse_one_buffer(http_cluster):
    repo, resources, _, _, transfer, app, _ = http_cluster
    chunk = 262147
    repo.policy = replace(repo.policy, chunk_size_bytes=chunk)
    sql(repo, "UPDATE node_budgets SET allocatable_bytes=2000000")
    for ledger, _, _ in resources:
        ledger.chunk_limit = chunk
    data = bytes(range(256)) * 2049  # Two multi-block chunks and a small tail.
    _, path = published(http_cluster, data)
    downloads = app.state.transfer.downloads
    original = downloads.read_chunk
    buffers = []

    async def observe(part, buffer):
        buffers.append((id(buffer), len(buffer)))
        await original(part, buffer)

    downloads.read_chunk = observe
    response = httpx.get(transfer + path)
    assert response.content == data and response.status_code == 200
    assert len(buffers) == 3 and len(set(buffers)) == 1
    released(repo)
    response = httpx.get(transfer + path, headers={"Range": "bytes=262143-262149"})
    assert response.status_code == 206 and response.content == data[262143:262150]
    released(repo)


@pytest.mark.parametrize("failure", ["renew", "roundtrip", "release"])
def test_control_failures_before_headers_and_best_effort_release(http_cluster, failure):
    repo, _, _, _, transfer, app, api = http_cluster
    _, path = published(http_cluster, b"abc")
    downloads = app.state.transfer.downloads
    downloads.limits = replace(downloads.limits, download_renew_seconds=0.1)
    original = api.call

    async def fail(method, path, *args, **kwargs):
        if path.endswith("/renew") and failure == "renew":
            raise DomainError("METADATA_UNAVAILABLE")
        if path.endswith("/release") and failure == "release":
            raise DomainError("METADATA_UNAVAILABLE")
        value = await original(method, path, *args, **kwargs)
        if path == "/internal/v2/downloads" and failure == "roundtrip":
            sql(repo, "UPDATE download_leases SET expires_at=created_at+interval '0.3 seconds'")
            value["expires_at"] = sql(repo, "SELECT expires_at FROM download_leases")[0][
                "expires_at"
            ].isoformat()
            await asyncio.sleep(0.4)  # Result is already too old to authorize bytes.
        return value

    api.call = fail
    with readers(http_cluster, ["slow" if failure == "renew" else "ok", "ok"]) as (calls, drained):
        response = httpx.get(transfer + path)
        if failure == "release":
            assert response.status_code == 200 and response.content == b"abc"
            until(lambda: not downloads.active)
            assert sql(repo, "SELECT state FROM download_leases")[0]["state"] == "ACTIVE"
        else:
            error(
                response,
                503,
                "METADATA_UNAVAILABLE" if failure == "renew" else "STORAGE_UNAVAILABLE",
            )
            if failure == "roundtrip":
                assert not calls
            else:
                until(lambda: bool(drained))
                assert len(calls) == 1
            released(repo)


@pytest.mark.parametrize("state", ["UPLOADING", "VERIFYING", "FAILED", "DELETING"])
def test_state_gate_precedes_matching_precondition(http_cluster, state):
    repo, _, _, _, transfer, _, _ = http_cluster
    _, path = published(http_cluster)
    sql(repo, "UPDATE files SET state=:state", state=state)
    response = httpx.get(transfer + path, headers={"If-None-Match": "*"})
    error(
        response,
        404 if state == "DELETING" else 409,
        "FILE_NOT_FOUND" if state == "DELETING" else "FILE_NOT_READY",
    )
    assert not sql(repo, "SELECT * FROM download_leases")
