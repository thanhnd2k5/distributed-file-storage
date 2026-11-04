"""Focused C3 boundary gates: real HTTP, PostgreSQL, gRPC and on-disk bytes."""

import asyncio
import hashlib
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from contextlib import ExitStack, contextmanager
from dataclasses import replace
from pathlib import Path
from threading import Event, Thread
from uuid import UUID, uuid4

import httpx
import pytest
import uvicorn
from test_v2_c2 import cluster as cluster
from test_v2_c2 import run_job
from test_v2_metadata import foundation as foundation
from test_v2_metadata import sql

from metadata.v2.api import create_app
from metadata.v2.http import JsonClient
from metadata.v2.repository import DomainError
from metadata.v2.runtime import Admission, Limits, worker_app
from metadata.v2.worker import Worker, WorkerConfig
from transfer.v2.app import create_app as transfer_app


def until(predicate, seconds=5):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("Condition timed out")


@contextmanager
def serve(app):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level="critical", timeout_graceful_shutdown=3))
    thread = Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    try:
        until(lambda: server.started or not thread.is_alive())
        assert server.started
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(5)
        sock.close()
        assert not thread.is_alive()


@pytest.fixture
def http_cluster(cluster):
    repo, resources = cluster
    with ExitStack() as stack:
        public = stack.enter_context(serve(create_app(repo)))
        internal_app = create_app(repo, internal=True)
        internal = stack.enter_context(serve(internal_app))
        client = JsonClient(public, internal, repo.epoch, timeout=3)
        app = transfer_app(client)
        transfer = stack.enter_context(serve(app))
        yield repo, resources, public, internal, transfer, app, client


def create(public, data=b"abcdefg", **values):
    payload = dict(
        original_name="sample.bin",
        size_bytes=len(data),
        expected_sha256=hashlib.sha256(data).hexdigest(),
    )
    payload.update(values)
    response = httpx.post(
        public + "/api/v2/uploads", json=payload, headers={"Idempotency-Key": str(uuid4())}
    )
    assert response.status_code == 201, response.text
    return response.json()


def put(transfer, upload, index, data, *, sha=None, chunked=False):
    return httpx.put(
        f"{transfer}/api/v2/uploads/{upload}/parts/{index}",
        content=iter([data[:1], data[1:]]) if chunked else data,
        headers={
            "Content-Type": "application/octet-stream",
            "X-Part-SHA256": sha or hashlib.sha256(data).hexdigest(),
        },
    )


def error(response, status, code):
    assert response.status_code == status, response.text
    assert response.json()["error"]["code"] == code
    if status in (429, 503) or code in ("OUTCOME_PENDING", "PART_BUSY", "PARTS_INCOMPLETE"):
        assert int(response.headers["Retry-After"]) > 0


@pytest.mark.parametrize("data", [b"", b"abc", b"abcd", b"abcdefg"])
def test_upload_complete_publish_real_http_bytes(http_cluster, data):
    repo, resources, public, internal, transfer, app, _ = http_cluster
    session = create(public, data)
    upload = session["upload_id"]
    assert session["confirmed_parts"] == session["confirmed_bytes"] == 0
    assert not {"claim_token", "claim_owner", "destinations", "revision"} & session.keys()
    for index in range(session["part_count"]):
        block = data[index * 3 : index * 3 + 3]
        response = put(transfer, upload, index, block, chunked=True)
        assert response.status_code == 200, response.text
        assert response.json()["state"] == "VERIFIED"
        # An independent connection sees the committed confirmation at HTTP 200.
        observed = repo.part_record(UUID(upload), index)
        assert observed["state"] == "VERIFIED"
    status = httpx.get(f"{public}/api/v2/uploads/{upload}").json()
    assert status["confirmed_bytes"] == len(data)
    page = httpx.get(f"{public}/api/v2/uploads/{upload}/parts?limit=1").json()
    assert len(page["items"]) == min(1, session["part_count"])
    assert page["has_more"] == (session["part_count"] > 1)
    response = httpx.post(f"{public}/api/v2/uploads/{upload}/complete", json={})
    assert response.status_code == 202
    assert response.headers["Location"] == f"/api/v2/uploads/{upload}"
    with serve(worker_app(repo, WorkerConfig(poll_seconds=0.02))) as worker_url:
        assert httpx.get(worker_url + "/health/ready").status_code == 200
        until(lambda: repo.get_session(UUID(upload))["state"] == "COMPLETED")
    assert httpx.post(f"{public}/api/v2/uploads/{upload}/complete", json={}).status_code == 200
    assert sql(repo, "SELECT actual_sha256,state FROM files")[0] == {
        "actual_sha256": hashlib.sha256(data).hexdigest(),
        "state": "AVAILABLE",
    }
    for ledger, _, _ in resources:
        records = ledger.inventory(ledger.scope)[0]
        assert len(records) == session["part_count"]
        assert all(r["state"] == "STORED" for r in records)


def test_create_replay_strict_json_and_pagination(http_cluster):
    repo, _, public, internal, transfer, _, _ = http_cluster
    key = str(uuid4())
    payload = dict(original_name="a.bin", size_bytes=3, expected_sha256="a" * 64)

    def send(value, **kw):
        return httpx.post(
            public + "/api/v2/uploads", json=value, headers={"Idempotency-Key": key}, **kw
        )

    first = send(payload)
    assert first.status_code == 201
    replay = send(dict(reversed(list(payload.items()))))
    assert replay.status_code == 200 and first.json() == replay.json()
    error(send(dict(payload, original_name="b.bin")), 409, "IDEMPOTENCY_CONFLICT")
    for value in (True, 3.0, "3", -1, 2**53):
        error(send(dict(payload, size_bytes=value)), 422, "VALIDATION_ERROR")
    for values in (
        {"unexpected": 1},
        {"original_name": "../bad"},
        {"expected_sha256": "A" * 64},
        {"original_name": "\u0085.bin"},
    ):
        error(send(dict(payload, **values)), 422, "VALIDATION_ERROR")
    response = httpx.post(
        public + "/api/v2/uploads",
        content=b"{" + b" " * 16384,
        headers={"Content-Type": "application/json"},
    )
    error(response, 400, "INVALID_REQUEST")
    uid = first.json()["upload_id"]
    error(httpx.get(f"{public}/api/v2/uploads/{uid}/parts?limit=1.0"), 422, "VALIDATION_ERROR")
    error(httpx.get(f"{public}/api/v2/uploads/{uid}/parts?limit=1001"), 422, "VALIDATION_ERROR")
    # Neither listener forwards the other's command routes.
    assert (
        httpx.post(public + f"/internal/v2/uploads/{uid}/parts/0/claim", json={}).status_code == 404
    )
    assert httpx.post(internal + "/api/v2/uploads", json={}).status_code == 404
    assert sql(repo, "SELECT count(*) n FROM files")[0]["n"] == 1


def test_verified_replay_drains_checks_body_without_new_allocations(http_cluster):
    repo, _, public, _, transfer, _, _ = http_cluster
    upload = create(public, b"abc")["upload_id"]
    assert put(transfer, upload, 0, b"abc").status_code == 200
    assert put(transfer, upload, 0, b"abc", chunked=True).status_code == 200
    error(
        put(transfer, upload, 0, b"xyz", sha=hashlib.sha256(b"abc").hexdigest()),
        422,
        "CHECKSUM_MISMATCH",
    )
    error(put(transfer, upload, 0, b"abc", sha="a" * 64), 409, "PART_CONFLICT")
    assert sql(repo, "SELECT count(*) n FROM allocations")[0]["n"] == 2
    assert repo.upload_status(UUID(upload))["confirmed_bytes"] == 3


@pytest.mark.parametrize(
    "data,sha,code,status",
    [
        (b"ab", None, "PART_SIZE_MISMATCH", 422),
        (b"abcd", None, "PART_TOO_LARGE", 413),
        (b"xyz", hashlib.sha256(b"abc").hexdigest(), "CHECKSUM_MISMATCH", 422),
    ],
)
def test_actual_bytes_fail_then_synchronous_retry_handoff(http_cluster, data, sha, code, status):
    repo, _, public, _, transfer, _, _ = http_cluster
    upload = create(public, b"abc")["upload_id"]
    sha = sha or hashlib.sha256(b"abc").hexdigest()
    error(put(transfer, upload, 0, data, sha=sha, chunked=True), status, code)
    assert repo.part_record(UUID(upload), 0)["state"] == "RECONCILING"
    response = put(transfer, upload, 0, b"abc")
    assert response.status_code == 200, response.text
    assert {r["generation"] for r in sql(repo, "SELECT generation FROM allocations")} == {2}
    assert sql(repo, "SELECT sum(charged_bytes) n FROM node_budgets")[0]["n"] == 6


def test_content_length_busy_epoch_and_internal_dto(http_cluster):
    repo, _, public, internal, transfer, _, _ = http_cluster
    upload = create(public, b"abc")["upload_id"]
    error(put(transfer, upload, 0, b"ab"), 422, "PART_SIZE_MISMATCH")
    assert not sql(repo, "SELECT * FROM allocations")
    payload = dict(
        recovery_epoch=str(repo.epoch),
        size_bytes=3,
        checksum_sha256=hashlib.sha256(b"abc").hexdigest(),
        request_id=str(uuid4()),
    )
    path = f"{internal}/internal/v2/uploads/{upload}/parts/0/claim"
    error(
        httpx.post(path, json=dict(payload, recovery_epoch=str(uuid4()))), 503, "RECOVERY_REQUIRED"
    )
    error(httpx.post(path, json=dict(payload, surprise=True)), 422, "VALIDATION_ERROR")
    assert httpx.post(path, json=payload).status_code == 200
    error(put(transfer, upload, 0, b"abc"), 409, "PART_BUSY")
    assert sql(repo, "SELECT count(*) n FROM allocations")[0]["n"] == 2


@pytest.mark.parametrize("committed", [False, True])
def test_lost_confirmation_response_reconcile_and_body_replay(http_cluster, committed):
    repo, _, public, _, transfer, _, client = http_cluster
    upload = create(public, b"abc")["upload_id"]
    original = client.call

    async def lost(method, path, *args, **kwargs):
        if path.endswith("/confirm"):
            if committed:
                await original(method, path, *args, **kwargs)
            raise DomainError("METADATA_UNAVAILABLE")
        return await original(method, path, *args, **kwargs)

    client.call = lost
    error(put(transfer, upload, 0, b"abc"), 503, "METADATA_UNAVAILABLE")
    client.call = original
    assert repo.part_record(UUID(upload), 0)["state"] == (
        "VERIFIED" if committed else "RECONCILING"
    )
    assert put(transfer, upload, 0, b"abc").status_code == 200
    assert {r["generation"] for r in sql(repo, "SELECT generation FROM allocations")} == {1}


def test_lost_retry_handoff_recovers_through_claim_sweeper(http_cluster):
    repo, _, public, _, transfer, _, client = http_cluster
    upload = create(public, b"abc")["upload_id"]
    sha = hashlib.sha256(b"abc").hexdigest()
    error(put(transfer, upload, 0, b"xyz", sha=sha), 422, "CHECKSUM_MISMATCH")
    original = client.call

    async def lose_claim(method, path, *args, **kwargs):
        result = await original(method, path, *args, **kwargs)
        if path.endswith("/claim"):
            raise DomainError("METADATA_UNAVAILABLE")
        return result

    client.call = lose_claim
    error(put(transfer, upload, 0, b"abc"), 503, "METADATA_UNAVAILABLE")
    client.call = original
    part = repo.part_record(UUID(upload), 0)
    assert part["state"] == "CLAIMED" and part["claim_held"]
    error(put(transfer, upload, 0, b"abc"), 409, "PART_BUSY")
    sql(repo, "UPDATE upload_parts SET claim_expires_at=clock_timestamp()-interval '1 second'")
    assert repo.sweep_claims() == 1
    assert put(transfer, upload, 0, b"abc").status_code == 200
    assert {r["generation"] for r in sql(repo, "SELECT generation FROM allocations")} == {3}
    assert sql(repo, "SELECT sum(charged_bytes) n FROM node_budgets")[0]["n"] == 6


def test_backlog_admission_preserves_idempotency_replay(http_cluster):
    repo, _, public, _, transfer, _, _ = http_cluster
    repo.max_job_backlog = 1
    key = str(uuid4())
    payload = dict(
        original_name="empty", size_bytes=0, expected_sha256=hashlib.sha256(b"").hexdigest()
    )
    first = httpx.post(public + "/api/v2/uploads", json=payload, headers={"Idempotency-Key": key})
    upload = first.json()["upload_id"]
    assert httpx.post(f"{public}/api/v2/uploads/{upload}/complete", json={}).status_code == 202
    error(
        httpx.post(
            public + "/api/v2/uploads", json=payload, headers={"Idempotency-Key": str(uuid4())}
        ),
        429,
        "RESOURCE_LIMIT",
    )
    replay = httpx.post(public + "/api/v2/uploads", json=payload, headers={"Idempotency-Key": key})
    assert replay.status_code == 200 and replay.json()["upload_id"] == upload


def test_multiple_grpc_blocks_real_http_hash_and_finalize(http_cluster):
    repo, resources, public, _, transfer, _, _ = http_cluster
    chunk_size = 1024 * 1024
    repo.policy = replace(repo.policy, chunk_size_bytes=chunk_size)
    for ledger, _, _ in resources:
        ledger.chunk_limit = chunk_size
        repo.set_budget(ledger.scope.node_id, UUID(ledger.scope.node_incarnation), 4 * chunk_size)
    data = bytes(range(256)) * 8192 + b"tail"
    session = create(public, data)
    for i in range(session["part_count"]):
        response = put(
            transfer,
            session["upload_id"],
            i,
            data[i * chunk_size : (i + 1) * chunk_size],
            chunked=True,
        )
        assert response.status_code == 200, response.text
    assert (
        httpx.post(f"{public}/api/v2/uploads/{session['upload_id']}/complete", json={}).status_code
        == 202
    )
    worker = Worker(repo)
    try:
        run_job(worker, "FINALIZE")
        assert repo.get_session(UUID(session["upload_id"]))["state"] == "COMPLETED"
        assert (
            sql(repo, "SELECT actual_sha256 FROM files")[0]["actual_sha256"]
            == hashlib.sha256(data).hexdigest()
        )
    finally:
        worker.close()


def test_abort_cleanup_and_full_hash_mismatch(http_cluster):
    repo, resources, public, _, transfer, _, _ = http_cluster
    session = create(public, b"abc", expected_sha256="a" * 64)
    upload = session["upload_id"]
    assert put(transfer, upload, 0, b"abc").status_code == 200
    assert httpx.post(f"{public}/api/v2/uploads/{upload}/complete", json={}).status_code == 202
    worker = Worker(repo)
    try:
        run_job(worker, "FINALIZE")
        status = httpx.get(f"{public}/api/v2/uploads/{upload}").json()
        assert status["state"] == "FAILED" and status["cleanup_pending"]
        run_job(worker, "CLEANUP")
        assert not httpx.get(f"{public}/api/v2/uploads/{upload}").json()["cleanup_pending"]
        other = create(public, b"abc")["upload_id"]
        assert put(transfer, other, 0, b"abc").status_code == 200
        assert httpx.post(f"{public}/api/v2/uploads/{other}/abort", json={}).status_code == 202
        error(put(transfer, other, 0, b"abc"), 410, "UPLOAD_ABORTED")
        run_job(worker, "CLEANUP")
        assert httpx.post(f"{public}/api/v2/uploads/{other}/abort", json={}).status_code == 200
        assert sql(repo, "SELECT sum(charged_bytes) n FROM node_budgets")[0]["n"] == 0
    finally:
        worker.close()


def test_stream_admission_deadline_disconnect_and_renew_failure(http_cluster):
    repo, _, public, _, transfer, app, client = http_cluster
    upload = create(public, b"abc")["upload_id"]
    service = app.state.transfer
    service.limits = replace(service.limits, streams=1, request_seconds=0.5, renew_seconds=0.05)
    sock = socket.create_connection(("127.0.0.1", int(transfer.rsplit(":", 1)[1])))
    headers = (
        f"PUT /api/v2/uploads/{upload}/parts/0 HTTP/1.1\r\nHost: localhost\r\n"
        "Content-Type: application/octet-stream\r\n"
        f"X-Part-SHA256: {hashlib.sha256(b'abc').hexdigest()}\r\n"
        "Transfer-Encoding: chunked\r\n\r\n1\r\na\r\n"
    )
    sock.sendall(headers.encode())
    until(lambda: len(service.active) == 1)
    error(put(transfer, upload, 0, b"abc"), 429, "RESOURCE_LIMIT")
    sock.settimeout(3)
    result = sock.recv(4096)
    assert b"503 Service Unavailable" in result
    sock.close()
    until(lambda: not service.active)
    assert repo.part_record(UUID(upload), 0)["state"] == "RECONCILING"

    # Distinct upload: disconnect cancels pending queue/RPC work promptly.
    other = create(public, b"abc")["upload_id"]
    sock = socket.create_connection(("127.0.0.1", int(transfer.rsplit(":", 1)[1])))
    sock.sendall(headers.replace(upload, other).encode())
    until(lambda: repo.upload_status(UUID(other))["active_claims"] == 1)
    sock.close()
    until(lambda: not service.active)
    assert repo.part_record(UUID(other), 0)["state"] == "RECONCILING"

    # A DB-authoritative lost renewal ends the stream before the request deadline.
    third = create(public, b"abc")["upload_id"]
    original = client.call

    async def lose_renew(method, path, *args, **kwargs):
        if path.endswith("/renew"):
            raise DomainError("STALE_CLAIM")
        return await original(method, path, *args, **kwargs)

    client.call = lose_renew
    sock = socket.create_connection(("127.0.0.1", int(transfer.rsplit(":", 1)[1])))
    try:
        sock.sendall(headers.replace(upload, third).encode())
        sock.settimeout(3)
        assert b"409 Conflict" in sock.recv(4096)
    finally:
        sock.close()
        client.call = original
    until(lambda: not service.active)
    assert repo.part_record(UUID(third), 0)["state"] == "RECONCILING"


def test_independent_readiness_and_recovery_gate(http_cluster):
    repo, _, public, internal, transfer, _, _ = http_cluster
    # No background Worker is running; API and Transfer remain independently ready.
    for url in (public, internal, transfer):
        assert httpx.get(url + "/health/ready").status_code == 200
    sql(repo, "UPDATE cluster_control SET mode='RECOVERING'")
    for url in (public, internal, transfer):
        error(httpx.get(url + "/health/ready"), 503, "RECOVERY_REQUIRED")
        assert httpx.get(url + "/health/live").status_code == 200


def test_limits_and_control_admission(cluster):
    repo, _ = cluster
    with pytest.raises(ValueError):
        Limits(buffered_bytes=1)
    with pytest.raises(ValueError):
        Limits(queue_blocks=3)
    with pytest.raises(ValueError):
        Limits(request_seconds=float("inf"))
    app = create_app(repo)
    app.add_middleware(Admission, limit=1)
    entered, release = Event(), Event()
    original = repo.upload_status

    def blocked(*args):
        entered.set()
        assert release.wait(3)
        return original(*args)

    repo.upload_status = blocked
    with serve(app) as url:
        session, _ = repo.create_upload(
            uuid4(),
            original_name="a",
            size_bytes=0,
            expected_sha256=hashlib.sha256(b"").hexdigest(),
        )
        first = Thread(target=lambda: httpx.get(f"{url}/api/v2/uploads/{session['upload_id']}"))
        first.start()
        try:
            assert entered.wait(3)
            error(httpx.get(url + "/api/v2/config"), 429, "RESOURCE_LIMIT")
            assert httpx.get(url + "/health/ready").status_code == 200
        finally:
            release.set()
            first.join(4)


def test_transfer_runtime_sigterm_drain_and_resume(http_cluster):
    repo, _, public, internal, _, _, _ = http_cluster
    port_socket = socket.socket()
    port_socket.bind(("127.0.0.1", 0))
    port = port_socket.getsockname()[1]
    port_socket.close()
    env = dict(
        os.environ,
        V2_SERVICE_ROLE="transfer",
        V2_PORT=str(port),
        V2_API_URL=public,
        V2_INTERNAL_URL=internal,
        V2_RECOVERY_EPOCH=str(repo.epoch),
        V2_SHUTDOWN_SECONDS="1",
        PYTHONPATH=os.pathsep.join([".", "generated"]),
    )
    process = subprocess.Popen(
        [sys.executable, "-m", "metadata.v2.runtime"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"http://127.0.0.1:{port}"

    def ready():
        assert process.poll() is None
        try:
            return httpx.get(url + "/health/ready", timeout=0.2).status_code == 200
        except httpx.TransportError:
            return False

    sock = None
    try:
        until(ready)
        upload = create(public, b"abc")["upload_id"]
        sock = socket.create_connection(("127.0.0.1", port))
        sock.sendall(
            (
                f"PUT /api/v2/uploads/{upload}/parts/0 HTTP/1.1\r\nHost: localhost\r\n"
                "Content-Type: application/octet-stream\r\n"
                f"X-Part-SHA256: {hashlib.sha256(b'abc').hexdigest()}\r\n"
                "Transfer-Encoding: chunked\r\n\r\n1\r\na\r\n"
            ).encode()
        )
        until(lambda: repo.upload_status(UUID(upload))["active_claims"] == 1)
        process.send_signal(signal.SIGTERM)
        # Uvicorn re-raises the captured signal after graceful shutdown on Unix.
        assert process.wait(timeout=7) in (0, -signal.SIGTERM)
        part = repo.part_record(UUID(upload), 0)
        if part["state"] == "CLAIMED":
            # SIGTERM can beat delivery of the committed claim response. The
            # replacement must wait for the durable lease, never steal it.
            sql(
                repo,
                "UPDATE upload_parts SET claim_expires_at=clock_timestamp()-interval '1 second'",
            )
            assert repo.sweep_claims() == 1
        assert repo.part_record(UUID(upload), 0)["state"] == "RECONCILING"
        # A replacement Transfer resumes using a generation-fenced handoff.
        with serve(transfer_app(JsonClient(public, internal, repo.epoch))) as replacement:
            assert put(replacement, upload, 0, b"abc").status_code == 200
    finally:
        if sock:
            sock.close()
        if process.poll() is None:
            process.kill()
            process.wait()


def test_real_ingress_streams_before_eof_and_blocks_internal(http_cluster, tmp_path):
    repo, _, public, internal, transfer, _, _ = http_cluster
    executable = shutil.which("nginx")
    assert executable, "C3 ingress gate requires compose.v2-http-tests.yml"
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    config = Path("/app/v2-ingress.conf").read_text()
    config = config.replace("api:8080", public.removeprefix("http://"))
    config = config.replace("transfer:8080", transfer.removeprefix("http://"))
    config = config.replace("listen 8080", f"listen {port}")
    config = config.replace("/tmp/v2-nginx.pid", str(tmp_path / "nginx.pid"))
    path = tmp_path / "nginx.conf"
    path.write_text(config)
    process = subprocess.Popen(
        [executable, "-c", str(path), "-g", "daemon off;"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    origin = f"http://127.0.0.1:{port}"

    def ready():
        assert process.poll() is None
        try:
            return httpx.get(origin + "/api/v2/config", timeout=0.2).status_code == 200
        except httpx.TransportError:
            return False

    stream = None
    try:
        until(ready)
        upload = create(origin, b"abc")["upload_id"]
        assert (
            httpx.post(origin + f"/internal/v2/uploads/{upload}/parts/0/claim", json={}).status_code
            == 404
        )
        stream = socket.create_connection(("127.0.0.1", port))
        stream.sendall(
            (
                f"PUT /api/v2/uploads/{upload}/parts/0 HTTP/1.1\r\nHost: localhost\r\n"
                "Content-Type: application/octet-stream\r\n"
                f"X-Part-SHA256: {hashlib.sha256(b'abc').hexdigest()}\r\n"
                "Transfer-Encoding: chunked\r\n\r\n1\r\na\r\n"
            ).encode()
        )
        # Claim and Prepare happen before the sender supplies HTTP EOF: the
        # actual ingress did not buffer the complete request before forwarding.
        until(lambda: repo.upload_status(UUID(upload))["active_claims"] == 1)
        assert repo.part_record(UUID(upload), 0)["state"] == "CLAIMED"
        stream.sendall(b"2\r\nbc\r\n0\r\n\r\n")
        stream.settimeout(5)
        assert b"200 OK" in stream.recv(4096)
        assert repo.part_record(UUID(upload), 0)["state"] == "VERIFIED"
        assert httpx.post(f"{origin}/api/v2/uploads/{upload}/complete", json={}).status_code == 202
        with serve(worker_app(repo, WorkerConfig(poll_seconds=0.02))):
            until(lambda: repo.get_session(UUID(upload))["state"] == "COMPLETED")
    finally:
        if stream:
            stream.close()
        process.terminate()
        process.wait(timeout=5)


def test_slow_replica_bounds_producer_queue_and_cancels(http_cluster):
    _, _, _, _, _, app, _ = http_cluster
    service = app.state.transfer
    consumed = []

    class Body:
        async def stream(self):
            for index in range(100):
                consumed.append(index)
                yield b"a" * service.limits.block_bytes

    async def check():
        queue = asyncio.Queue(service.limits.queue_blocks)
        task = asyncio.create_task(
            service.consume(
                Body(), 100 * service.limits.block_bytes, "a" * 64, [queue], asyncio.Event()
            )
        )
        await asyncio.sleep(0.05)
        assert queue.qsize() == 2
        assert len(consumed) == 3  # two queued blocks and one held producer block
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(check())


@pytest.mark.parametrize("role", ["api", "internal", "worker"])
def test_role_entrypoint_readiness_and_sigterm(cluster, role):
    repo, _ = cluster
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    env = dict(
        os.environ,
        V2_SERVICE_ROLE=role,
        V2_PORT=str(port),
        V2_DATABASE_URL=repo.engine.url.render_as_string(hide_password=False),
        V2_DATABASE_SCHEMA=repo.schema,
        V2_NAMESPACE_ID=str(repo.namespace_id),
        V2_RECOVERY_EPOCH=str(repo.epoch),
        V2_JOB_BACKLOG_LIMIT="2",
        V2_WORKER_POLL_SECONDS="0.02",
        V2_SHUTDOWN_SECONDS="2",
        PYTHONPATH=os.pathsep.join([".", "generated"]),
    )
    process = subprocess.Popen(
        [sys.executable, "-m", "metadata.v2.runtime"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"http://127.0.0.1:{port}"

    def ready():
        assert process.poll() is None
        try:
            return httpx.get(url + "/health/ready", timeout=0.2).status_code == 200
        except httpx.TransportError:
            return False

    try:
        until(ready)
        assert httpx.get(url + "/health/live").status_code == 200
        if role == "api":
            assert httpx.get(url + "/api/v2/config").json()["max_job_backlog"] == 2
        else:
            assert httpx.get(url + "/api/v2/config").status_code == 404
        sql(repo, "UPDATE cluster_control SET mode='RECOVERING'")
        # Worker can stop its loop when the gate changes. Its health is still
        # independent from liveness of the HTTP listener.
        assert httpx.get(url + "/health/ready").status_code == 503
        assert httpx.get(url + "/health/live").status_code == 200
        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=7) in (0, -signal.SIGTERM)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


@pytest.mark.parametrize("state", ["ABORTED", "EXPIRED"])
def test_complete_terminal_session_returns_gone(http_cluster, state):
    repo, _, public, _, _, _, _ = http_cluster
    upload = create(public, b"")["upload_id"]
    if state == "ABORTED":
        repo.abort(UUID(upload))
    else:
        sql(repo, "UPDATE upload_sessions SET expires_at=clock_timestamp()-interval '1 second'")
        repo.expire_sessions()
    response = httpx.post(f"{public}/api/v2/uploads/{upload}/complete", json={})
    error(response, 410, "UPLOAD_" + state)


def test_error_request_id_and_invalid_confirmation_evidence(http_cluster, caplog):
    repo, _, public, internal, _, _, _ = http_cluster
    upload = create(public, b"abc")["upload_id"]
    path = f"{internal}/internal/v2/uploads/{upload}/parts/0"
    claim = httpx.post(
        path + "/claim",
        json=dict(
            recovery_epoch=str(repo.epoch),
            size_bytes=3,
            checksum_sha256=hashlib.sha256(b"abc").hexdigest(),
            request_id=str(uuid4()),
        ),
    ).json()
    proofs = [
        dict(d, state="STORED", actual_size=3, actual_hash=hashlib.sha256(b"abc").hexdigest())
        for d in claim["destinations"]
    ]
    for proof in proofs:
        proof.pop("host")
        proof.pop("port")
    proofs[0]["generation"] += 1
    error(
        httpx.post(
            path + "/confirm",
            json=dict(
                recovery_epoch=str(repo.epoch),
                claim_token=claim["claim_token"],
                revision=claim["revision"],
                allocations=proofs,
            ),
        ),
        422,
        "VALIDATION_ERROR",
    )
    assert repo.part_record(UUID(upload), 0)["state"] == "CLAIMED"
    app = create_app(repo)

    @app.get("/boom")
    async def boom():
        raise RuntimeError("C3 error log probe")

    with serve(app) as url:
        response = httpx.get(url + "/boom")
        error(response, 500, "INTERNAL_ERROR")
        request_id = response.headers["X-Request-ID"]
        assert str(UUID(request_id)) == request_id
        assert request_id in caplog.text
