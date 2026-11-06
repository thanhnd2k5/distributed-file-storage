"""D3 gates over real HTTP/PostgreSQL; selected flows include ingress and Storage."""

import asyncio
import base64
import json
import shutil
import socket
import subprocess
from contextlib import ExitStack, contextmanager
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy.exc import OperationalError
from test_v2_c2 import cluster as cluster
from test_v2_c2 import due, run_job
from test_v2_c3 import create, error, put, serve, until
from test_v2_c3 import http_cluster as http_cluster
from test_v2_d2 import available
from test_v2_metadata import foundation as foundation
from test_v2_metadata import parallel, sql

from metadata.v2.api import create_app
from metadata.v2.http import JsonClient
from metadata.v2.worker import Worker
from transfer.v2.app import create_app as transfer_app


@pytest.fixture
def api_http(foundation):
    repo = foundation
    with ExitStack() as stack:
        public = stack.enter_context(serve(create_app(repo)))
        internal = stack.enter_context(serve(create_app(repo, internal=True)))
        client = JsonClient(public, internal, repo.epoch, timeout=3)
        transfer = stack.enter_context(serve(transfer_app(client)))
        with httpx.Client(timeout=5) as http:
            yield repo, http, public, internal, transfer, client


def read(http, internal, repo, file_id, **extra):
    response = http.post(
        internal + "/internal/v2/downloads",
        json=dict(recovery_epoch=str(repo.epoch), file_id=str(file_id), **extra),
    )
    assert response.status_code == 200, response.text
    return response.json()


def read_headers(repo, lease):
    return {"X-Recovery-Epoch": str(repo.epoch), "X-Read-Lease-Token": lease["lease_token"]}


def command(repo, lease):
    return dict(recovery_epoch=str(repo.epoch), lease_token=lease["lease_token"])


def public_only(value):
    if isinstance(value, dict):
        assert (
            not {
                "host",
                "port",
                "lease_token",
                "claim_token",
                "claim_owner",
                "allocation_id",
                "incarnation",
                "namespace_id",
                "recovery_epoch",
                "destinations",
                "revision",
                "expected_sha256",
                "actual_sha256",
            }
            & value.keys()
        )
        for child in value.values():
            public_only(child)
    elif isinstance(value, list):
        for child in value:
            public_only(child)


def test_catalog_keyset_ties_deleted_cursor_and_concurrent_insert(api_http):
    repo, http, public, _, _, _ = api_http
    sessions = [available(repo, b"") for _ in range(5)]
    sql(repo, "UPDATE files SET created_at='2026-01-01T00:00:00Z'")
    expected = sorted([str(s["file_id"]) for s in sessions], reverse=True)
    first = http.get(public + "/api/v2/files?limit=2").json()
    public_only(first)
    assert [f["file_id"] for f in first["items"]] == expected[:2]
    assert first["next_cursor"]
    # The cursor remains usable after its anchor row is hidden.
    repo.delete_file(UUID(first["items"][-1]["file_id"]))
    create(public, b"")  # UPLOADING is not in the catalog.
    newcomer = available(repo, b"")
    rest = http.get(
        public + "/api/v2/files", params={"cursor": first["next_cursor"], "limit": 2}
    ).json()
    assert [f["file_id"] for f in rest["items"]] == expected[2:4]
    last = http.get(public + "/api/v2/files", params={"cursor": rest["next_cursor"]}).json()
    assert [f["file_id"] for f in last["items"]] == expected[4:]
    assert last["next_cursor"] is None
    current = http.get(public + "/api/v2/files").json()
    assert current["items"][0]["file_id"] == str(newcomer["file_id"])
    assert len(current["items"]) == 5
    assert not sql(repo, "SELECT * FROM download_leases")


@pytest.mark.parametrize(
    "query",
    [
        "limit=0",
        "limit=101",
        "limit=true",
        "limit=1.0",
        "limit=01",
        "limit=1&limit=2",
        "state=FAILED",
        "cursor=",
        "cursor=!",
        "cursor=YWJj",
        "cursor=" + "a" * 1025,
    ],
)
def test_catalog_rejects_invalid_or_ambiguous_query(api_http, query):
    _, http, public, _, _, _ = api_http
    error(http.get(public + "/api/v2/files?" + query), 422, "VALIDATION_ERROR")


@pytest.mark.parametrize("change", ["v", "state", "scope", "timestamp", "uuid", "extra"])
def test_catalog_cursor_binds_version_filter_and_scope(api_http, change):
    repo, http, public, _, _, _ = api_http
    available(repo, b"")
    available(repo, b"")
    cursor = http.get(public + "/api/v2/files?limit=1").json()["next_cursor"]
    data = json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
    if change == "timestamp":
        data["created_at"] = "2026-01-01T00:00:00"  # no timezone
    elif change == "uuid":
        data["file_id"] = "not-a-uuid"
    else:
        data[{"extra": "extra"}.get(change, change)] = {
            "v": 2,
            "state": "FAILED",
            "scope": "0" * 64,
            "extra": True,
        }[change]
    encoded = base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip("=")
    error(http.get(public + "/api/v2/files", params={"cursor": encoded}), 422, "VALIDATION_ERROR")


@pytest.mark.parametrize(
    "state", ["UPLOADING", "VERIFYING", "FAILED", "AVAILABLE", "DELETING", "DELETED"]
)
def test_file_summary_visibility_checksum_and_chunks(api_http, state):
    repo, http, public, _, _, _ = api_http
    session = available(repo)
    sql(repo, "UPDATE files SET state=:state", state=state)
    path = public + f"/api/v2/files/{session['file_id']}"
    response = http.get(path)
    if state in ("DELETING", "DELETED"):
        error(response, 404, "FILE_NOT_FOUND")
        error(http.get(path + "/chunks"), 404, "FILE_NOT_FOUND")
    else:
        assert response.status_code == 200
        summary = response.json()
        assert set(summary) == {
            "file_id",
            "version_id",
            "original_name",
            "content_type",
            "size_bytes",
            "checksum_sha256",
            "state",
            "created_at",
            "available_at",
        }
        assert summary["state"] == state
        assert summary["checksum_sha256"] == (
            session["expected_sha256"] if state == "AVAILABLE" else None
        )
        public_only(summary)
        chunks = http.get(path + "/chunks")
        if state == "AVAILABLE":
            assert chunks.status_code == 200
            public_only(chunks.json())
            assert len(chunks.json()["items"][0]["replicas"]) == 2
        else:
            error(chunks, 409, "FILE_NOT_READY")
    assert len(http.get(public + "/api/v2/files").json()["items"]) == (state == "AVAILABLE")
    assert not sql(repo, "SELECT * FROM download_leases")


def test_public_manifest_pages_observations_without_internal_destinations(api_http):
    repo, http, public, _, _, _ = api_http
    session = available(repo, b"abcdefg")
    sql(repo, "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-0'")
    sql(
        repo,
        "UPDATE allocations SET replica_health='CORRUPT',observed_at=clock_timestamp() "
        "WHERE node_id='node-0'",
    )
    path = public + f"/api/v2/files/{session['file_id']}/chunks"
    first = http.get(path + "?limit=1").json()
    assert first["has_more"] and first["next_after_index"] == 0
    assert first["items"][0]["replicas"][0]["node_health"] == "DOWN"
    assert first["items"][0]["replicas"][0]["replica_health"] == "CORRUPT"
    rest = http.get(path + "?after_index=0&limit=1000").json()
    assert [p["index"] for p in rest["items"]] == [1, 2]
    assert rest["items"][-1]["size_bytes"] == 1 and not rest["has_more"]
    assert rest["next_after_index"] is None
    public_only(first)
    public_only(rest)
    for query in (
        "after_index=-2",
        "limit=1001",
        "after_index=true",
        "limit=1&limit=2",
        "token=abc",
    ):
        error(http.get(path + "?" + query), 422, "VALIDATION_ERROR")


def test_internal_read_commands_commit_page_renew_and_release_via_json_client(api_http):
    repo, http, _, internal, _, client = api_http
    session = available(repo, b"abcdefg")
    lease = asyncio.run(
        client.call(
            "POST",
            "/internal/v2/downloads",
            dict(file_id=str(session["file_id"]), version_id=str(session["version_id"])),
            internal=True,
        )
    )
    row = sql(repo, "SELECT * FROM download_leases")[0]
    assert (
        str(row["read_id"]) == lease["read_id"] and str(row["lease_token"]) == lease["lease_token"]
    )
    assert lease["size_bytes"] == 7 and lease["checksum_sha256"] == session["expected_sha256"]
    assert lease["db_now"] == lease["created_at"]
    path = f"/internal/v2/downloads/{lease['read_id']}"
    page = asyncio.run(
        client.call(
            "GET",
            path + "/manifest?limit=1",
            internal=True,
            headers={"X-Read-Lease-Token": lease["lease_token"]},
        )
    )
    assert page["has_more"] and page["next_after_index"] == 0
    d = page["items"][0]["destinations"][0]
    assert {
        "namespace_id",
        "recovery_epoch",
        "incarnation",
        "allocation_id",
        "generation",
        "host",
        "port",
    } <= d.keys()
    renewed = asyncio.run(
        client.call("POST", path + "/renew", dict(lease_token=lease["lease_token"]), internal=True)
    )
    assert renewed["absolute_expires_at"] == lease["absolute_expires_at"]
    for _ in range(2):
        assert (
            asyncio.run(
                client.call(
                    "POST", path + "/release", dict(lease_token=lease["lease_token"]), internal=True
                )
            )
            is None
        )
    error(
        http.get(internal + path + "/manifest", headers=read_headers(repo, lease)),
        409,
        "STALE_READ_LEASE",
    )
    error(http.post(internal + path + "/renew", json=command(repo, lease)), 409, "STALE_READ_LEASE")


@pytest.mark.parametrize(
    "change,code,status",
    [
        ("unknown", "VALIDATION_ERROR", 422),
        ("missing_epoch", "VALIDATION_ERROR", 422),
        ("wrong_epoch", "RECOVERY_REQUIRED", 503),
        ("bad_id", "VALIDATION_ERROR", 422),
        ("upper_id", "VALIDATION_ERROR", 422),
        ("bad_version", "VALIDATION_ERROR", 422),
        ("other_version", "PRECONDITION_FAILED", 412),
    ],
)
def test_acquire_strict_dto_does_not_create_lease_on_error(api_http, change, code, status):
    repo, http, _, internal, _, _ = api_http
    session = available(repo, b"")
    data = dict(recovery_epoch=str(repo.epoch), file_id=str(session["file_id"]))
    if change == "unknown":
        data["lease_token"] = str(uuid4())
    elif change == "missing_epoch":
        del data["recovery_epoch"]
    elif change == "wrong_epoch":
        data["recovery_epoch"] = str(uuid4())
    elif change in ("bad_id", "upper_id"):
        data["file_id"] = True if change == "bad_id" else data["file_id"].upper()
    else:
        data["version_id"] = "bad" if change == "bad_version" else str(uuid4())
    error(http.post(internal + "/internal/v2/downloads", json=data), status, code)
    assert not sql(repo, "SELECT * FROM download_leases")


def test_internal_manifest_headers_token_validation_and_listener_isolation(api_http):
    repo, http, public, internal, _, _ = api_http
    session = available(repo)
    lease = read(http, internal, repo, session["file_id"])
    path = f"/internal/v2/downloads/{lease['read_id']}"
    valid = read_headers(repo, lease)
    for headers in (
        {},
        {"X-Recovery-Epoch": str(repo.epoch)},
        dict(valid, **{"X-Read-Lease-Token": "bad"}),
        [
            ("X-Recovery-Epoch", str(repo.epoch)),
            ("X-Read-Lease-Token", lease["lease_token"]),
            ("X-Read-Lease-Token", lease["lease_token"]),
        ],
    ):
        error(http.get(internal + path + "/manifest", headers=headers), 422, "VALIDATION_ERROR")
    error(
        http.get(
            internal + path + "/manifest", headers=dict(valid, **{"X-Recovery-Epoch": str(uuid4())})
        ),
        503,
        "RECOVERY_REQUIRED",
    )
    wrong = dict(valid, **{"X-Read-Lease-Token": str(uuid4())})
    error(http.get(internal + path + "/manifest", headers=wrong), 409, "STALE_READ_LEASE")
    error(
        http.post(
            internal + path + "/release", json=dict(command(repo, lease), lease_token=str(uuid4()))
        ),
        409,
        "STALE_READ_LEASE",
    )
    error(
        http.post(internal + f"/internal/v2/downloads/{uuid4()}/renew", json=command(repo, lease)),
        404,
        "READ_LEASE_NOT_FOUND",
    )
    assert http.get(public + path + "/manifest", headers=valid).status_code == 404
    assert (
        http.post(
            public + "/internal/v2/downloads",
            json=dict(file_id=str(session["file_id"]), recovery_epoch=str(repo.epoch)),
        ).status_code
        == 404
    )
    assert http.get(internal + f"/api/v2/files/{session['file_id']}").status_code == 404
    assert http.delete(internal + f"/api/v2/files/{session['file_id']}").status_code == 404


@pytest.mark.parametrize(
    "headers,status",
    [
        ({}, 200),
        ({"Range": "bad", "If-Range": "bad"}, 200),
        ({"If-Match": '"other"'}, 412),
        ({"If-None-Match": "*"}, 304),
        ({"If-Match": "bad"}, 400),
        ({"If-Match": '"other"', "If-None-Match": "*"}, 412),
    ],
)
@pytest.mark.parametrize("data", [b"", b"abc"])
def test_head_status_headers_and_preconditions_without_lease_or_storage(
    api_http, monkeypatch, headers, status, data
):
    repo, http, _, _, transfer, _ = api_http
    session = available(repo, data)
    sql(repo, "UPDATE files SET original_name=:name", name='tệp "gốc".bin')

    def forbidden(*args, **kwargs):
        pytest.fail("HEAD must not acquire a read lease or access a manifest")

    monkeypatch.setattr(repo, "acquire_read", forbidden)
    monkeypatch.setattr(repo, "read_manifest", forbidden)
    response = http.head(transfer + f"/api/v2/files/{session['file_id']}/download", headers=headers)
    assert response.status_code == status and response.content == b""
    assert response.headers["ETag"] == f'"sha256-{session["expected_sha256"]}"'
    assert response.headers["Accept-Ranges"] == "bytes"
    assert response.headers["X-Request-ID"]
    if status == 200:
        assert response.headers["Content-Length"] == str(len(data))
        assert response.headers["Content-Type"] == "application/octet-stream"
        assert "filename*=UTF-8''" in response.headers["Content-Disposition"]
        assert "%22" in response.headers["Content-Disposition"]
    assert "Content-Encoding" not in response.headers and "Last-Modified" not in response.headers
    assert not sql(repo, "SELECT * FROM download_leases")


def test_head_combines_repeated_etag_lists_and_handles_state_before_conditionals(api_http):
    repo, http, _, _, transfer, _ = api_http
    session = available(repo)
    path = transfer + f"/api/v2/files/{session['file_id']}/download"
    etag = f'"sha256-{session["expected_sha256"]}"'
    response = http.head(path, headers=[("If-Match", '"other"'), ("If-Match", etag)])
    assert response.status_code == 200
    for state, status in (("VERIFYING", 409), ("FAILED", 409), ("DELETING", 404), ("DELETED", 404)):
        sql(repo, "UPDATE files SET state=:state", state=state)
        response = http.head(path, headers={"If-None-Match": etag})
        assert response.status_code == status and response.content == b""
        assert "ETag" not in response.headers
    assert http.head(transfer + "/api/v2/files/bad/download").status_code == 422


@pytest.mark.parametrize("change", ["mode", "epoch", "database"])
def test_d3_errors_use_envelope_retry_after_and_head_suppresses_body(api_http, monkeypatch, change):
    repo, http, public, internal, transfer, _ = api_http
    session = available(repo, b"")
    lease = read(http, internal, repo, session["file_id"])
    if change == "database":

        def unavailable(*args, **kwargs):
            raise OperationalError("private SQL", {}, Exception("private database details"))

        monkeypatch.setattr(repo, "file_record", unavailable)
        response = http.get(public + f"/api/v2/files/{session['file_id']}")
        code = "METADATA_UNAVAILABLE"
    else:
        if change == "mode":
            sql(repo, "UPDATE cluster_control SET mode='RECOVERING'")
        else:
            sql(repo, "UPDATE cluster_control SET recovery_epoch=:epoch", epoch=uuid4())
        code = "RECOVERY_REQUIRED"
        response = http.get(public + "/api/v2/files")
        error(http.delete(public + f"/api/v2/files/{session['file_id']}"), 503, code)
        error(
            http.get(
                internal + f"/internal/v2/downloads/{lease['read_id']}/manifest",
                headers=read_headers(repo, lease),
            ),
            503,
            code,
        )
    error(response, 503, code)
    assert set(response.json()) == {"error"} and response.json()["error"]["details"] == {}
    assert "private" not in response.text and response.headers["X-Request-ID"]
    head = http.head(transfer + f"/api/v2/files/{session['file_id']}/download")
    assert head.status_code == 503 and head.content == b"" and head.headers["Retry-After"] == "1"


def test_delete_rejects_unexpected_body_query_and_invalid_id(api_http):
    repo, http, public, _, _, _ = api_http
    session = available(repo, b"")
    path = public + f"/api/v2/files/{session['file_id']}"
    error(http.request("DELETE", path, content=b"{}"), 400, "INVALID_REQUEST")
    error(http.delete(path + "?extra=1"), 422, "VALIDATION_ERROR")
    error(http.delete(public + "/api/v2/files/bad"), 422, "VALIDATION_ERROR")
    error(http.delete(public + f"/api/v2/files/{uuid4()}"), 404, "FILE_NOT_FOUND")
    assert repo.resolve_download(session["file_id"])["state"] == "AVAILABLE"


@pytest.mark.parametrize("data", [b"", b"abcdefg"])
def test_http_delete_waits_for_reader_and_reaches_deleted_after_durable_cleanup(http_cluster, data):
    repo, resources, public, internal, transfer, _, _ = http_cluster
    session = create(public, data)
    for i in range(session["part_count"]):
        assert put(transfer, session["upload_id"], i, data[i * 3 : i * 3 + 3]).status_code == 200
    assert (
        httpx.post(public + f"/api/v2/uploads/{session['upload_id']}/complete", json={}).status_code
        == 202
    )
    worker = Worker(repo)
    try:
        assert run_job(worker, "FINALIZE")["state"] == "COMPLETED"
        with httpx.Client() as http:
            lease = read(http, internal, repo, session["file_id"])
            path = public + f"/api/v2/files/{session['file_id']}"
            first = http.delete(path)
            assert first.status_code == 202 and first.json()["cleanup_pending"]
            assert http.delete(path).json() == first.json()
            error(http.get(path), 404, "FILE_NOT_FOUND")
            error(
                http.post(
                    internal + "/internal/v2/downloads",
                    json=dict(file_id=session["file_id"], recovery_epoch=str(repo.epoch)),
                ),
                404,
                "FILE_NOT_FOUND",
            )
            assert http.get(public + "/api/v2/files").json()["items"] == []
            run_job(worker, "CLEANUP")
            assert (
                sql(repo, "SELECT coalesce(sum(charged_bytes),0) AS n FROM node_budgets")[0]["n"]
                == len(data) * 2
            )
            command_path = internal + f"/internal/v2/downloads/{lease['read_id']}"
            assert http.post(command_path + "/renew", json=command(repo, lease)).status_code == 200
            assert (
                len(
                    http.get(command_path + "/manifest", headers=read_headers(repo, lease)).json()[
                        "items"
                    ]
                )
                == session["part_count"]
            )
            assert (
                http.post(command_path + "/release", json=command(repo, lease)).status_code == 204
            )
            due(repo)
            run_job(worker, "CLEANUP")
            final = http.delete(path)
            assert (
                final.status_code == 200
                and final.json()["state"] == "DELETED"
                and not final.json()["cleanup_pending"]
            )
            for ledger, _, _ in resources:
                assert all(r["cleanup_complete"] for r in ledger.inventory(ledger.scope)[0])
                assert not list((ledger.root / "chunks").iterdir())
    finally:
        worker.close()


def test_http_acquire_delete_race_preserves_visibility_and_existing_reader(api_http):
    repo, http, public, internal, _, _ = api_http
    session = available(repo, b"")
    responses = parallel(
        [
            lambda: httpx.post(
                internal + "/internal/v2/downloads",
                json=dict(file_id=str(session["file_id"]), recovery_epoch=str(repo.epoch)),
            ),
            lambda: httpx.delete(public + f"/api/v2/files/{session['file_id']}"),
        ]
    )
    assert responses[1].status_code == 202
    if responses[0].status_code == 200:
        lease = responses[0].json()
        assert (
            http.get(
                internal + f"/internal/v2/downloads/{lease['read_id']}/manifest",
                headers=read_headers(repo, lease),
            ).status_code
            == 200
        )
    else:
        error(responses[0], 404, "FILE_NOT_FOUND")
    error(http.get(public + f"/api/v2/files/{session['file_id']}"), 404, "FILE_NOT_FOUND")


@contextmanager
def ingress(public, transfer, tmp_path):
    executable = shutil.which("nginx")
    assert executable, "D3 ingress requires compose.v2-http-tests.yml"
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
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

    try:
        until(ready)
        yield origin
    finally:
        process.terminate()
        process.wait(5)


def test_real_ingress_routes_head_to_transfer_and_keeps_commands_private(api_http, tmp_path):
    repo, http, public, internal, transfer, _ = api_http
    session = available(repo, b"")
    with ingress(public, transfer, tmp_path) as origin:
        path = f"/api/v2/files/{session['file_id']}/download"
        assert http.head(public + path).status_code == 404
        response = http.head(origin + path)
        assert response.status_code == 200 and response.headers["Content-Length"] == "0"
        assert response.content == b"" and response.headers["X-Request-ID"]
        assert http.get(origin + f"/api/v2/files/{session['file_id']}").status_code == 200
        assert http.post(origin + "/internal/v2/downloads", json={}).status_code == 404
        assert http.delete(origin + f"/api/v2/files/{session['file_id']}").status_code == 202
        assert http.head(origin + path).status_code == 404
        assert not sql(repo, "SELECT * FROM download_leases")
