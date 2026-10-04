from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Event
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, update
from sqlalchemy.exc import SQLAlchemyError
from test_metadata_upload import cluster as production_cluster
from test_metadata_upload import start_http

from metadata.config import MetadataSettings
from metadata.main import create_app
from metadata.models import Chunk, ChunkReplica, File, StorageNode

PREFIX = "/api/v1/files"
upload_cluster = production_cluster
NOW = datetime(2026, 10, 4, 10, tzinfo=timezone(timedelta(hours=7)))
SUMMARY_KEYS = {
    "file_id",
    "original_name",
    "content_type",
    "size_bytes",
    "chunk_size_bytes",
    "total_chunks",
    "replication_factor",
    "checksum_sha256",
    "status",
    "created_at",
}
COUNTERS = (
    "under_replicated_chunks",
    "unavailable_chunks",
    "domain_degraded_chunks",
    "over_replicated_chunks",
)


@pytest.fixture
def api(database, monkeypatch):
    sessions, url = database
    settings = MetadataSettings(
        _env_file=None,
        database_url=url,
        health_interval_seconds=60,
        storage_nodes_json=[
            {"node_id": f"node-{i}", "host": "127.0.0.1", "port": 1, "failure_domain": domain}
            for i, domain in ((1, "A"), (2, "A"), (3, "B"))
        ],
    )
    monkeypatch.setattr("metadata.main.create_database", lambda _: (sessions.kw["bind"], sessions))
    # These are cached read APIs: keep lifespan real and freeze only health polling.
    monkeypatch.setattr("metadata.worker.MetadataWorker._poll", lambda self, config: None)
    app = create_app(settings)
    with TestClient(app) as client:
        with sessions.begin() as session:
            session.execute(update(StorageNode).values(status="ACTIVE"))
        yield client, app, sessions, settings


def add_file(session, *, status="AVAILABLE", rf=2, count=1, id=None, created_at=NOW):
    file = File(
        id=id or uuid4(),
        original_name="báo cáo.bin",
        content_type="application/test",
        size_bytes=count * 1048576,
        chunk_size_bytes=1048576,
        total_chunks=count,
        replication_factor=rf,
        checksum_sha256="a" * 64 if status == "AVAILABLE" else None,
        status=status,
        created_at=created_at,
        error_code="UPLOAD_INTERRUPTED" if status == "FAILED" else None,
    )
    session.add(file)
    session.flush()
    # Insert reverse order to ensure REST ordering is explicit.
    chunks = []
    for index in reversed(range(count)):
        chunk = Chunk(
            id=uuid4(),
            file_id=file.id,
            chunk_index=index,
            size_bytes=1048576,
            checksum_sha256="b" * 64,
        )
        session.add(chunk)
        chunks.insert(0, chunk)
    session.flush()
    return file, chunks


def replica(session, chunk, node, status="VERIFIED", cleanup=False):
    session.add(
        ChunkReplica(
            chunk_id=chunk.id,
            node_id=node,
            status=status,
            cleanup_pending=cleanup,
            last_verified_at=NOW if status == "VERIFIED" else None,
            last_error="previous failure" if status in {"CORRUPTED", "PENDING"} else None,
        )
    )


def get(client, path="", **kwargs):
    response = client.get(f"{PREFIX}{path}", **kwargs)
    assert response.status_code == 200, response.text
    return response.json()


def test_list_visibility_pagination_order_total_and_summary_schema(api):
    client, _, sessions, _ = api
    with sessions.begin() as session:
        low, _ = add_file(session, id=UUID(int=1))
        high, _ = add_file(session, id=UUID(int=2))
        newer, _ = add_file(session, created_at=NOW + timedelta(seconds=1))
        inactive = [add_file(session, status=s)[0] for s in ("UPLOADING", "FAILED", "DELETING")]
        add_file(session, status="DELETED")
    body = get(client)
    assert set(body) == {"items", "total", "limit", "offset"}
    assert (body["total"], body["limit"], body["offset"]) == (3, 50, 0)
    assert [row["file_id"] for row in body["items"]] == [str(newer.id), str(high.id), str(low.id)]
    assert all(set(row) == SUMMARY_KEYS for row in body["items"])
    assert body["items"][0]["created_at"] == "2026-10-04T03:00:01Z"
    page = get(client, params={"limit": 1, "offset": 1})
    assert page["total"] == 3 and page["items"][0]["file_id"] == str(high.id)
    assert get(client, params={"offset": 100})["items"] == []
    all_files = get(client, params={"include_inactive": "true"})
    assert all_files["total"] == 6
    assert {row["status"] for row in all_files["items"]} == {
        "AVAILABLE",
        "UPLOADING",
        "FAILED",
        "DELETING",
    }
    assert {str(f.id) for f in inactive} <= {row["file_id"] for row in all_files["items"]}
    assert all(
        row["checksum_sha256"] is None for row in all_files["items"] if row["status"] != "AVAILABLE"
    )


@pytest.mark.parametrize(
    "query", ["limit=0", "limit=101", "limit=x", "offset=-1", "offset=x", "include_inactive=x"]
)
def test_invalid_query_returns_validation_envelope(api, query):
    response = api[0].get(f"{PREFIX}?{query}")
    assert response.status_code == 422 and response.json()["error"]["code"] == "VALIDATION_ERROR"
    assert response.json()["error"]["details"]["fields"]


@pytest.mark.parametrize("suffix", ["", "/chunks"])
@pytest.mark.parametrize("kind", ["invalid", "missing", "deleted"])
def test_uuid_missing_and_tombstone_read_errors(api, suffix, kind):
    client, _, sessions, _ = api
    file_id = uuid4()
    if kind == "deleted":
        with sessions.begin() as session:
            add_file(session, id=file_id, status="DELETED")
    path = "invalid-uuid" if kind == "invalid" else str(file_id)
    response = client.get(f"{PREFIX}/{path}{suffix}")
    assert response.status_code == (422 if kind == "invalid" else 404)
    assert response.json()["error"]["code"] == (
        "VALIDATION_ERROR" if kind == "invalid" else "FILE_NOT_FOUND"
    )


def test_mixed_placement_retains_history_and_matches_detail_cluster_counts(api):
    client, _, sessions, settings = api
    with sessions.begin() as session:
        for number, enabled, status, domain in (
            (4, False, "ACTIVE", "C"),
            (5, True, "DOWN", "D"),
            (6, False, "DOWN", "E"),
        ):
            session.add(
                StorageNode(
                    node_id=f"node-{number}",
                    host="old",
                    port=1,
                    enabled=enabled,
                    status=status,
                    failure_domain=domain,
                )
            )
        session.flush()
        file, chunks = add_file(session, count=5)
        for node in ("node-2", "node-1"):
            replica(session, chunks[0], node)
        replica(session, chunks[1], "node-1")
        for node in ("node-3", "node-2", "node-1"):
            replica(session, chunks[3], node)
        for node, status, cleanup in (
            ("node-1", "PENDING", False),
            ("node-2", "CORRUPTED", False),
            ("node-3", "VERIFIED", True),
            ("node-4", "VERIFIED", False),
            ("node-5", "VERIFIED", False),
            ("node-6", "DELETED", False),
        ):
            replica(session, chunks[4], node, status, cleanup)
    settings.replication_factor = 3  # Existing file uses its RF=2 snapshot.
    detail = get(client, f"/{file.id}")
    expected = dict(zip(COUNTERS, (1, 2, 4, 1), strict=True))
    assert {key: detail[key] for key in COUNTERS} == expected
    assert not detail["known_readable"] and detail["cleanup_pending_replicas"] == 1
    assert detail["error_code"] is None and detail["replication_factor"] == 2
    assert set(detail) == SUMMARY_KEYS | set(COUNTERS) | {
        "known_readable",
        "cleanup_pending_replicas",
        "error_code",
    }
    placement = get(client, f"/{file.id}/chunks")
    assert set(placement) == {"file_id", "items"} and placement["file_id"] == str(file.id)
    items = placement["items"]
    assert [c["chunk_index"] for c in items] == list(range(5))
    assert [c["state"] for c in items] == [
        "AVAILABLE",
        "UNDER_REPLICATED",
        "UNAVAILABLE",
        "AVAILABLE",
        "UNAVAILABLE",
    ]
    assert [c["live_replica_count"] for c in items] == [2, 1, 0, 3, 0]
    assert [c["live_failure_domain_count"] for c in items] == [1, 1, 0, 2, 0]
    assert items[0]["domain_degraded"] and items[3]["over_replicated"]
    assert items[2]["replicas"] == []
    history = items[4]["replicas"]
    assert [r["node_id"] for r in history] == [f"node-{i}" for i in range(1, 7)]
    assert [r["status"] for r in history] == [
        "PENDING",
        "CORRUPTED",
        "VERIFIED",
        "VERIFIED",
        "VERIFIED",
        "DELETED",
    ]
    assert history[0]["last_verified_at"] is None and history[0]["last_error"] == "previous failure"
    assert history[2]["last_verified_at"] == "2026-10-04T03:00:00Z"
    assert history[4]["node_status"] == "DOWN" and history[5]["failure_domain"] == "E"
    assert set(history[0]) == {
        "node_id",
        "failure_domain",
        "node_status",
        "status",
        "cleanup_pending",
        "last_verified_at",
        "last_error",
    }
    cluster = client.get("/api/v1/cluster").json()
    assert {key: cluster[key] for key in COUNTERS} == expected
    assert cluster["cleanup_pending_replicas"] == detail["cleanup_pending_replicas"]


@pytest.mark.parametrize(
    "status,state", [("UPLOADING", "PENDING"), ("FAILED", "INACTIVE"), ("DELETING", "INACTIVE")]
)
def test_inactive_files_visible_with_counters_on_created_chunks(api, status, state):
    client, _, sessions, _ = api
    with sessions.begin() as session:
        file, chunks = add_file(session, status=status, count=2)
        replica(session, chunks[0], "node-1", cleanup=status != "UPLOADING")
    detail = get(client, f"/{file.id}")
    assert not detail["known_readable"] and detail["checksum_sha256"] is None
    assert detail["error_code"] == ("UPLOAD_INTERRUPTED" if status == "FAILED" else None)
    assert detail["unavailable_chunks"] == (2 if status != "UPLOADING" else 1)
    assert all(c["state"] == state for c in get(client, f"/{file.id}/chunks")["items"])
    assert client.get("/api/v1/cluster").json()["unavailable_chunks"] == 0


@pytest.mark.parametrize("status", ["AVAILABLE", "UPLOADING", "FAILED", "DELETING"])
def test_empty_file_views_and_known_readable(api, status):
    client, _, sessions, _ = api
    with sessions.begin() as session:
        session.execute(update(StorageNode).values(status="DOWN"))
        file, _ = add_file(session, status=status, count=0)
    detail = get(client, f"/{file.id}")
    assert detail["known_readable"] == (status == "AVAILABLE")
    assert all(detail[key] == 0 for key in COUNTERS)
    assert detail["cleanup_pending_replicas"] == 0
    assert get(client, f"/{file.id}/chunks") == {"file_id": str(file.id), "items": []}


@pytest.mark.parametrize(
    "status,cleanup,node_status,enabled,live",
    [
        ("VERIFIED", False, "ACTIVE", True, 1),
        ("PENDING", False, "ACTIVE", True, 0),
        ("MISSING", False, "ACTIVE", True, 0),
        ("CORRUPTED", False, "ACTIVE", True, 0),
        ("DELETED", False, "ACTIVE", True, 0),
        ("VERIFIED", True, "ACTIVE", True, 0),
        ("VERIFIED", False, "SUSPECTED", True, 0),
        ("VERIFIED", False, "DOWN", True, 0),
        ("VERIFIED", False, "ACTIVE", False, 0),
    ],
)
def test_live_eligibility_matches_file_and_cluster(
    api, status, cleanup, node_status, enabled, live
):
    client, _, sessions, _ = api
    with sessions.begin() as session:
        file, chunks = add_file(session, rf=3)
        node = session.get(StorageNode, "node-1")
        node.enabled, node.status = enabled, node_status
        replica(session, chunks[0], "node-1", status, cleanup)
    detail = get(client, f"/{file.id}")
    chunk = get(client, f"/{file.id}/chunks")["items"][0]
    assert chunk["live_replica_count"] == live
    assert chunk["state"] == ("UNDER_REPLICATED" if live else "UNAVAILABLE")
    assert detail["known_readable"] == bool(live)
    assert detail["under_replicated_chunks"] == live
    cluster = client.get("/api/v1/cluster").json()
    assert {key: detail[key] for key in COUNTERS} == {key: cluster[key] for key in COUNTERS}


@pytest.mark.parametrize("endpoint", ["list", "detail", "chunks"])
def test_reads_ignore_busy_lock_stopped_clients_and_never_rpc(api, monkeypatch, endpoint):
    client, app, sessions, _ = api
    with sessions.begin() as session:
        file, chunks = add_file(session)
        replica(session, chunks[0], "node-1")
    app.state.health_worker.stop()
    app.state.storage_client.close()

    def forbidden(*args, **kwargs):
        pytest.fail("Snapshot GET called operation lock or data RPC")

    class BusyLock:
        acquire = forbidden
        release = forbidden

        def locked(self):
            return True

    monkeypatch.setattr(app.state.storage_client, "store_chunk", forbidden)
    monkeypatch.setattr(app.state.storage_client, "get_chunk", forbidden)
    original = app.state.operation_lock
    app.state.operation_lock = BusyLock()
    try:
        path = {"list": "", "detail": f"/{file.id}", "chunks": f"/{file.id}/chunks"}[endpoint]
        assert get(client, path)
        assert client.get("/api/v1/health/ready").status_code == 503
    finally:
        app.state.operation_lock = original


@pytest.mark.parametrize("endpoint", ["list", "detail", "chunks"])
def test_uninitialized_and_database_outage_envelopes(api, monkeypatch, endpoint):
    client, app, sessions, _ = api
    with sessions.begin() as session:
        file, _ = add_file(session)
    path = {
        "list": PREFIX,
        "detail": f"{PREFIX}/{file.id}",
        "chunks": f"{PREFIX}/{file.id}/chunks",
    }[endpoint]
    engine = sessions.kw["bind"]
    touched = []

    def unavailable(*args):
        touched.append(True)
        raise SQLAlchemyError("private connection details")

    event.listen(engine, "before_cursor_execute", unavailable)
    try:
        app.state.initialized = False
        assert client.get(path).status_code == 503 and touched == []
        app.state.initialized = True
        response = client.get(path)
        assert response.status_code == 503
        assert response.json() == {
            "error": {
                "code": "METADATA_UNAVAILABLE",
                "message": "Metadata chưa sẵn sàng.",
                "details": {},
            }
        }
        assert "private" not in response.text
    finally:
        app.state.initialized = True
        event.remove(engine, "before_cursor_execute", unavailable)
    assert client.get(path).status_code == 200


@pytest.mark.parametrize("endpoint", ["list", "detail", "chunks"])
@pytest.mark.parametrize("count", [1, 256])
def test_query_count_is_bounded_and_transaction_is_read_only(api, endpoint, count):
    client, _, sessions, _ = api
    with sessions.begin() as session:
        file, chunks = add_file(session, count=count)
        for chunk in chunks:
            for node in ("node-1", "node-2", "node-3"):
                replica(session, chunk, node)
    statements = []
    engine = sessions.kw["bind"]

    def observe(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", observe)
    try:
        path = {"list": "", "detail": f"/{file.id}", "chunks": f"/{file.id}/chunks"}[endpoint]
        get(client, path)
    finally:
        event.remove(engine, "before_cursor_execute", observe)
    assert statements[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    assert len(statements) == {"list": 4, "detail": 5, "chunks": 4}[endpoint]
    assert all(s.startswith(("SET", "SELECT", "WITH")) for s in statements)
    if endpoint == "list":
        assert all("chunk_replicas" not in s for s in statements)


@pytest.mark.parametrize("endpoint", ["list", "detail", "chunks"])
def test_read_snapshot_does_not_mix_concurrent_commits(api, endpoint):
    client, _, sessions, _ = api
    with sessions.begin() as session:
        file, chunks = add_file(session)
        replica(session, chunks[0], "node-1")
    engine = sessions.kw["bind"]
    changed = False

    def concurrent_update(connection, cursor, statement, parameters, context, executemany):
        nonlocal changed
        if not changed and statement.startswith("SELECT"):
            changed = True
            with sessions.begin() as writer:
                writer.execute(update(StorageNode).values(status="DOWN"))
                add_file(writer)

    event.listen(engine, "after_cursor_execute", concurrent_update)
    try:
        path = {"list": "", "detail": f"/{file.id}", "chunks": f"/{file.id}/chunks"}[endpoint]
        before = get(client, path)
    finally:
        event.remove(engine, "after_cursor_execute", concurrent_update)
    after = get(client, path)
    if endpoint == "list":
        assert before["total"] == len(before["items"]) == 1
        assert after["total"] == len(after["items"]) == 2
    elif endpoint == "detail":
        assert before["known_readable"] and not after["known_readable"]
    else:
        assert before["items"][0]["live_replica_count"] == 1
        assert before["items"][0]["replicas"][0]["node_status"] == "ACTIVE"
        assert after["items"][0]["live_replica_count"] == 0
        assert after["items"][0]["replicas"][0]["node_status"] == "DOWN"


def test_get_views_observe_real_upload_and_continue_during_next_store(upload_cluster, monkeypatch):
    _, _, servers = upload_cluster
    app, client = start_http(upload_cluster)
    entered, release = Event(), Event()
    try:
        uploaded = client.post(PREFIX, files={"file": ("real.bin", b"x" * (256 * 1024 + 1))})
        assert uploaded.status_code == 201
        summary = uploaded.json()
        detail = get(client, f"/{summary['file_id']}")
        assert {key: detail[key] for key in SUMMARY_KEYS} == summary
        assert detail["known_readable"] and all(detail[key] == 0 for key in COUNTERS)
        chunks = get(client, f"/{summary['file_id']}/chunks")["items"]
        assert len(chunks) == 2 and all(c["live_replica_count"] == 2 for c in chunks)
        for server in servers:
            original = server.service.store.put

            def block(*args, original=original):
                entered.set()
                assert release.wait(4)
                return original(*args)

            monkeypatch.setattr(server.service.store, "put", block)
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(client.post, PREFIX, files={"file": ("pending", b"pending")})
            try:
                assert entered.wait(2) and app.state.operation_lock.locked()
                active = get(client)
                assert active["total"] == 1
                visible = get(client, params={"include_inactive": "true"})
                in_progress = next(f for f in visible["items"] if f["status"] == "UPLOADING")
                assert not get(client, f"/{in_progress['file_id']}")["known_readable"]
                placement = get(client, f"/{in_progress['file_id']}/chunks")["items"][0]
                assert placement["state"] == "PENDING"
                assert placement["replicas"][0]["status"] == "PENDING"
                assert get(client, f"/{summary['file_id']}")["known_readable"]
            finally:
                release.set()
            assert pending.result(timeout=4).status_code == 201
    finally:
        release.set()
        client.__exit__(None, None, None)
