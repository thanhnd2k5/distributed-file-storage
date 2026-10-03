from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta, timezone
from threading import Event
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete, event
from sqlalchemy.exc import OperationalError

from metadata.bootstrap import initialize_metadata
from metadata.config import MetadataSettings
from metadata.main import create_app
from metadata.models import Chunk, ChunkReplica, File, StorageNode

PREFIX = "/api/v1"
TIMESTAMP = datetime(2026, 10, 3, 15, 30, tzinfo=timezone(timedelta(hours=7)))
NODE_KEYS = {
    "node_id",
    "host",
    "port",
    "failure_domain",
    "enabled",
    "status",
    "last_success_at",
    "last_error",
    "capacity_bytes",
    "available_bytes",
    "used_bytes",
}
CLUSTER_KEYS = {
    "chunk_size_bytes",
    "default_replication_factor",
    "max_file_size_bytes",
    "nodes",
    "configured_failure_domains",
    "active_failure_domains",
    "files_available",
    "under_replicated_chunks",
    "unavailable_chunks",
    "domain_degraded_chunks",
    "over_replicated_chunks",
    "cleanup_pending_replicas",
    "operation_busy",
}
CHUNK_KEYS = (
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
        replication_factor=2,
        chunk_size_bytes=1048576,
        max_file_size_bytes=32 * 1048576,
        health_interval_seconds=60,
        health_rpc_timeout_seconds=0.05,
        storage_nodes_json=[
            {"node_id": f"node-{i}", "host": "127.0.0.1", "port": 1, "failure_domain": domain}
            for i, domain in ((1, "A"), (2, "A"), (3, "B"))
        ],
    )
    monkeypatch.setattr("metadata.main.create_database", lambda _: (sessions.kw["bind"], sessions))
    # Keep lifecycle/readiness real; P3 owns RPC polling, so freeze only poll work.
    monkeypatch.setattr("metadata.worker.MetadataWorker._poll", lambda self, config: None)
    app = create_app(settings)
    with TestClient(app) as client:
        initialize_metadata(sessions, settings)
        yield client, app, sessions, settings


def seed_node(session, node_id, status="ACTIVE", enabled=True):
    node = session.get(StorageNode, node_id)
    node.enabled = enabled
    node.status = status
    node.last_success_at = TIMESTAMP
    node.last_error = None if status == "ACTIVE" else "UNAVAILABLE"
    node.capacity_bytes, node.available_bytes, node.used_bytes = 1000, 900, 10
    return node


def add_file(session, status="AVAILABLE", rf=2, chunks=1):
    file = File(
        original_name="fixture.bin",
        content_type="application/octet-stream",
        size_bytes=chunks * 1048576,
        chunk_size_bytes=1048576,
        total_chunks=chunks,
        replication_factor=rf,
        status=status,
        checksum_sha256="f" * 64 if status == "AVAILABLE" else None,
    )
    session.add(file)
    session.flush()
    ids = []
    for index in range(chunks):
        chunk = Chunk(
            file_id=file.id, chunk_index=index, size_bytes=1048576, checksum_sha256="a" * 64
        )
        session.add(chunk)
        session.flush()
        ids.append(chunk.id)
    return ids


def add_replica(session, chunk_id, node_id, status="VERIFIED", cleanup_pending=False):
    session.add(
        ChunkReplica(
            chunk_id=chunk_id, node_id=node_id, status=status, cleanup_pending=cleanup_pending
        )
    )


def get_cluster(client):
    response = client.get(f"{PREFIX}/cluster")
    assert response.status_code == 200
    data = response.json()
    assert set(data) == CLUSTER_KEYS
    assert set(data["nodes"]) == {"active", "suspected", "down", "disabled"}
    assert type(data["operation_busy"]) is bool
    for key, value in data.items():
        if key not in {"nodes", "operation_busy"}:
            assert type(value) is int and value >= 0
    return data


def test_nodes_contract_unknown_metrics_and_utc_history_sorted_with_disabled(api):
    client, _, sessions, _ = api
    with sessions.begin() as session:
        session.add(
            StorageNode(
                node_id="node-0",
                host="removed",
                port=1234,
                failure_domain="old",
                enabled=False,
                status="DOWN",
                last_success_at=TIMESTAMP,
                last_error="previous failure",
                capacity_bytes=1000,
                available_bytes=900,
                used_bytes=10,
            )
        )
        seed_node(session, "node-2", "SUSPECTED")
        unknown = session.get(StorageNode, "node-1")
        unknown.capacity_bytes = unknown.available_bytes = unknown.used_bytes = 0
    response = client.get(f"{PREFIX}/nodes")
    assert response.status_code == 200
    data = response.json()
    assert set(data) == {"items"}
    assert [node["node_id"] for node in data["items"]] == ["node-0", "node-1", "node-2", "node-3"]
    for node in data["items"]:
        assert set(node) == NODE_KEYS and type(node["enabled"]) is bool
        assert type(node["port"]) is int
    old, unknown, suspected, never_seen = data["items"]
    assert old == {
        "node_id": "node-0",
        "host": "removed",
        "port": 1234,
        "failure_domain": "old",
        "enabled": False,
        "status": "DOWN",
        "last_success_at": "2026-10-03T08:30:00Z",
        "last_error": "previous failure",
        "capacity_bytes": 1000,
        "available_bytes": 900,
        "used_bytes": 10,
    }
    assert suspected["status"] == "SUSPECTED" and suspected["last_error"] == "UNAVAILABLE"
    assert datetime.fromisoformat(suspected["last_success_at"]).tzinfo == UTC
    for node in (unknown, never_seen):
        assert node["last_success_at"] is None and node["status"] == "DOWN"
        assert all(node[key] is None for key in ("capacity_bytes", "available_bytes", "used_bytes"))


def test_successful_zero_metrics_remain_zero(api):
    client, _, sessions, _ = api
    with sessions.begin() as session:
        node = seed_node(session, "node-1")
        node.capacity_bytes = node.available_bytes = node.used_bytes = 0
    node = client.get(f"{PREFIX}/nodes").json()["items"][0]
    assert (node["capacity_bytes"], node["available_bytes"], node["used_bytes"]) == (0, 0, 0)


def test_empty_cluster_contract_configuration_and_all_down_ready(api):
    client, _, _, settings = api
    data = get_cluster(client)
    assert data == {
        "chunk_size_bytes": settings.chunk_size_bytes,
        "default_replication_factor": 2,
        "max_file_size_bytes": 32 * 1048576,
        "nodes": {"active": 0, "suspected": 0, "down": 3, "disabled": 0},
        "configured_failure_domains": 2,
        "active_failure_domains": 0,
        "files_available": 0,
        "under_replicated_chunks": 0,
        "unavailable_chunks": 0,
        "domain_degraded_chunks": 0,
        "over_replicated_chunks": 0,
        "cleanup_pending_replicas": 0,
        "operation_busy": False,
    }
    assert client.get(f"{PREFIX}/health/ready").json() == {"status": "READY"}


def test_no_registry_rows_returns_zero_aggregates(api):
    client, _, sessions, _ = api
    with sessions.begin() as session:
        session.execute(delete(StorageNode))
    data = get_cluster(client)
    assert all(value == 0 for value in data["nodes"].values())
    assert data["configured_failure_domains"] == data["active_failure_domains"] == 0
    assert all(data[key] == 0 for key in CHUNK_KEYS)
    assert client.get(f"{PREFIX}/nodes").json() == {"items": []}


@pytest.mark.parametrize("disabled_status", ["ACTIVE", "SUSPECTED", "DOWN"])
def test_node_counts_exclude_disabled_regardless_of_cached_status(api, disabled_status):
    client, _, sessions, _ = api
    with sessions.begin() as session:
        seed_node(session, "node-1", "ACTIVE")
        seed_node(session, "node-2", "SUSPECTED")
        seed_node(session, "node-3", disabled_status, enabled=False)
        session.add(
            StorageNode(node_id="node-4", host="other", port=1, failure_domain="C", status="DOWN")
        )
    data = get_cluster(client)
    assert data["nodes"] == {"active": 1, "suspected": 1, "down": 1, "disabled": 1}
    assert data["configured_failure_domains"] == 2 and data["active_failure_domains"] == 1


@pytest.mark.parametrize(
    "replica_status, cleanup, node_status, enabled, live",
    [
        ("VERIFIED", False, "ACTIVE", True, True),
        ("PENDING", False, "ACTIVE", True, False),
        ("MISSING", False, "ACTIVE", True, False),
        ("CORRUPTED", False, "ACTIVE", True, False),
        ("DELETED", False, "ACTIVE", True, False),
        ("VERIFIED", True, "ACTIVE", True, False),
        ("VERIFIED", False, "SUSPECTED", True, False),
        ("VERIFIED", False, "DOWN", True, False),
        ("VERIFIED", False, "ACTIVE", False, False),
    ],
)
def test_live_replica_eligibility_uses_file_rf(
    api, replica_status, cleanup, node_status, enabled, live
):
    client, _, sessions, _ = api
    with sessions.begin() as session:
        seed_node(session, "node-1", node_status, enabled)
        chunk_id = add_file(session, rf=1)[0]
        add_replica(session, chunk_id, "node-1", replica_status, cleanup)
    data = get_cluster(client)
    assert data["files_available"] == 1
    assert data["under_replicated_chunks"] == data["over_replicated_chunks"] == 0
    assert data["unavailable_chunks"] == data["domain_degraded_chunks"] == int(not live)
    assert data["cleanup_pending_replicas"] == int(cleanup)


def test_mixed_chunk_counters_use_per_file_rf_and_overlap_domain_degradation(api):
    client, _, sessions, _ = api
    with sessions.begin() as session:
        for node_id in ("node-1", "node-2", "node-3"):
            seed_node(session, node_id)
        placements = [
            (),
            ("node-1",),
            ("node-1", "node-2"),
            ("node-1", "node-3"),
            ("node-1", "node-2", "node-3"),
        ]
        for placement in placements:
            chunk_id = add_file(session, rf=2)[0]
            for node_id in placement:
                add_replica(session, chunk_id, node_id)
        for rf in (1, 3):
            chunk_id = add_file(session, rf=rf)[0]
            for node_id in ("node-1", "node-3"):
                add_replica(session, chunk_id, node_id)
        add_file(session, chunks=0)
    data = get_cluster(client)
    assert data["files_available"] == 8
    assert {key: data[key] for key in CHUNK_KEYS} == {
        "under_replicated_chunks": 2,
        "unavailable_chunks": 1,
        "domain_degraded_chunks": 3,
        "over_replicated_chunks": 2,
    }
    assert data["active_failure_domains"] == 2 and data["cleanup_pending_replicas"] == 0


@pytest.mark.parametrize("status", ["UPLOADING", "FAILED", "DELETING", "DELETED"])
def test_inactive_files_excluded_from_chunk_counters_but_unfinished_cleanup_counted(api, status):
    client, _, sessions, _ = api
    pending = status != "DELETED"
    with sessions.begin() as session:
        chunk_id = add_file(session, status=status)[0]
        add_replica(session, chunk_id, "node-1", "PENDING" if pending else "DELETED", pending)
    data = get_cluster(client)
    assert data["files_available"] == 0 and all(data[key] == 0 for key in CHUNK_KEYS)
    assert data["cleanup_pending_replicas"] == int(pending)


@pytest.mark.parametrize("third_enabled, expected_domains, degraded", [(True, 2, 1), (False, 1, 0)])
def test_domain_target_includes_enabled_down_domains_and_excludes_disabled_domains(
    api, third_enabled, expected_domains, degraded
):
    client, _, sessions, _ = api
    with sessions.begin() as session:
        seed_node(session, "node-1")
        seed_node(session, "node-2")
        seed_node(session, "node-3", "DOWN", third_enabled)
        chunk_id = add_file(session, rf=2)[0]
        add_replica(session, chunk_id, "node-1")
        add_replica(session, chunk_id, "node-2")
    data = get_cluster(client)
    assert data["configured_failure_domains"] == expected_domains
    assert data["active_failure_domains"] == 1
    assert data["domain_degraded_chunks"] == degraded
    assert data["under_replicated_chunks"] == data["unavailable_chunks"] == 0


def test_gets_do_not_acquire_busy_lock_or_call_health_rpc(api, monkeypatch):
    client, app, _, _ = api

    def forbid_rpc(*args, **kwargs):
        raise AssertionError("GET must only read persisted snapshots")

    monkeypatch.setattr("metadata.worker.grpc.insecure_channel", forbid_rpc)
    for stub in app.state.health_worker._stubs.values():
        monkeypatch.setattr(stub, "HealthCheck", forbid_rpc)
    with ThreadPoolExecutor(max_workers=1) as executor:
        with app.state.operation_lock:
            nodes = executor.submit(client.get, f"{PREFIX}/nodes").result(timeout=2)
            data = executor.submit(get_cluster, client).result(timeout=2)
            assert nodes.status_code == 200 and data["operation_busy"] is True
    assert get_cluster(client)["operation_busy"] is False


@pytest.mark.parametrize("endpoint", ["nodes", "cluster"])
def test_db_outage_returns_503_without_leaking_details_and_session_recovers(api, endpoint, caplog):
    client, _, sessions, _ = api
    engine = sessions.kw["bind"]

    def fail_query(*args):
        raise OperationalError("private SQL", {}, Exception("password=secret"))

    event.listen(engine, "before_cursor_execute", fail_query)
    try:
        response = client.get(f"{PREFIX}/{endpoint}")
        assert response.status_code == 503
        assert response.json() == {
            "error": {
                "code": "METADATA_UNAVAILABLE",
                "message": "Metadata chưa sẵn sàng.",
                "details": {},
            }
        }
        assert "secret" not in response.text and "private SQL" not in caplog.text
        assert "password=secret" not in caplog.text
        assert client.get(f"{PREFIX}/health/live").json() == {"status": "LIVE"}
        assert client.get(f"{PREFIX}/health/ready").status_code == 503
        assert engine.pool.checkedout() == 0
    finally:
        event.remove(engine, "before_cursor_execute", fail_query)
    assert client.get(f"{PREFIX}/{endpoint}").status_code == 200


@pytest.mark.parametrize("endpoint", ["nodes", "cluster"])
def test_uninitialized_returns_503_before_accessing_database(api, endpoint, monkeypatch):
    client, app, _, _ = api

    def forbid_db():
        raise AssertionError("Initialization must gate database access")

    app.state.initialized = False
    monkeypatch.setattr(app.state, "session_factory", SimpleNamespace(begin=forbid_db))
    response = client.get(f"{PREFIX}/{endpoint}")
    assert (
        response.status_code == 503 and response.json()["error"]["code"] == "METADATA_UNAVAILABLE"
    )
    assert client.get(f"{PREFIX}/health/live").status_code == 200
    assert client.get(f"{PREFIX}/health/ready").status_code == 503


@pytest.mark.parametrize("chunks", [1, 40])
def test_summary_query_count_is_constant_with_chunk_count(api, chunks):
    client, _, sessions, _ = api
    with sessions.begin() as session:
        add_file(session, chunks=chunks)
    statements = []
    engine = sessions.kw["bind"]

    def record(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", record)
    try:
        data = get_cluster(client)
        assert data["unavailable_chunks"] == chunks
        assert len(statements) == 1
        assert client.get(f"{PREFIX}/nodes").status_code == 200
        assert len(statements) == 2
    finally:
        event.remove(engine, "before_cursor_execute", record)


def test_summary_aggregates_share_one_snapshot_during_concurrent_commit(api):
    client, _, sessions, _ = api
    engine = sessions.kw["bind"]
    mutated = Event()

    def concurrent_commit(connection, cursor, statement, parameters, context, executemany):
        if "node_counts AS" in statement and not mutated.is_set():
            mutated.set()
            with sessions.begin() as other:
                other.get(StorageNode, "node-3").enabled = False
                add_file(other, chunks=0)

    event.listen(engine, "after_cursor_execute", concurrent_commit)
    try:
        before = get_cluster(client)
        assert mutated.is_set()
        assert before["files_available"] == 0 and before["configured_failure_domains"] == 2
        assert before["nodes"] == {"active": 0, "suspected": 0, "down": 3, "disabled": 0}
        after = get_cluster(client)
        assert after["files_available"] == 1 and after["configured_failure_domains"] == 1
        assert after["nodes"] == {"active": 0, "suspected": 0, "down": 2, "disabled": 1}
    finally:
        event.remove(engine, "after_cursor_execute", concurrent_commit)
