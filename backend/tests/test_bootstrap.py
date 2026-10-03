from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from metadata.bootstrap import initialize_metadata
from metadata.config import MetadataSettings
from metadata.main import create_app
from metadata.models import Chunk, ChunkReplica, File, StorageNode


def settings(url, node_ids=(1, 2, 3)):
    return MetadataSettings(
        _env_file=None,
        database_url=url,
        replication_factor=1,
        storage_nodes_json=[
            {"node_id": f"node-{i}", "host": "storage", "port": 50050 + i, "failure_domain": "test"}
            for i in node_ids
        ],
    )


def add_file(session, state, replica_state="PENDING"):
    file = File(
        original_name="test.bin",
        content_type="application/octet-stream",
        size_bytes=1,
        chunk_size_bytes=2097152,
        total_chunks=1,
        replication_factor=2,
        status=state,
    )
    session.add(file)
    session.flush()
    chunk = Chunk(file_id=file.id, chunk_index=0, size_bytes=1, checksum_sha256="a" * 64)
    session.add(chunk)
    session.flush()
    replica = ChunkReplica(
        chunk_id=chunk.id, node_id="node-3", status=replica_state, cleanup_pending=False
    )
    session.add(replica)
    session.flush()
    return file.id, chunk.id


def test_restart_preserves_attempted_replicas_and_pending_cleanup(database):
    sessions, url = database
    initialize_metadata(sessions, settings(url))
    with sessions.begin() as session:
        uploading, upload_chunk = add_file(session, "UPLOADING")
        deleting, delete_chunk = add_file(session, "DELETING", "VERIFIED")
        available, available_chunk = add_file(session, "AVAILABLE")
        failed, failed_chunk = add_file(session, "FAILED", "VERIFIED")
        deleted, deleted_chunk = add_file(session, "DELETED", "DELETED")
    initialize_metadata(sessions, settings(url))
    with sessions() as session:
        file = session.get(File, uploading)
        assert file.status == "FAILED" and file.error_code == "UPLOAD_INTERRUPTED"
        assert session.get(File, deleting).status == "DELETING"
        assert session.get(File, available).status == "AVAILABLE"
        assert session.get(File, failed).status == "FAILED"
        assert session.get(File, deleted).status == "DELETED"
        for chunk_id in (upload_chunk, delete_chunk, failed_chunk):
            assert session.get(ChunkReplica, (chunk_id, "node-3")).cleanup_pending
        assert not session.get(ChunkReplica, (available_chunk, "node-3")).cleanup_pending
        assert session.get(ChunkReplica, (available_chunk, "node-3")).status == "PENDING"
        replica = session.get(ChunkReplica, (deleted_chunk, "node-3"))
        assert replica.status == "DELETED" and not replica.cleanup_pending
        for chunk_id in (upload_chunk, delete_chunk, available_chunk, failed_chunk, deleted_chunk):
            assert session.get(Chunk, chunk_id).checksum_sha256 == "a" * 64
        assert len(session.scalars(select(StorageNode)).all()) == 3


def test_registry_removal_keeps_mapping_and_domain_change_rolls_back(database):
    sessions, url = database
    initialize_metadata(sessions, settings(url))
    with sessions.begin() as session:
        _, chunk_id = add_file(session, "AVAILABLE", "VERIFIED")
        session.get(StorageNode, "node-3").status = "ACTIVE"
    initialize_metadata(sessions, settings(url, node_ids=(1, 2)))
    with sessions() as session:
        node = session.get(StorageNode, "node-3")
        assert not node.enabled and node.status == "DOWN"
        assert session.get(ChunkReplica, (chunk_id, "node-3")).status == "VERIFIED"
    changed = settings(url)
    changed.storage_nodes_json[2].failure_domain = "new-host"
    with pytest.raises(ValueError, match="Cannot change failure_domain"):
        initialize_metadata(sessions, changed)
    with sessions() as session:
        assert session.get(StorageNode, "node-3").failure_domain == "test"


HEALTH_TIMESTAMP = datetime(2026, 10, 3, 1, 0, tzinfo=UTC)


def seed_health(node, status="ACTIVE"):
    node.status = status
    node.last_success_at = HEALTH_TIMESTAMP
    node.capacity_bytes = 1000
    node.available_bytes = 900
    node.used_bytes = 1
    node.last_error = "previous health failure"


def assert_health_history(node):
    assert node.last_success_at == HEALTH_TIMESTAMP
    assert (node.capacity_bytes, node.available_bytes, node.used_bytes) == (1000, 900, 1)


def test_registry_initialization_is_idempotent_with_unknown_health(database):
    sessions, url = database
    for _ in range(2):
        initialize_metadata(sessions, settings(url))
        with sessions() as session:
            nodes = session.scalars(select(StorageNode).order_by(StorageNode.node_id)).all()
            assert [node.node_id for node in nodes] == ["node-1", "node-2", "node-3"]
            for index, node in enumerate(nodes, start=1):
                assert (node.host, node.port, node.failure_domain) == (
                    "storage",
                    50050 + index,
                    "test",
                )
                assert node.enabled and node.status == "DOWN"
                assert node.last_success_at is None and node.last_error is None
                assert node.capacity_bytes is None
                assert node.available_bytes is None
                assert node.used_bytes is None


@pytest.mark.parametrize("previous_status", ["ACTIVE", "SUSPECTED", "DOWN"])
def test_startup_resets_status_but_retains_health_history(database, previous_status):
    sessions, url = database
    initialize_metadata(sessions, settings(url))
    with sessions.begin() as session:
        seed_health(session.get(StorageNode, "node-1"), previous_status)
    initialize_metadata(sessions, settings(url))
    with sessions() as session:
        node = session.get(StorageNode, "node-1")
        assert node.enabled and node.status == "DOWN" and node.last_error is None
        assert_health_history(node)


@pytest.mark.parametrize(
    ("field", "value"), [("host", "new-storage"), ("port", 51001), ("failure_domain", "new-host")]
)
def test_endpoint_or_unreferenced_domain_change_invalidates_health(database, field, value):
    sessions, url = database
    initialize_metadata(sessions, settings(url))
    with sessions.begin() as session:
        seed_health(session.get(StorageNode, "node-1"))
        seed_health(session.get(StorageNode, "node-2"))
        _, chunk_id = add_file(session, "AVAILABLE", "VERIFIED")
    changed = settings(url)
    setattr(changed.storage_nodes_json[0], field, value)
    initialize_metadata(sessions, changed)
    with sessions() as session:
        node = session.get(StorageNode, "node-1")
        assert getattr(node, field) == value
        assert node.enabled and node.status == "DOWN" and node.last_error is None
        assert node.last_success_at is None
        assert (node.capacity_bytes, node.available_bytes, node.used_bytes) == (None, None, None)
        assert_health_history(session.get(StorageNode, "node-2"))
        assert session.get(ChunkReplica, (chunk_id, "node-3")).status == "VERIFIED"


def test_registry_reenable_retains_history_and_replica_mapping(database):
    sessions, url = database
    initialize_metadata(sessions, settings(url))
    with sessions.begin() as session:
        seed_health(session.get(StorageNode, "node-3"))
        file_id, chunk_id = add_file(session, "AVAILABLE", "VERIFIED")
        replica = session.get(ChunkReplica, (chunk_id, "node-3"))
        replica.last_verified_at = HEALTH_TIMESTAMP
        replica.last_error = "retained replica observation"
    for node_ids, enabled in (((1, 2), False), ((1, 2, 3), True)):
        initialize_metadata(sessions, settings(url, node_ids))
        with sessions() as session:
            node = session.get(StorageNode, "node-3")
            assert node.enabled is enabled and node.status == "DOWN"
            assert_health_history(node)
            replica = session.get(ChunkReplica, (chunk_id, "node-3"))
            assert replica.status == "VERIFIED" and not replica.cleanup_pending
            assert replica.last_verified_at == HEALTH_TIMESTAMP
            assert replica.last_error == "retained replica observation"
            assert session.get(File, file_id).status == "AVAILABLE"


@pytest.mark.parametrize("replica_status", ["PENDING", "VERIFIED", "DELETED"])
def test_forbidden_domain_change_rolls_back_entire_registry_transaction(database, replica_status):
    sessions, url = database
    initialize_metadata(sessions, settings(url))
    with sessions.begin() as session:
        for node in session.scalars(select(StorageNode)):
            seed_health(node)
        file_id, chunk_id = add_file(session, "UPLOADING", replica_status)
    changed = settings(url, node_ids=(1, 4, 3))
    changed.storage_nodes_json[0].host = "changed-before-failure"
    changed.storage_nodes_json[2].failure_domain = "forbidden-domain"
    with pytest.raises(ValueError, match="Cannot change failure_domain for node node-3"):
        initialize_metadata(sessions, changed)
    with sessions() as session:
        assert session.get(StorageNode, "node-4") is None
        for node in session.scalars(select(StorageNode)):
            assert node.enabled and node.status == "ACTIVE"
            assert node.host == "storage" and node.failure_domain == "test"
            assert node.last_error == "previous health failure"
            assert_health_history(node)
        assert session.get(File, file_id).status == "UPLOADING"
        replica = session.get(ChunkReplica, (chunk_id, "node-3"))
        assert replica.status == replica_status and not replica.cleanup_pending


@pytest.mark.parametrize("startup_failure", [False, True])
def test_app_startup_holds_operation_lock_and_gates_readiness(
    database, monkeypatch, startup_failure
):
    sessions, url = database
    initialize_metadata(sessions, settings(url))
    with sessions.begin() as session:
        seed_health(session.get(StorageNode, "node-3"))
        file_id, chunk_id = add_file(session, "UPLOADING")
    app_settings = settings(url)
    if startup_failure:
        app_settings.storage_nodes_json[2].failure_domain = "forbidden-domain"
    monkeypatch.setattr("metadata.main.create_database", lambda _: (sessions.kw["bind"], sessions))
    app = create_app(app_settings)
    lock_observations = []

    def observe_initialize(session_factory, config):
        lock_observations.append(app.state.operation_lock.locked())
        assert not app.state.initialized
        initialize_metadata(session_factory, config)

    monkeypatch.setattr("metadata.main.initialize_metadata", observe_initialize)
    assert not app.state.initialized
    with TestClient(app) as client:
        assert lock_observations == [True]
        assert not app.state.operation_lock.locked()
        assert app.state.initialized is not startup_failure
        if startup_failure:
            assert app.state.health_worker is None
        assert client.get("/api/v1/health/live").json() == {"status": "LIVE"}
        ready = client.get("/api/v1/health/ready")
        if startup_failure:
            assert ready.status_code == 503
            assert ready.json()["error"]["code"] == "METADATA_UNAVAILABLE"
        else:
            assert ready.status_code == 200 and ready.json() == {"status": "READY"}
        with sessions() as session:
            node = session.get(StorageNode, "node-3")
            assert node.status == ("ACTIVE" if startup_failure else "DOWN")
            assert_health_history(node)
            assert session.get(File, file_id).status == (
                "UPLOADING" if startup_failure else "FAILED"
            )
            assert (
                session.get(ChunkReplica, (chunk_id, "node-3")).cleanup_pending
                is not startup_failure
            )
    assert not app.state.initialized and not app.state.operation_lock.locked()
