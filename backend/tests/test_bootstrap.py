import os
import uuid

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from metadata.bootstrap import initialize_metadata
from metadata.config import MetadataSettings
from metadata.db import Base
from metadata.models import Chunk, ChunkReplica, File, StorageNode


@pytest.fixture
def database():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("PostgreSQL integration: run the Compose tests service")
    schema = f"test_base_{uuid.uuid4().hex}"
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.execute(CreateSchema(schema))
    scoped_engine = engine.execution_options(schema_translate_map={None: schema})
    Base.metadata.create_all(scoped_engine)
    try:
        yield sessionmaker(bind=scoped_engine, expire_on_commit=False), url
    finally:
        # Only the unique schema created by this test is removed; app tables are untouched.
        with engine.begin() as connection:
            connection.execute(DropSchema(schema, cascade=True))
        engine.dispose()


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
    initialize_metadata(sessions, settings(url))
    with sessions() as session:
        file = session.get(File, uploading)
        assert file.status == "FAILED" and file.error_code == "UPLOAD_INTERRUPTED"
        assert session.get(File, deleting).status == "DELETING"
        assert session.get(File, available).status == "AVAILABLE"
        for chunk_id in (upload_chunk, delete_chunk):
            assert session.get(ChunkReplica, (chunk_id, "node-3")).cleanup_pending
        assert not session.get(ChunkReplica, (available_chunk, "node-3")).cleanup_pending
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
