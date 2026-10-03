import hashlib
import uuid

import grpc
import pytest
import storage_pb2
from fastapi.testclient import TestClient
from pydantic import ValidationError

from metadata.config import MetadataSettings
from metadata.main import create_app


def metadata_settings(**overrides):
    values = dict(
        _env_file=None,
        database_url="postgresql+psycopg://dfs:dfs@127.0.0.1:1/dfs",
        storage_nodes_json=[
            {
                "node_id": f"node-{i}",
                "host": "127.0.0.1",
                "port": 50050 + i,
                "failure_domain": "test",
            }
            for i in (1, 2, 3)
        ],
    )
    return MetadataSettings(**(values | overrides))


def test_config_rejects_duplicate_nodes_and_impossible_rf():
    nodes = metadata_settings().storage_nodes_json
    with pytest.raises(ValidationError, match="duplicate node_id"):
        metadata_settings(storage_nodes_json=[nodes[0], nodes[0]])
    with pytest.raises(ValidationError, match="exceeds configured"):
        metadata_settings(replication_factor=4)


def test_message_limit_must_fit_payload():
    with pytest.raises(ValidationError, match="must exceed"):
        metadata_settings(grpc_max_message_bytes=2 * 1024 * 1024)


def test_proto_two_mib_roundtrip_and_unary_contract():
    data = b"x" * (2 * 1024 * 1024)
    message = storage_pb2.StoreChunkRequest(
        chunk_id=str(uuid.uuid4()), data=data, checksum_sha256=hashlib.sha256(data).hexdigest()
    )
    assert len(message.SerializeToString()) < metadata_settings().grpc_max_message_bytes
    assert storage_pb2.StoreChunkRequest.FromString(message.SerializeToString()) == message
    methods = storage_pb2.DESCRIPTOR.services_by_name["StorageService"].methods
    assert {m.name for m in methods} == {"StoreChunk", "GetChunk", "DeleteChunk", "HealthCheck"}
    assert all(not m.client_streaming and not m.server_streaming for m in methods)


def test_grpc_health_identity_and_missing_chunk(storage_node_factory):
    stub, _, directory = storage_node_factory("test-node", "test-host")
    response = stub.HealthCheck(storage_pb2.HealthCheckRequest(), timeout=2)
    assert response.node_id == "test-node"
    assert response.failure_domain == "test-host"
    assert response.storage_writable and response.capacity_bytes > 0
    assert list(directory.iterdir()) == []
    with pytest.raises(grpc.RpcError) as error:
        stub.GetChunk(storage_pb2.GetChunkRequest(chunk_id=str(uuid.uuid4())), timeout=2)
    assert error.value.code() == grpc.StatusCode.NOT_FOUND


def test_metadata_db_down_still_live_but_not_ready():
    with TestClient(create_app(metadata_settings())) as client:
        assert client.get("/api/v1/health/live").json() == {"status": "LIVE"}
        ready = client.get("/api/v1/health/ready")
        assert ready.status_code == 503
        assert ready.json()["error"]["code"] == "METADATA_UNAVAILABLE"
