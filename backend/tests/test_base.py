import hashlib
import uuid
from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest
import storage_pb2
import storage_pb2_grpc
from fastapi.testclient import TestClient
from pydantic import ValidationError

from metadata.config import MetadataSettings
from metadata.main import create_app
from storage.config import StorageSettings
from storage.service import StorageService


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


def test_grpc_health_identity_and_unimplemented_data_rpc(tmp_path):
    settings = StorageSettings(
        _env_file=None,
        node_id="test-node",
        failure_domain="test-host",
        data_dir=tmp_path,
        grpc_bind_host="127.0.0.1",
        grpc_port=50061,
    )
    server = grpc.server(ThreadPoolExecutor(max_workers=4), options=settings.grpc_options())
    storage_pb2_grpc.add_StorageServiceServicer_to_server(StorageService(settings), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}", options=settings.grpc_options()) as channel:
            grpc.channel_ready_future(channel).result(timeout=5)
            stub = storage_pb2_grpc.StorageServiceStub(channel)
            response = stub.HealthCheck(storage_pb2.HealthCheckRequest(), timeout=2)
            assert response.node_id == "test-node"
            assert response.failure_domain == "test-host"
            assert response.storage_writable and response.capacity_bytes > 0
            assert list(tmp_path.iterdir()) == []
            with pytest.raises(grpc.RpcError) as error:
                stub.GetChunk(storage_pb2.GetChunkRequest(chunk_id=str(uuid.uuid4())), timeout=2)
            assert error.value.code() == grpc.StatusCode.UNIMPLEMENTED
    finally:
        server.stop(0).wait()


def test_metadata_db_down_still_live_but_not_ready():
    with TestClient(create_app(metadata_settings())) as client:
        assert client.get("/api/v1/health/live").json() == {"status": "LIVE"}
        ready = client.get("/api/v1/health/ready")
        assert ready.status_code == 503
        assert ready.json()["error"]["code"] == "METADATA_UNAVAILABLE"
