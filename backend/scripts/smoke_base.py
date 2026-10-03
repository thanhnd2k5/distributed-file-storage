"""Run inside Metadata's container to verify HTTP, schema, registry and three RPC nodes."""

import json
import urllib.request

import grpc
import storage_pb2
import storage_pb2_grpc
from sqlalchemy import inspect, select

from metadata.config import MetadataSettings
from metadata.db import create_database
from metadata.models import StorageNode


def main():
    settings = MetadataSettings()
    for endpoint, expected in (("live", "LIVE"), ("ready", "READY")):
        with urllib.request.urlopen(
            f"http://127.0.0.1:8000/api/v1/health/{endpoint}", timeout=5
        ) as response:
            assert json.load(response) == {"status": expected}
    engine, sessions = create_database(settings.database_url)
    try:
        assert {"files", "chunks", "chunk_replicas", "storage_nodes"} <= set(
            inspect(engine).get_table_names()
        )
        with sessions() as session:
            nodes = session.scalars(select(StorageNode).where(StorageNode.enabled)).all()
            assert len(nodes) == len(settings.storage_nodes_json)
        for node in settings.storage_nodes_json:
            with grpc.insecure_channel(
                f"{node.host}:{node.port}", options=settings.grpc_options()
            ) as channel:
                result = storage_pb2_grpc.StorageServiceStub(channel).HealthCheck(
                    storage_pb2.HealthCheckRequest(), timeout=2
                )
                assert result.node_id == node.node_id
                assert result.failure_domain == node.failure_domain
                assert result.storage_writable
                print(f"{result.node_id}: RPC identity/domain/writable OK")
    finally:
        engine.dispose()
    print("Base smoke passed: live/ready, four DB tables, registry and three Storage Nodes")


if __name__ == "__main__":
    main()
