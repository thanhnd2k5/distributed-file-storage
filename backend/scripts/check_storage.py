"""Container health check: verify configured identity and an actual writable probe."""

import grpc
import storage_pb2
import storage_pb2_grpc

from storage.config import StorageSettings

settings = StorageSettings()
with grpc.insecure_channel(
    f"127.0.0.1:{settings.grpc_port}", options=settings.grpc_options()
) as channel:
    response = storage_pb2_grpc.StorageServiceStub(channel).HealthCheck(
        storage_pb2.HealthCheckRequest(), timeout=1
    )
    if (
        response.node_id != settings.node_id
        or response.failure_domain != settings.failure_domain
        or not response.storage_writable
    ):
        raise SystemExit("Storage health/identity check failed")
