import os
import shutil
import tempfile

import grpc
import storage_pb2
import storage_pb2_grpc

from storage.chunk_store import ChunkStore
from storage.validation import validate_chunk_id, validate_store


class StorageService(storage_pb2_grpc.StorageServiceServicer):
    """P1: validate data RPC input; successful transfers arrive in P2/P3."""

    def __init__(self, settings):
        self.settings = settings
        self.store = ChunkStore(settings.data_dir, settings.chunk_size_bytes)

    def StoreChunk(self, request, context):
        try:
            validate_store(
                request.chunk_id,
                request.data,
                request.checksum_sha256,
                self.settings.chunk_size_bytes,
            )
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "StoreChunk commit is planned for M1 P2")

    def GetChunk(self, request, context):
        try:
            validate_chunk_id(request.chunk_id)
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "GetChunk read is planned for M1 P3")

    def DeleteChunk(self, request, context):
        try:
            validate_chunk_id(request.chunk_id)
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "DeleteChunk unlink is planned for M1 P3")

    def HealthCheck(self, request, context):
        writable = True
        try:
            with tempfile.TemporaryFile(dir=self.store.data_dir) as probe:
                probe.write(b"health")
                probe.flush()
                os.fsync(probe.fileno())
        except OSError:
            writable = False
        try:
            capacity = shutil.disk_usage(self.store.data_dir)
        except OSError:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Storage filesystem unavailable")
        return storage_pb2.HealthCheckResponse(
            node_id=self.settings.node_id,
            failure_domain=self.settings.failure_domain,
            storage_writable=writable,
            capacity_bytes=capacity.total,
            available_bytes=capacity.free,
            used_bytes=self.store.used_bytes,
        )
