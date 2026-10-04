import errno
import logging
import os
import shutil
import tempfile

import grpc
import storage_pb2
import storage_pb2_grpc

from storage.chunk_store import ChunkConflictError, ChunkDataError, ChunkStore, InactiveStoreError

logger = logging.getLogger(__name__)


class StorageService(storage_pb2_grpc.StorageServiceServicer):
    """Store/Get/Delete operate on immutable local chunks under a shared mutex."""

    def __init__(self, settings):
        self.settings = settings
        self.store = ChunkStore(settings.data_dir, settings.chunk_size_bytes)

    def StoreChunk(self, request, context):
        try:
            result = self.store.put(
                request.chunk_id,
                request.data,
                request.checksum_sha256,
                context.is_active,
            )
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except ChunkConflictError:
            context.abort(
                grpc.StatusCode.ALREADY_EXISTS, "Chunk ID already contains different bytes"
            )
        except InactiveStoreError:
            context.abort(grpc.StatusCode.CANCELLED, "Store request is no longer active")
        except OSError as exc:
            logger.exception("Store filesystem operation failed")
            if exc.errno in {errno.ENOSPC, errno.EDQUOT}:
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "Storage capacity exhausted")
            if exc.errno in {errno.EACCES, errno.EPERM, errno.EROFS, errno.ENOENT, errno.ENOTDIR}:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Storage directory unavailable")
            context.abort(grpc.StatusCode.INTERNAL, "Storage operation failed")
        except Exception:
            logger.exception("Unexpected Store failure")
            context.abort(grpc.StatusCode.INTERNAL, "Storage operation failed")
        return storage_pb2.StoreChunkResponse(
            chunk_id=result.chunk_id,
            size_bytes=result.size_bytes,
            checksum_sha256=result.checksum_sha256,
            already_existed=result.already_existed,
        )

    def GetChunk(self, request, context):
        try:
            result = self.store.get(request.chunk_id)
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except FileNotFoundError:
            context.abort(grpc.StatusCode.NOT_FOUND, "Chunk not found")
        except (ChunkDataError, OSError):
            logger.exception("Get could not read local chunk data")
            context.abort(grpc.StatusCode.DATA_LOSS, "Local chunk could not be read completely")
        except Exception:
            logger.exception("Unexpected Get failure")
            context.abort(grpc.StatusCode.INTERNAL, "Storage operation failed")
        return storage_pb2.GetChunkResponse(
            chunk_id=result.chunk_id, data=result.data, checksum_sha256=result.checksum_sha256
        )

    def DeleteChunk(self, request, context):
        try:
            existed = self.store.delete(request.chunk_id, context.is_active)
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except InactiveStoreError:
            context.abort(grpc.StatusCode.CANCELLED, "Delete request is no longer active")
        except OSError as exc:
            logger.exception("Delete filesystem operation failed")
            if exc.errno in {errno.EACCES, errno.EPERM, errno.EROFS, errno.ENOTDIR}:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Storage directory unavailable")
            context.abort(grpc.StatusCode.INTERNAL, "Storage operation failed")
        except Exception:
            logger.exception("Unexpected Delete failure")
            context.abort(grpc.StatusCode.INTERNAL, "Storage operation failed")
        return storage_pb2.DeleteChunkResponse(chunk_id=request.chunk_id, existed=existed)

    def HealthCheck(self, request, context):
        writable = True
        try:
            with tempfile.TemporaryFile(dir=self.store.data_dir) as probe:
                if probe.write(b"health") != len(b"health"):
                    raise OSError(errno.EIO, "Incomplete health probe write")
                probe.flush()
                os.fsync(probe.fileno())
        except OSError:
            writable = False
        except Exception:
            logger.exception("Unexpected health probe failure")
            context.abort(grpc.StatusCode.INTERNAL, "Storage operation failed")
        try:
            capacity = shutil.disk_usage(self.store.data_dir)
        except OSError:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Storage filesystem unavailable")
        except Exception:
            logger.exception("Unexpected capacity query failure")
            context.abort(grpc.StatusCode.INTERNAL, "Storage operation failed")
        return storage_pb2.HealthCheckResponse(
            node_id=self.settings.node_id,
            failure_domain=self.settings.failure_domain,
            storage_writable=writable,
            capacity_bytes=capacity.total,
            available_bytes=capacity.free,
            used_bytes=self.store.used_bytes,
        )
