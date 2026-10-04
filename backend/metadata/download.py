"""Verify known replicas and an assembled disk snapshot before any binary response."""

import hashlib
import logging
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError

from metadata.download_temp import DownloadTemp
from metadata.models import Chunk, ChunkReplica, File, StorageNode
from metadata.operations import OperationError, data_operation
from metadata.storage_client import StorageRpcError

logger = logging.getLogger(__name__)
HASH_PATTERN = re.compile(r"[0-9a-f]{64}")
NODE_PRIORITY = {"ACTIVE": 0, "SUSPECTED": 1, "DOWN": 2}


class DownloadError(OperationError):
    def __init__(self, status, code, message, file_id, chunk_index=None):
        super().__init__(status, code, message)
        self.details = {"file_id": str(file_id)}
        if chunk_index is not None:
            self.details["chunk_index"] = chunk_index


@dataclass(frozen=True)
class ReplicaSource:
    node_id: str
    node_status: str
    status: str


@dataclass
class ReadChunk:
    chunk_id: object
    index: int
    size_bytes: int
    checksum: str
    sources: list[ReplicaSource] = field(default_factory=list)


@dataclass(frozen=True)
class ReadFile:
    file_id: object
    original_name: str
    size_bytes: int
    chunk_size: int
    total_chunks: int
    checksum: str | None
    chunks: list[ReadChunk]


def integrity_error(file_id):
    return DownloadError(
        503, "INTEGRITY_CHECK_FAILED", "Metadata hoặc checksum file không khớp.", file_id
    )


def check_cancel(cancel, file_id, chunk_index=None):
    if cancel.is_set():
        raise DownloadError(503, "CHUNK_UNAVAILABLE", "Download đã bị hủy.", file_id, chunk_index)


def read_snapshot(operation, settings, file_id):
    configured = {node.node_id for node in settings.storage_nodes_json}
    with operation.transaction() as session:
        session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        file = session.get(File, file_id)
        if file is None or file.status == "DELETED":
            raise DownloadError(404, "FILE_NOT_FOUND", "Không tìm thấy file.", file_id)
        if file.status == "DELETING":
            raise DownloadError(409, "FILE_DELETING", "File đang được xóa.", file_id)
        if file.status != "AVAILABLE":
            raise DownloadError(409, "FILE_NOT_READY", "File chưa sẵn sàng để download.", file_id)
        statement = (
            select(Chunk, ChunkReplica, StorageNode.enabled, StorageNode.status)
            .outerjoin(ChunkReplica, ChunkReplica.chunk_id == Chunk.id)
            .outerjoin(StorageNode, StorageNode.node_id == ChunkReplica.node_id)
            .where(Chunk.file_id == file_id)
            .order_by(Chunk.chunk_index, ChunkReplica.node_id)
        )
        chunks = {}
        for chunk, replica, enabled, node_status in session.execute(statement):
            item = chunks.setdefault(
                chunk.id,
                ReadChunk(chunk.id, chunk.chunk_index, chunk.size_bytes, chunk.checksum_sha256),
            )
            if (
                replica is not None
                and enabled
                and replica.node_id in configured
                and replica.status != "DELETED"
                and not replica.cleanup_pending
            ):
                item.sources.append(ReplicaSource(replica.node_id, node_status, replica.status))
        snapshot = ReadFile(
            file.id,
            file.original_name,
            file.size_bytes,
            file.chunk_size_bytes,
            file.total_chunks,
            file.checksum_sha256,
            list(chunks.values()),
        )
    if (
        snapshot.checksum is None
        or not HASH_PATTERN.fullmatch(snapshot.checksum)
        or snapshot.total_chunks != len(snapshot.chunks)
        or snapshot.total_chunks
        != (snapshot.size_bytes + snapshot.chunk_size - 1) // snapshot.chunk_size
    ):
        raise integrity_error(file_id)
    for index, chunk in enumerate(snapshot.chunks):
        if (
            chunk.index != index
            or chunk.size_bytes
            != min(snapshot.chunk_size, snapshot.size_bytes - index * snapshot.chunk_size)
            or not HASH_PATTERN.fullmatch(chunk.checksum)
        ):
            raise integrity_error(file_id)
        chunk.sources.sort(
            key=lambda r: (NODE_PRIORITY[r.node_status], r.status != "VERIFIED", r.node_id)
        )
    return snapshot


def observe_replica(operation, chunk, source, error=None):
    with operation.transaction() as session:
        replica = session.get(ChunkReplica, (chunk.chunk_id, source.node_id))
        if error is None:
            replica.status = "VERIFIED"
            replica.last_verified_at = datetime.now(UTC)
            replica.last_error = None
        else:
            if error.replica_status is not None:
                replica.status = error.replica_status
            replica.last_error = error.reason


def read_chunk(operation, file, chunk, cancel):
    for source in chunk.sources:
        check_cancel(cancel, file.file_id, chunk.index)
        try:
            data = operation.get_chunk(
                source.node_id, str(chunk.chunk_id), chunk.size_bytes, chunk.checksum, cancel=cancel
            )
        except StorageRpcError as exc:
            observe_replica(operation, chunk, source, exc)
            if exc.cancelled:
                raise DownloadError(
                    503, "CHUNK_UNAVAILABLE", "Download đã bị hủy.", file.file_id, chunk.index
                ) from None
            continue
        observe_replica(operation, chunk, source)
        return data
    raise DownloadError(
        503,
        "CHUNK_UNAVAILABLE",
        "Không có replica đọc hợp lệ cho chunk.",
        file.file_id,
        chunk.index,
    )


def assemble(operation, settings, file_id, cancel):
    snapshot = read_snapshot(operation, settings, file_id)
    check_cancel(cancel, file_id)
    temp = DownloadTemp(settings.download_temp_dir)
    try:
        for chunk in snapshot.chunks:
            data = read_chunk(operation, snapshot, chunk, cancel)
            if temp.file.write(data) != len(data):
                raise OSError("Incomplete download temp write")
        # Verify the actual assembled disk bytes with bounded reads before 200.
        temp.file.flush()
        temp.file.seek(0)
        digest = hashlib.sha256()
        size = 0
        while True:
            check_cancel(cancel, file_id)
            block = temp.file.read(snapshot.chunk_size)
            if not block:
                break
            size += len(block)
            if size > snapshot.size_bytes:
                raise integrity_error(file_id)
            digest.update(block)
        if size != snapshot.size_bytes or digest.hexdigest() != snapshot.checksum:
            raise integrity_error(file_id)
        temp.file.seek(0)
        temp.original_name = snapshot.original_name
        temp.size_bytes = size
        temp.checksum_sha256 = digest.hexdigest()
        check_cancel(cancel, file_id)
        return temp
    except BaseException:
        temp.close()
        raise


def prepare_download(state, file_id, cancel):
    with data_operation(state) as operation:
        try:
            return assemble(operation, state.settings, file_id, cancel)
        except DownloadError:
            raise
        except SQLAlchemyError as exc:
            logger.warning("Download database failure (%s)", type(exc).__name__)
            raise DownloadError(
                503, "METADATA_UNAVAILABLE", "Metadata chưa sẵn sàng.", file_id
            ) from None
        except OSError:
            raise DownloadError(
                503, "TEMP_STORAGE_UNAVAILABLE", "Không thể dùng file tạm của download.", file_id
            ) from None
        except Exception as exc:
            logger.error("Download preparation failed (%s)", type(exc).__name__)
            raise DownloadError(
                500, "INTERNAL_ERROR", "Không thể chuẩn bị download.", file_id
            ) from None
