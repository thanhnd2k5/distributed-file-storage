"""Multipart spool to durable, acknowledged replicas; one transfer thread owns the scope."""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import select, update
from sqlalchemy.exc import SQLAlchemyError

from metadata.file_input import ChunkReader, FileInputError, prepare_upload
from metadata.models import Chunk, ChunkReplica, File, StorageNode
from metadata.operations import OperationError, data_operation
from metadata.placement import PlacementNode, select_destination
from metadata.schemas import FileSummary
from metadata.storage_client import StorageRpcError

logger = logging.getLogger(__name__)


@dataclass
class UploadAttempt:
    file_id: UUID = field(default_factory=uuid4)
    creation_started: bool = False


class UploadError(FileInputError):
    def __init__(self, status_code, code, message, *, failure_code=None):
        super().__init__(status_code, code, message)
        self.failure_code = failure_code or code
        self.file_id = None


def check_cancel(cancel):
    if cancel.is_set():
        raise UploadError(
            503,
            "UPLOAD_REPLICATION_FAILED",
            "Upload đã bị hủy trước khi lưu đủ bản sao.",
            failure_code="UPLOAD_INTERRUPTED",
        )


def node_snapshots(operation):
    with operation.transaction() as session:
        return [
            PlacementNode(
                row.node_id,
                row.failure_domain,
                row.enabled,
                row.status,
                row.available_bytes,
                row.used_bytes,
            )
            for row in session.scalars(select(StorageNode))
        ]


def mark_failed(operation, file_id, code):
    with operation.transaction() as session:
        changed = session.execute(
            update(File)
            .where(File.id == file_id, File.status == "UPLOADING")
            .values(status="FAILED", error_code=code)
        ).rowcount
        if changed:
            session.execute(
                update(ChunkReplica)
                .where(
                    ChunkReplica.chunk_id.in_(select(Chunk.id).where(Chunk.file_id == file_id)),
                    ChunkReplica.status != "DELETED",
                )
                .values(cleanup_pending=True)
            )
        # A final commit can succeed even if its acknowledgement is lost. Never
        # change AVAILABLE in response to a later exception or disconnect.
        return session.get(File, file_id) is not None


def commit_available(operation, file_id, reader, expected, cancel):
    check_cancel(cancel)
    digest = reader.summary
    if digest is None:
        raise RuntimeError("Upload input did not reach verified EOF")
    with operation.transaction() as session:
        file = session.scalar(select(File).where(File.id == file_id).with_for_update())
        chunks = session.execute(
            select(Chunk.id, Chunk.chunk_index, Chunk.size_bytes, Chunk.checksum_sha256)
            .where(Chunk.file_id == file_id)
            .order_by(Chunk.chunk_index)
        ).all()
        verified = session.execute(
            select(ChunkReplica.chunk_id, ChunkReplica.node_id)
            .join(Chunk)
            .where(Chunk.file_id == file_id, ChunkReplica.status == "VERIFIED")
        ).all()
        acknowledged = {}
        for chunk_id, node_id in verified:
            acknowledged.setdefault(chunk_id, set()).add(node_id)
        if (
            file.status != "UPLOADING"
            or chunks != expected
            or digest.size_bytes != file.size_bytes
            or digest.total_chunks != file.total_chunks
            or any(len(acknowledged.get(row.id, ())) < file.replication_factor for row in chunks)
        ):
            raise RuntimeError("Upload commit invariants failed")
        check_cancel(cancel)
        file.checksum_sha256 = digest.checksum_sha256
        file.status = "AVAILABLE"
        session.flush()
        summary = FileSummary(
            file_id=file.id,
            original_name=file.original_name,
            content_type=file.content_type,
            size_bytes=file.size_bytes,
            chunk_size_bytes=file.chunk_size_bytes,
            total_chunks=file.total_chunks,
            replication_factor=file.replication_factor,
            checksum_sha256=file.checksum_sha256,
            status=file.status,
            created_at=file.created_at,
        )
    return summary


def replicate(operation, settings, upload, attempt, cancel):
    check_cancel(cancel)
    file_id = attempt.file_id
    chunk_size, rf = settings.chunk_size_bytes, settings.replication_factor
    nodes = node_snapshots(operation)
    first_size = min(chunk_size, upload.size_bytes)
    if (
        first_size
        and sum(
            node.enabled
            and node.status == "ACTIVE"
            and node.available_bytes is not None
            and node.available_bytes >= first_size
            and node.used_bytes is not None
            and node.used_bytes >= 0
            for node in nodes
        )
        < rf
    ):
        raise UploadError(
            503, "INSUFFICIENT_NODES", "Không đủ node sẵn sàng cho replication factor."
        )
    attempt.creation_started = True
    with operation.transaction() as session:
        session.add(
            File(
                id=file_id,
                original_name=upload.original_name,
                content_type=upload.content_type,
                size_bytes=upload.size_bytes,
                chunk_size_bytes=chunk_size,
                total_chunks=(upload.size_bytes + chunk_size - 1) // chunk_size,
                replication_factor=rf,
                status="UPLOADING",
            )
        )
    reader = ChunkReader(upload.stream, chunk_size, upload.size_bytes)
    failed_nodes, allocated, expected = set(), {}, []
    for chunk in reader:
        check_cancel(cancel)
        chunk_id = uuid4()
        with operation.transaction() as session:
            session.add(
                Chunk(
                    id=chunk_id,
                    file_id=file_id,
                    chunk_index=chunk.chunk_index,
                    size_bytes=chunk.size_bytes,
                    checksum_sha256=chunk.checksum_sha256,
                )
            )
        expected.append((chunk_id, chunk.chunk_index, chunk.size_bytes, chunk.checksum_sha256))
        verified = []
        while len(verified) < rf:
            check_cancel(cancel)
            destination = select_destination(
                node_snapshots(operation),
                chunk.size_bytes,
                rf,
                verified_replicas=verified,
                failed_node_ids=failed_nodes,
                allocated_bytes=allocated,
            )
            if destination is None:
                raise UploadError(
                    503, "UPLOAD_REPLICATION_FAILED", "Không thể lưu đủ bản sao cho mọi chunk."
                )
            node_id = destination.node_id
            with operation.transaction() as session:
                session.add(ChunkReplica(chunk_id=chunk_id, node_id=node_id, status="PENDING"))
            check_cancel(cancel)
            try:
                operation.store_chunk(
                    node_id, str(chunk_id), chunk.data, chunk.checksum_sha256, cancel=cancel
                )
            except StorageRpcError as exc:
                failed_nodes.add(node_id)
                with operation.transaction() as session:
                    session.get(ChunkReplica, (chunk_id, node_id)).last_error = exc.reason
                if exc.cancelled:
                    raise UploadError(
                        503,
                        "UPLOAD_REPLICATION_FAILED",
                        "Upload đã bị hủy trước khi lưu đủ bản sao.",
                        failure_code="UPLOAD_INTERRUPTED",
                    ) from None
                continue
            with operation.transaction() as session:
                replica = session.get(ChunkReplica, (chunk_id, node_id))
                replica.status = "VERIFIED"
                replica.last_verified_at = datetime.now(UTC)
                replica.last_error = None
            verified.append(destination)
            allocated[node_id] = allocated.get(node_id, 0) + chunk.size_bytes
    return commit_available(operation, file_id, reader, expected, cancel)


def upload_file(state, parts, cancel):
    with prepare_upload(parts, state.settings.max_file_size_bytes) as upload:
        with data_operation(state) as operation:
            attempt = UploadAttempt()
            try:
                return replicate(operation, state.settings, upload, attempt, cancel)
            except SQLAlchemyError as exc:
                logger.warning("Upload database failure (%s)", type(exc).__name__)
                error = UploadError(503, "METADATA_UNAVAILABLE", "Metadata chưa sẵn sàng.")
            except (FileInputError, OperationError) as exc:
                error = (
                    exc
                    if isinstance(exc, UploadError)
                    else UploadError(exc.status_code, exc.code, str(exc))
                )
            except Exception as exc:
                logger.error("Upload failed (%s)", type(exc).__name__)
                error = UploadError(500, "INTERNAL_ERROR", "Không thể xử lý upload.")
            if attempt.creation_started:
                try:
                    if mark_failed(operation, attempt.file_id, error.failure_code):
                        error.file_id = attempt.file_id
                except SQLAlchemyError as exc:
                    logger.warning("Cannot persist failed upload (%s)", type(exc).__name__)
                    error = UploadError(503, "METADATA_UNAVAILABLE", "Metadata chưa sẵn sàng.")
                    # The creation commit may have succeeded without acknowledgement.
                    error.file_id = attempt.file_id
            raise error
