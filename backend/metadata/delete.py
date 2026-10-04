"""Commit a tombstone before bounded cleanup; never discard replica history."""

import logging
from datetime import UTC, datetime

from sqlalchemy import func, select, update
from sqlalchemy.exc import SQLAlchemyError

from metadata.cleanup import CleanupPass
from metadata.models import Chunk, ChunkReplica, File
from metadata.operations import OperationError, data_operation
from metadata.schemas import DeleteResult

logger = logging.getLogger(__name__)


def delete_result(session, file_id):
    file = session.get(File, file_id)
    pending = session.scalar(
        select(func.count())
        .select_from(ChunkReplica)
        .join(Chunk, Chunk.id == ChunkReplica.chunk_id)
        .where(Chunk.file_id == file_id, ChunkReplica.cleanup_pending)
    )
    return DeleteResult(file_id=file.id, status=file.status, cleanup_pending_replicas=pending)


def delete_file(state, file_id, cancel):
    try:
        with data_operation(state) as operation:
            with operation.transaction() as session:
                file = session.get(File, file_id)
                if file is None:
                    raise OperationError(404, "FILE_NOT_FOUND", "Không tìm thấy file.")
                if file.status == "UPLOADING":
                    raise OperationError(409, "FILE_NOT_READY", "File chưa sẵn sàng để xóa.")
                if file.status in ("DELETING", "DELETED"):
                    return delete_result(session, file_id)
                file.status = "DELETING"
                file.deleted_at = datetime.now(UTC)
                session.execute(
                    update(ChunkReplica)
                    .where(
                        ChunkReplica.chunk_id.in_(select(Chunk.id).where(Chunk.file_id == file_id)),
                        ChunkReplica.status != "DELETED",
                    )
                    .values(cleanup_pending=True)
                )
            CleanupPass().run(operation, state.settings, cancel, file_id=file_id)
            with operation.transaction() as session:
                return delete_result(session, file_id)
    except SQLAlchemyError as exc:
        logger.warning("File delete failed (%s); persisted cleanup will resume", type(exc).__name__)
        raise OperationError(503, "METADATA_UNAVAILABLE", "Metadata chưa sẵn sàng.") from None
