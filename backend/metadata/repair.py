"""Probe known replicas and repair only from verified bytes, in bounded request scopes."""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import monotonic

from sqlalchemy import exists, func, select, tuple_
from sqlalchemy.exc import SQLAlchemyError

from metadata.models import Chunk, ChunkReplica, File, StorageNode
from metadata.operations import OperationError, data_operation
from metadata.placement import PlacementNode, select_destination
from metadata.replica_health import health_flags
from metadata.schemas import RepairChunkResult, RepairCursor, RepairResult
from metadata.storage_client import StorageRpcError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RepairChunk:
    file_id: object
    chunk_id: object
    index: int
    size: int
    checksum: str
    rf: int


@dataclass
class ChunkProgress:
    good: set = field(default_factory=set)
    repaired: int = 0
    node_id: str | None = None
    phase: str = "snapshot"


def check_cancel(cancel):
    if cancel.is_set():
        raise OperationError(503, "CHUNK_UNAVAILABLE", "Lượt repair đã bị hủy.")


def nodes_snapshot(operation, settings):
    configured = {node.node_id: node for node in settings.storage_nodes_json}
    with operation.transaction() as session:
        nodes = []
        for row in session.scalars(select(StorageNode)):
            config = configured.get(row.node_id)
            matches = config is not None and (row.host, row.port, row.failure_domain) == (
                config.host,
                config.port,
                config.failure_domain,
            )
            nodes.append(
                PlacementNode(
                    row.node_id,
                    row.failure_domain,
                    row.enabled and matches,
                    row.status,
                    row.available_bytes,
                    row.used_bytes,
                )
            )
    return {node.node_id: node for node in nodes}


def observe(operation, chunk, node_id, error=None):
    with operation.transaction() as session:
        replica = session.get(ChunkReplica, (chunk.chunk_id, node_id))
        if error is None:
            replica.status = "VERIFIED"
            replica.last_verified_at = datetime.now(UTC)
            replica.last_error = None
        else:
            if error.replica_status is not None:
                replica.status = error.replica_status
            replica.last_error = error.reason


def pending_attempt(operation, chunk, node_id):
    with operation.transaction() as session:
        replica = session.get(ChunkReplica, (chunk.chunk_id, node_id))
        if replica is None:
            session.add(
                ChunkReplica(
                    chunk_id=chunk.chunk_id,
                    node_id=node_id,
                    status="PENDING",
                    cleanup_pending=False,
                )
            )
        else:
            replica.status = "PENDING"
            replica.last_error = None


def result_for(chunk, outcome, good, nodes, message=None):
    live = [
        nodes[node_id]
        for node_id in good
        if nodes[node_id].enabled and nodes[node_id].status == "ACTIVE"
    ]
    domains = len({node.failure_domain for node in live})
    target = min(chunk.rf, len({node.failure_domain for node in nodes.values() if node.enabled}))
    flags = health_flags(len(live), domains, chunk.rf, target)
    return RepairChunkResult(
        file_id=chunk.file_id,
        chunk_id=chunk.chunk_id,
        chunk_index=chunk.index,
        outcome=outcome,
        live_replica_count=len(live),
        domain_degraded=flags["domain_degraded"],
        message=message,
    )


def repair_chunk(operation, settings, chunk, cancel, failed_nodes, allocated):
    progress = ChunkProgress()
    try:
        return _repair_chunk(operation, settings, chunk, cancel, failed_nodes, allocated, progress)
    except (SQLAlchemyError, OperationError):
        raise
    except Exception as exc:
        logger.warning(
            "Chunk repair failed file_id=%s chunk_id=%s node_id=%s phase=%s (%s)",
            chunk.file_id,
            chunk.chunk_id,
            progress.node_id,
            progress.phase,
            type(exc).__name__,
        )
        return result_for(
            chunk,
            "ERROR",
            progress.good,
            nodes_snapshot(operation, settings),
            "Lỗi khi xử lý chunk; có thể thử lại ở vòng scan mới.",
        ), progress.repaired


def _repair_chunk(operation, settings, chunk, cancel, failed_nodes, allocated, progress):
    check_cancel(cancel)
    nodes = nodes_snapshot(operation, settings)
    with operation.transaction() as session:
        replicas = session.scalars(
            select(ChunkReplica)
            .where(ChunkReplica.chunk_id == chunk.chunk_id)
            .order_by(ChunkReplica.node_id)
        ).all()
    good, corrupt = progress.good, set()
    source = None
    for replica in replicas:
        node = nodes[replica.node_id]
        if (
            not node.enabled
            or node.status == "DOWN"
            or replica.status == "DELETED"
            or replica.cleanup_pending
        ):
            continue
        check_cancel(cancel)
        progress.node_id, progress.phase = node.node_id, "probe"
        try:
            data = operation.get_chunk(
                node.node_id, str(chunk.chunk_id), chunk.size, chunk.checksum, cancel=cancel
            )
        except StorageRpcError as exc:
            observe(operation, chunk, node.node_id, exc)
            if exc.cancelled:
                raise OperationError(503, "CHUNK_UNAVAILABLE", "Lượt repair đã bị hủy.") from None
            if exc.replica_status == "CORRUPTED":
                corrupt.add(node.node_id)
            elif exc.replica_status != "MISSING":
                failed_nodes.add(node.node_id)
        else:
            observe(operation, chunk, node.node_id)
            good.add(node.node_id)
            if source is None:
                source = data
    nodes = nodes_snapshot(operation, settings)
    if source is None:
        return result_for(
            chunk, "UNAVAILABLE", good, nodes, "Không có source qua kiểm tra checksum."
        ), 0
    had_error = False
    while True:
        check_cancel(cancel)
        verified = [
            nodes[node_id]
            for node_id in good
            if nodes[node_id].enabled and nodes[node_id].status == "ACTIVE"
        ]
        if len(verified) >= chunk.rf:
            return result_for(
                chunk, "REPAIRED" if progress.repaired else "HEALTHY", good, nodes
            ), progress.repaired
        destination = select_destination(
            list(nodes.values()),
            chunk.size,
            chunk.rf,
            verified_replicas=verified,
            failed_node_ids=failed_nodes | good,
            allocated_bytes=allocated,
        )
        if destination is None:
            return result_for(
                chunk,
                "ERROR" if had_error else "NO_DESTINATION",
                good,
                nodes,
                "Chưa đủ RF; không còn destination dùng được trong lượt này.",
            ), progress.repaired
        node_id = destination.node_id
        progress.node_id, progress.phase = node_id, "pending"
        # This commit also protects the crash window between deleting corrupt bytes and Store.
        pending_attempt(operation, chunk, node_id)
        try:
            if node_id in corrupt:
                progress.phase = "delete_corrupt"
                operation.delete_chunk(node_id, str(chunk.chunk_id), cancel=cancel)
                check_cancel(cancel)
            progress.phase = "store"
            operation.store_chunk(
                node_id, str(chunk.chunk_id), source, chunk.checksum, cancel=cancel
            )
        except StorageRpcError as exc:
            observe(operation, chunk, node_id, exc)
            failed_nodes.add(node_id)
            had_error = True
            if exc.cancelled:
                raise OperationError(503, "CHUNK_UNAVAILABLE", "Lượt repair đã bị hủy.") from None
        else:
            progress.phase = "observe_ack"
            observe(operation, chunk, node_id)
            progress.repaired += 1
            good.add(node_id)
            allocated[node_id] = allocated.get(node_id, 0) + chunk.size
        nodes = nodes_snapshot(operation, settings)


def scan_scope(request):
    statement = select(Chunk).join(File).where(File.status == "AVAILABLE")
    if request.file_id is not None:
        statement = statement.where(Chunk.file_id == request.file_id)
    if request.node_id is not None:
        statement = statement.where(
            exists(
                select(ChunkReplica.chunk_id).where(
                    ChunkReplica.chunk_id == Chunk.id, ChunkReplica.node_id == request.node_id
                )
            )
        )
    return statement


def after_cursor(statement, cursor):
    if cursor is not None:
        statement = statement.where(
            tuple_(Chunk.file_id, Chunk.chunk_index) > (cursor.file_id, cursor.chunk_index)
        )
    return statement


def validate_scope(operation, settings, request):
    if request.max_chunks > settings.repair_max_chunks:
        raise OperationError(422, "VALIDATION_ERROR", "max_chunks vượt giới hạn repair cấu hình.")
    with operation.transaction() as session:
        if request.file_id is not None:
            file = session.get(File, request.file_id)
            if file is None:
                raise OperationError(404, "FILE_NOT_FOUND", "Không tìm thấy file.")
            if file.status != "AVAILABLE":
                raise OperationError(
                    409,
                    "FILE_DELETING" if file.status in {"DELETING", "DELETED"} else "FILE_NOT_READY",
                    "File không phục vụ repair.",
                )
        if request.node_id is not None and session.get(StorageNode, request.node_id) is None:
            raise OperationError(422, "VALIDATION_ERROR", "node_id chưa có trong registry.")


def repair_files(state, request, cancel):
    try:
        with data_operation(state) as operation:
            started = monotonic()
            validate_scope(operation, state.settings, request)
            # One bounded cleanup pass has priority under the SAME operation lock.
            state.health_worker.cleanup_pass.run(operation, state.settings, cancel)
            check_cancel(cancel)
            with operation.transaction() as session:
                selected = session.execute(
                    after_cursor(scan_scope(request), request.after)
                    .with_only_columns(Chunk, File.replication_factor)
                    .order_by(Chunk.file_id, Chunk.chunk_index)
                    .limit(request.max_chunks)
                ).all()
                chunks = [
                    RepairChunk(
                        row.file_id,
                        row.id,
                        row.chunk_index,
                        row.size_bytes,
                        row.checksum_sha256,
                        rf,
                    )
                    for row, rf in selected
                ]
            results, repaired = [], 0
            cursor = request.after
            failed_nodes, allocated = set(), {}
            for chunk in chunks:
                check_cancel(cancel)
                if monotonic() - started >= state.settings.repair_time_budget_seconds:
                    break
                result, count = repair_chunk(
                    operation, state.settings, chunk, cancel, failed_nodes, allocated
                )
                results.append(result)
                repaired += count
                cursor = RepairCursor(file_id=chunk.file_id, chunk_index=chunk.index)
            with operation.transaction() as session:
                remaining = session.scalar(
                    select(func.count()).select_from(
                        after_cursor(scan_scope(request), cursor).subquery()
                    )
                )
            return RepairResult(
                checked_chunks=len(results),
                repaired_replicas=repaired,
                remaining_chunks=remaining,
                next_after=cursor if remaining else None,
                results=results,
            )
    except SQLAlchemyError as exc:
        logger.warning("Repair DB unavailable (%s)", type(exc).__name__)
        raise OperationError(503, "METADATA_UNAVAILABLE", "Metadata chưa sẵn sàng.") from None
