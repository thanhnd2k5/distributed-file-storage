"""V2 wire adapter. Domain operations live in ledger/streaming modules."""

import errno
import shutil
import stat
from functools import wraps

import grpc
import v2_storage_pb2 as pb
import v2_storage_pb2_grpc as rpc

from storage.v2.ledger import ConflictError, MissingError, PreconditionError, Scope, Spec
from storage.v2.streaming import BusyError, DataError, InactiveError


def fail(context, exc):
    code = grpc.StatusCode.INTERNAL
    message = "Storage operation failed; inspect mutation outcome"
    for kind, status in (
        (ValueError, grpc.StatusCode.INVALID_ARGUMENT),
        (PreconditionError, grpc.StatusCode.FAILED_PRECONDITION),
        (ConflictError, grpc.StatusCode.ALREADY_EXISTS),
        (MissingError, grpc.StatusCode.NOT_FOUND),
        (BusyError, grpc.StatusCode.ABORTED),
        (DataError, grpc.StatusCode.DATA_LOSS),
        (InactiveError, grpc.StatusCode.CANCELLED),
    ):
        if isinstance(exc, kind):
            code, message = status, str(exc)
            break
    if isinstance(exc, OSError) and exc.errno in (errno.ENOSPC, errno.EDQUOT):
        code, message = grpc.StatusCode.RESOURCE_EXHAUSTED, "Local disk capacity exhausted"
    context.abort(code, message)


def unary(function):
    @wraps(function)
    def call(self, request, context):
        try:
            self.deadline(context)
            return function(self, request, context)
        except Exception as exc:
            fail(context, exc)

    return call


def scope_of(scope):
    return Scope(scope.namespace_id, scope.recovery_epoch, scope.node_id, scope.node_incarnation)


def spec_of(spec):
    return Spec(spec.chunk_id, spec.size_bytes, spec.checksum_sha256)


def observed_bytes(paths):
    total = 0
    for path in paths:
        try:
            observed = path.stat()
        except FileNotFoundError:
            # Store/delete and SQLite checkpoint may remove an entry after the
            # directory scan. Health counters are observations, not reservations.
            continue
        if stat.S_ISREG(observed.st_mode):
            total += observed.st_size
    return total


class StorageService(rpc.StorageServiceServicer):
    def __init__(self, store, failure_domain):
        self.store = store
        self.ledger = store.ledger
        self.failure_domain = failure_domain

    @staticmethod
    def deadline(context):
        remaining = context.time_remaining()
        if remaining is None or remaining > 120:
            raise ValueError("RPC requires deadline no longer than 120 seconds")
        if remaining <= 0 or not context.is_active():
            raise InactiveError("RPC expired")

    def record(self, row):
        scope = self.ledger.scope
        result = pb.AllocationRecord(
            key=pb.AllocationKey(
                scope=pb.NodeScope(
                    namespace_id=scope.namespace_id,
                    recovery_epoch=row["epoch"],
                    node_id=scope.node_id,
                    node_incarnation=scope.node_incarnation,
                ),
                allocation_id=row["allocation_id"],
            ),
            generation=row["generation"],
            state=pb.AllocationState.Value(row["state"]),
            inventory_sequence=row["inventory_sequence"],
            cleanup_complete=bool(row["cleanup_complete"]),
        )
        if row["chunk_id"] is not None:
            result.chunk.CopyFrom(
                pb.ChunkSpec(
                    chunk_id=row["chunk_id"],
                    size_bytes=row["size_bytes"],
                    checksum_sha256=row["checksum_sha256"],
                )
            )
        if row["actual_size"] is not None:
            result.actual_size_bytes = row["actual_size"]
        if row["actual_hash"] is not None:
            result.actual_checksum_sha256 = row["actual_hash"]
        return result

    @unary
    def PrepareAllocation(self, request, context):
        return self.record(
            self.store.prepare(
                scope_of(request.key.scope),
                request.key.allocation_id,
                request.generation,
                spec_of(request.chunk),
            )
        )

    @unary
    def InspectAllocation(self, request, context):
        return self.record(
            self.ledger.inspect(
                scope_of(request.key.scope), request.key.allocation_id, request.generation
            )
        )

    @unary
    def DeleteAllocation(self, request, context):
        return self.record(
            self.store.delete(
                scope_of(request.key.scope), request.key.allocation_id, request.generation
            )
        )

    @unary
    def StoreChunk(self, requests, context):
        try:
            first = next(requests)
        except StopIteration as exc:
            raise ValueError("Missing stream header") from exc
        if first.WhichOneof("payload") != "header":
            raise ValueError("Stream must start with header")
        header = first.header

        def blocks():
            for frame in requests:
                if frame.WhichOneof("payload") != "block":
                    raise ValueError("Only data blocks may follow header")
                yield frame.block.offset, frame.block.data

        row, replay = self.store.store(
            scope_of(header.key.scope),
            header.key.allocation_id,
            header.generation,
            spec_of(header.chunk),
            blocks(),
            context.is_active,
        )
        return pb.StoreChunkResponse(allocation=self.record(row), already_stored=replay)

    def GetChunk(self, request, context):
        try:
            self.deadline(context)
            scope = scope_of(request.key.scope)
            self.ledger.require_scope(scope)
            expected = spec_of(request.expected_chunk)
            expected.validate(self.ledger.chunk_limit)
            with self.store.reader(request.key.allocation_id) as owner:
                with owner.commit:
                    row = self.ledger.lookup(request.key.allocation_id)
                    if row["epoch"] != scope.recovery_epoch:
                        raise PreconditionError("Allocation requires explicit epoch adoption")
                    if row["state"] != "STORED":
                        raise PreconditionError("Allocation not readable")
                    if (row["chunk_id"], row["size_bytes"], row["checksum_sha256"]) != (
                        expected.chunk_id,
                        expected.size_bytes,
                        expected.checksum_sha256,
                    ):
                        raise ConflictError("Expected spec differs")
                    if not self.ledger.verify_final(row):
                        with self.ledger.transaction() as db:
                            db.execute(
                                "UPDATE allocations SET state='CORRUPT' WHERE allocation_id=?",
                                (row["allocation_id"],),
                            )
                        raise DataError("Stored bytes missing/corrupt")
                    handle = (self.ledger.root / "chunks" / row["allocation_id"]).open("rb")
                with handle:
                    import hashlib

                    digest = hashlib.sha256()
                    count = 0
                    yield pb.GetChunkFrame(header=self.record(row))
                    while block := handle.read(262144):
                        if not context.is_active():
                            raise InactiveError("Reader cancelled")
                        self.ledger.require_scope(scope)
                        digest.update(block)
                        yield pb.GetChunkFrame(block=pb.DataBlock(offset=count, data=block))
                        count += len(block)
                    if (
                        count != expected.size_bytes
                        or digest.hexdigest() != expected.checksum_sha256
                    ):
                        raise DataError("Read bytes changed during stream")
                    yield pb.GetChunkFrame(
                        footer=pb.GetChunkFooter(
                            actual_size_bytes=count, actual_checksum_sha256=digest.hexdigest()
                        )
                    )
        except Exception as exc:
            fail(context, exc)

    @unary
    def ListAllocations(self, request, context):
        rows, upper, more = self.ledger.inventory(
            scope_of(request.scope),
            request.after_sequence,
            request.upper_sequence if request.HasField("upper_sequence") else None,
            request.limit,
        )
        return pb.ListAllocationsResponse(
            items=[self.record(row) for row in rows],
            upper_sequence=upper,
            next_after_sequence=rows[-1]["inventory_sequence"] if rows else request.after_sequence,
            has_more=more,
        )

    @unary
    def HealthCheck(self, request, context):
        self.ledger.require_scope(scope_of(request.scope))
        usage = shutil.disk_usage(self.ledger.root)
        with self.ledger._mutex:
            committed = self.ledger._db.execute(
                "SELECT COALESCE(SUM(actual_size),0) FROM allocations WHERE state='STORED'"
            ).fetchone()[0]
        quarantine_dir = self.ledger.root / "quarantine"
        has_quarantined = quarantine_dir.exists() and any(quarantine_dir.iterdir())
        return pb.HealthCheckResponse(
            scope=request.scope,
            failure_domain=self.failure_domain,
            writable=self.ledger.writable(self.ledger.chunk_limit),
            recovery_required=has_quarantined,
            filesystem_capacity_bytes=usage.total,
            filesystem_available_bytes=usage.free,
            committed_bytes=committed,
            temporary_bytes=observed_bytes((self.ledger.root / "tmp").iterdir()),
            ledger_bytes=observed_bytes(self.ledger.root.glob("ledger.sqlite3*")),
        )
