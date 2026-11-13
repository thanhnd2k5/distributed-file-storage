"""Worker-only V2 transport. One channel/call per operation; no unbounded cache."""

import hashlib
from threading import Event
from uuid import UUID

import grpc
import v2_storage_pb2 as pb
import v2_storage_pb2_grpc as rpc

from metadata.v2.health import HealthObservation
from metadata.v2.repository import (
    AllocationEvidence,
    DeleteEvidence,
    DomainError,
    InventoryItem,
    InventoryPage,
)


class RepairSourceError(DomainError):
    def __init__(self, code="REPAIR_SOURCE_UNAVAILABLE", *, store_opened=False):
        self.store_opened = store_opened
        super().__init__(code)


def scope_for(d):
    return pb.NodeScope(
        namespace_id=str(d["namespace_id"]),
        recovery_epoch=str(d["recovery_epoch"]),
        node_id=d["node_id"],
        node_incarnation=str(d["incarnation"]),
    )


def key_for(d):
    return pb.AllocationKey(scope=scope_for(d), allocation_id=str(d["allocation_id"]))


def spec_for(d):
    return pb.ChunkSpec(
        chunk_id=str(d["chunk_id"]),
        size_bytes=d["size_bytes"],
        checksum_sha256=d["checksum_sha256"],
    )


def evidence(record, d, *, deleted=False):
    if record.key != key_for(d) or record.generation != d["generation"]:
        raise DomainError("INVALID_STORAGE_ACK")
    s = record.key.scope
    if deleted:
        if record.state != pb.DELETED or not record.cleanup_complete:
            raise DomainError("INVALID_DELETE_ACK")
        return DeleteEvidence(
            UUID(s.namespace_id),
            UUID(s.recovery_epoch),
            s.node_id,
            UUID(s.node_incarnation),
            UUID(record.key.allocation_id),
            record.generation,
            "DELETED",
            True,
        )
    if record.chunk != spec_for(d):
        raise DomainError("INVALID_STORAGE_ACK")
    if record.state not in (pb.PREPARED, pb.WRITING, pb.STORED, pb.DELETED, pb.CORRUPT):
        raise DomainError("INVALID_STORAGE_ACK")
    result = AllocationEvidence(
        UUID(s.namespace_id),
        UUID(s.recovery_epoch),
        s.node_id,
        UUID(s.node_incarnation),
        UUID(record.key.allocation_id),
        record.generation,
        UUID(record.chunk.chunk_id),
        record.chunk.size_bytes,
        record.chunk.checksum_sha256,
        pb.AllocationState.Name(record.state),
        record.actual_size_bytes if record.HasField("actual_size_bytes") else None,
        record.actual_checksum_sha256 if record.HasField("actual_checksum_sha256") else None,
    )
    if result.state == "STORED" and (result.actual_size, result.actual_hash) != (
        d["size_bytes"],
        d["checksum_sha256"],
    ):
        raise DomainError("INVALID_STORAGE_ACK")
    return result


def inventory_item(record):
    if record.state not in (pb.PREPARED, pb.WRITING, pb.STORED, pb.DELETED, pb.CORRUPT):
        raise DomainError("INVALID_STORAGE_INVENTORY")
    has_chunk = record.HasField("chunk")
    if not has_chunk and record.state != pb.DELETED:
        raise DomainError("INVALID_STORAGE_INVENTORY")
    s = record.key.scope
    try:
        return InventoryItem(
            UUID(s.namespace_id),
            UUID(s.recovery_epoch),
            s.node_id,
            UUID(s.node_incarnation),
            UUID(record.key.allocation_id),
            record.generation,
            pb.AllocationState.Name(record.state),
            record.cleanup_complete,
            record.inventory_sequence,
            UUID(record.chunk.chunk_id) if has_chunk else None,
            record.chunk.size_bytes if has_chunk else None,
            record.chunk.checksum_sha256 if has_chunk else None,
            record.actual_size_bytes if record.HasField("actual_size_bytes") else None,
            record.actual_checksum_sha256 if record.HasField("actual_checksum_sha256") else None,
        )
    except ValueError as exc:
        raise DomainError("INVALID_STORAGE_INVENTORY") from exc


class StorageClient:
    def __init__(self, timeout=5.0):
        if not 0 < timeout <= 120:
            raise ValueError("RPC timeout must be in (0,120]")
        self.timeout = timeout

    @staticmethod
    def channel(d):
        return grpc.insecure_channel(
            f"{d['host']}:{d['port']}",
            options=[
                ("grpc.max_receive_message_length", 300000),
                ("grpc.max_send_message_length", 300000),
            ],
        )

    def unary(self, method, d, work):
        work.check()
        with self.channel(d) as channel:
            stub = rpc.StorageServiceStub(channel)
            request = (
                pb.PrepareAllocationRequest(
                    key=key_for(d), generation=d["generation"], chunk=spec_for(d)
                )
                if method == "PrepareAllocation"
                else pb.AllocationRequest(key=key_for(d), generation=d["generation"])
            )
            call = getattr(stub, method).future(request, timeout=self.timeout)
            work.attach(call)
            try:
                record = call.result()
                work.check()
                return evidence(record, d, deleted=method == "DeleteAllocation")
            finally:
                work.detach(call)
                call.cancel()

    def health(self, node, stopping):
        scope = scope_for(node)
        with self.channel(node) as channel:
            call = rpc.StorageServiceStub(channel).HealthCheck.future(
                pb.HealthCheckRequest(scope=scope), timeout=2.0
            )
            try:
                while not stopping.is_set():
                    try:
                        response = call.result(timeout=0.1)
                        break
                    except grpc.FutureTimeoutError:
                        continue
                else:
                    return None
            except grpc.RpcError as exc:
                outcome = (
                    "scope_mismatch"
                    if exc.code() == grpc.StatusCode.FAILED_PRECONDITION
                    else "unreachable"
                )
                return HealthObservation(outcome)
            finally:
                call.cancel()
        if response.scope != scope or response.failure_domain != node["failure_domain"]:
            return HealthObservation("scope_mismatch")
        if not response.filesystem_available_bytes <= response.filesystem_capacity_bytes < 2**63:
            return HealthObservation("unreachable")
        return HealthObservation(
            "reachable",
            response.filesystem_available_bytes,
            response.writable,
            recovery_required=response.recovery_required,
        )

    def list_allocations(
        self, node, *, after_sequence=0, upper_sequence=None, limit=100, stopping=None
    ):
        if type(limit) is not int or not 0 < limit <= 1000:
            raise ValueError("Inventory limit must be in (0,1000]")
        if type(after_sequence) is not int or after_sequence < 0:
            raise ValueError("Invalid inventory cursor")
        request = pb.ListAllocationsRequest(
            scope=scope_for(node),
            after_sequence=after_sequence,
            limit=limit,
        )
        if upper_sequence is not None:
            request.upper_sequence = upper_sequence
        with self.channel(node) as channel:
            call = rpc.StorageServiceStub(channel).ListAllocations.future(
                request, timeout=self.timeout
            )
            try:
                if stopping is None:
                    response = call.result()
                else:
                    while not stopping.is_set():
                        try:
                            response = call.result(timeout=0.1)
                            break
                        except grpc.FutureTimeoutError:
                            continue
                    else:
                        return None
            except grpc.RpcError as exc:
                if exc.code() == grpc.StatusCode.FAILED_PRECONDITION:
                    raise DomainError("NODE_SCOPE_MISMATCH") from exc
                raise DomainError("STORAGE_UNAVAILABLE") from exc
            finally:
                call.cancel()
        if len(response.items) > limit:
            raise DomainError("INVALID_STORAGE_INVENTORY")
        items = tuple(inventory_item(record) for record in response.items)
        page = InventoryPage(
            items=items,
            upper_sequence=response.upper_sequence,
            next_after_sequence=response.next_after_sequence,
            has_more=response.has_more,
        )
        return page

    def read(self, d, digest, work):
        """Return updated full digest only after validated footer AND successful EOF.

        A failed replica cannot contaminate the prefix used for fallback.
        """
        work.check()
        # Pay for this replica's entire expected chunk BEFORE opening GetChunk.
        # A partial/failed attempt keeps its conservative charge; no rate credit
        # is refunded on cancellation. Split reservations into bounded fair turns.
        remaining = d["size_bytes"]
        while remaining:
            block = min(262144, remaining)
            work.consume(block)
            remaining -= block
        work.check()
        with self.channel(d) as channel:
            call = rpc.StorageServiceStub(channel).GetChunk(
                pb.GetChunkRequest(key=key_for(d), expected_chunk=spec_for(d)), timeout=self.timeout
            )
            work.attach(call)
            total, chunk_hash, full = 0, hashlib.sha256(), digest.copy()
            header = footer = False
            try:
                for frame in call:
                    work.check()
                    kind = frame.WhichOneof("payload")
                    if kind == "header" and not header and total == 0 and not footer:
                        if evidence(frame.header, d).state != "STORED":
                            raise DomainError("INVALID_STORAGE_STREAM")
                        header = True
                    elif kind == "block" and header and not footer:
                        block = frame.block
                        if (
                            block.offset != total
                            or not 0 < len(block.data) <= 262144
                            or total + len(block.data) > d["size_bytes"]
                        ):
                            raise DomainError("INVALID_STORAGE_STREAM")
                        chunk_hash.update(block.data)
                        full.update(block.data)
                        total += len(block.data)
                    elif kind == "footer" and header and not footer:
                        if (total, chunk_hash.hexdigest()) != (
                            d["size_bytes"],
                            d["checksum_sha256"],
                        ) or (
                            frame.footer.actual_size_bytes,
                            frame.footer.actual_checksum_sha256,
                        ) != (total, chunk_hash.hexdigest()):
                            raise DomainError("INVALID_STORAGE_STREAM")
                        footer = True
                    else:
                        raise DomainError("INVALID_STORAGE_STREAM")
                work.check()
                if not header or not footer:
                    raise DomainError("INVALID_STORAGE_STREAM")
                return full, total
            finally:
                work.detach(call)
                call.cancel()

    def _store_single(self, dest, block, work):
        work.check()
        frames = [
            pb.StoreChunkFrame(
                header=pb.StoreChunkHeader(
                    key=key_for(dest), generation=dest["generation"], chunk=spec_for(dest)
                )
            ),
            pb.StoreChunkFrame(block=pb.DataBlock(offset=block[0], data=block[1])),
        ]
        with self.channel(dest) as channel:
            call = rpc.StorageServiceStub(channel).StoreChunk.future(
                iter(frames), timeout=self.timeout
            )
            work.attach(call)
            try:
                response = call.result()
                work.check()
                return evidence(response.allocation, dest)
            finally:
                work.detach(call)
                call.cancel()

    def copy(self, source, dest, work):
        """Stream one verified chunk GetChunk -> StoreChunk keeping bounded wire buffers.

        Holds back the trailing pending block until both footer and EOF match spec
        so the destination never sees the final byte of a corrupt/truncated source.
        """
        work.check()
        if (source["chunk_id"], source["size_bytes"], source["checksum_sha256"]) != (
            dest["chunk_id"],
            dest["size_bytes"],
            dest["checksum_sha256"],
        ):
            raise DomainError("INVALID_STORAGE_ACK")
        remaining = source["size_bytes"]
        while remaining:
            block = min(262144, remaining)
            work.consume(block)
            remaining -= block
        work.check()
        with self.channel(source) as source_channel:
            get_call = rpc.StorageServiceStub(source_channel).GetChunk(
                pb.GetChunkRequest(key=key_for(source), expected_chunk=spec_for(source)),
                timeout=self.timeout,
            )
            work.attach(get_call)
            total = 0
            chunk_hash = hashlib.sha256()
            header = False
            footer = False
            pending = None
            prefetched = None
            eof_verified = False
            try:
                get_iter = iter(get_call)
                try:
                    while True:
                        work.check()
                        try:
                            frame = next(get_iter)
                        except StopIteration:
                            if not header or not footer or pending is None:
                                raise RepairSourceError("INVALID_STORAGE_STREAM")
                            eof_verified = True
                            break
                        kind = frame.WhichOneof("payload")
                        if kind == "header" and not header and total == 0 and not footer:
                            if evidence(frame.header, source).state != "STORED":
                                raise RepairSourceError("INVALID_STORAGE_STREAM")
                            header = True
                        elif kind == "block" and header and not footer:
                            block = frame.block
                            if (
                                block.offset != total
                                or not 0 < len(block.data) <= 262144
                                or total + len(block.data) > source["size_bytes"]
                            ):
                                raise RepairSourceError("INVALID_STORAGE_STREAM")
                            chunk_hash.update(block.data)
                            total += len(block.data)
                            item = (block.offset, block.data)
                            if pending is None:
                                pending = item
                            else:
                                prefetched = item
                                break
                        elif kind == "footer" and header and not footer and pending is not None:
                            if (total, chunk_hash.hexdigest()) != (
                                source["size_bytes"],
                                source["checksum_sha256"],
                            ) or (
                                frame.footer.actual_size_bytes,
                                frame.footer.actual_checksum_sha256,
                            ) != (total, chunk_hash.hexdigest()):
                                raise RepairSourceError("INVALID_STORAGE_STREAM")
                            footer = True
                        else:
                            raise RepairSourceError("INVALID_STORAGE_STREAM")
                except RepairSourceError:
                    raise
                except (grpc.RpcError, grpc.FutureCancelledError, DomainError) as exc:
                    work.check()
                    raise RepairSourceError(store_opened=False) from exc

                if eof_verified:
                    work.detach(get_call)
                    get_call.cancel()
                    return self._store_single(dest, pending, work)

                closing = Event()
                source_error = None

                def store_frames():
                    nonlocal total, footer, source_error
                    current = pending
                    nxt = prefetched
                    try:
                        yield pb.StoreChunkFrame(
                            header=pb.StoreChunkHeader(
                                key=key_for(dest),
                                generation=dest["generation"],
                                chunk=spec_for(dest),
                            )
                        )
                        yield pb.StoreChunkFrame(
                            block=pb.DataBlock(offset=current[0], data=current[1])
                        )
                        current = nxt
                        nxt = None
                        while True:
                            work.check()
                            try:
                                frame = next(get_iter)
                            except StopIteration:
                                if not footer:
                                    raise RepairSourceError(
                                        "INVALID_STORAGE_STREAM", store_opened=True
                                    )
                                yield pb.StoreChunkFrame(
                                    block=pb.DataBlock(offset=current[0], data=current[1])
                                )
                                return
                            except (grpc.RpcError, grpc.FutureCancelledError, DomainError) as exc:
                                if closing.is_set():
                                    return
                                raise RepairSourceError(store_opened=True) from exc
                            kind = frame.WhichOneof("payload")
                            if kind == "block" and not footer:
                                block = frame.block
                                if (
                                    block.offset != total
                                    or not 0 < len(block.data) <= 262144
                                    or total + len(block.data) > source["size_bytes"]
                                ):
                                    raise RepairSourceError(
                                        "INVALID_STORAGE_STREAM", store_opened=True
                                    )
                                chunk_hash.update(block.data)
                                total += len(block.data)
                                yield pb.StoreChunkFrame(
                                    block=pb.DataBlock(offset=current[0], data=current[1])
                                )
                                current = (block.offset, block.data)
                            elif kind == "footer" and not footer:
                                if (total, chunk_hash.hexdigest()) != (
                                    source["size_bytes"],
                                    source["checksum_sha256"],
                                ) or (
                                    frame.footer.actual_size_bytes,
                                    frame.footer.actual_checksum_sha256,
                                ) != (total, chunk_hash.hexdigest()):
                                    raise RepairSourceError(
                                        "INVALID_STORAGE_STREAM", store_opened=True
                                    )
                                footer = True
                            else:
                                raise RepairSourceError("INVALID_STORAGE_STREAM", store_opened=True)
                    except BaseException as exc:
                        if not closing.is_set():
                            source_error = exc
                        raise

                with self.channel(dest) as dest_channel:
                    store_call = rpc.StorageServiceStub(dest_channel).StoreChunk.future(
                        store_frames(), timeout=self.timeout
                    )
                    work.attach(store_call)
                    try:
                        response = store_call.result()
                        work.check()
                        if source_error is not None:
                            if isinstance(source_error, RepairSourceError):
                                raise source_error
                            raise RepairSourceError(store_opened=True) from source_error
                        return evidence(response.allocation, dest)
                    except BaseException:
                        closing.set()
                        get_call.cancel()
                        store_call.cancel()
                        work.check()
                        if source_error is not None:
                            if isinstance(source_error, RepairSourceError):
                                raise source_error
                            raise RepairSourceError(store_opened=True) from source_error
                        raise
                    finally:
                        closing.set()
                        work.detach(store_call)
                        store_call.cancel()
            finally:
                work.detach(get_call)
                get_call.cancel()
