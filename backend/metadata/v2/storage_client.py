"""Worker-only V2 transport. One channel/call per operation; no unbounded cache."""

import hashlib
from uuid import UUID

import grpc
import v2_storage_pb2 as pb
import v2_storage_pb2_grpc as rpc

from metadata.v2.repository import AllocationEvidence, DeleteEvidence, DomainError


def key_for(d):
    return pb.AllocationKey(
        scope=pb.NodeScope(
            namespace_id=str(d["namespace_id"]),
            recovery_epoch=str(d["recovery_epoch"]),
            node_id=d["node_id"],
            node_incarnation=str(d["incarnation"]),
        ),
        allocation_id=str(d["allocation_id"]),
    )


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
