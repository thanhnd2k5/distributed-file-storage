"""Lifespan-owned unary data RPCs, with bounded retries and verified responses."""

import hashlib
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Condition, Event
from time import sleep

import grpc
import storage_pb2
import storage_pb2_grpc

logger = logging.getLogger(__name__)
TRANSIENT_CODES = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}
RETRY_BACKOFF_SECONDS = 0.2


class StorageRpcError(Exception):
    """Transport/integrity outcome; the coordinator owns DB state and fallback."""

    def __init__(self, method, node_id, status, attempts, *, reason=None):
        self.method = method
        self.node_id = node_id
        self.grpc_status = status
        self.attempts = attempts
        self.reason = reason or status.name
        super().__init__(f"{method} failed on {node_id}: {self.reason}")

    @property
    def replica_status(self):
        # A Store failure never proves MISSING/CORRUPTED; preserve attempted PENDING.
        if self.method == "GetChunk":
            if self.grpc_status == grpc.StatusCode.NOT_FOUND:
                return "MISSING"
            if (
                self.grpc_status == grpc.StatusCode.DATA_LOSS
                or self.reason == "INVALID_GET_RESPONSE"
            ):
                return "CORRUPTED"
        return None

    @property
    def cancelled(self):
        return self.grpc_status == grpc.StatusCode.CANCELLED


@dataclass(frozen=True)
class StoreAck:
    chunk_id: str
    size_bytes: int
    checksum_sha256: str
    already_existed: bool


@dataclass(frozen=True)
class DeleteAck:
    chunk_id: str
    existed: bool


class StorageClient:
    """Own independent data channels; close drains accepted calls before closing.

    Construction has no network side effects. Every accepted call has a finite
    per-attempt deadline and at most two attempts. close rejects new calls but
    waits for existing retries/cancellation to finish; it does not guess whether
    a timed-out Store committed. Health channels remain owned by MetadataWorker.
    """

    def __init__(self, settings):
        self._nodes = tuple(node.model_copy(deep=True) for node in settings.storage_nodes_json)
        self._options = settings.grpc_options()
        self._timeout = settings.chunk_rpc_timeout_seconds
        self._attempts = settings.rpc_max_attempts
        self._condition = Condition()
        self._channels = {}
        self._stubs = {}
        self._active_calls = 0
        self._started = False
        self._closing = False
        self._closed = False

    @property
    def running(self):
        with self._condition:
            return self._started and not self._closing and not self._closed

    def start(self):
        with self._condition:
            if self._started or self._closed:
                raise RuntimeError("Create a new StorageClient for each lifespan")
            try:
                for node in self._nodes:
                    host = (
                        f"[{node.host}]"
                        if ":" in node.host and not node.host.startswith("[")
                        else node.host
                    )
                    channel = grpc.insecure_channel(f"{host}:{node.port}", options=self._options)
                    self._channels[node.node_id] = channel
                    self._stubs[node.node_id] = storage_pb2_grpc.StorageServiceStub(channel)
                self._started = True
            except Exception:
                for channel in self._channels.values():
                    channel.close()
                self._closed = True
                raise

    def close(self):
        with self._condition:
            if self._closed:
                return
            if self._closing:
                self._condition.wait_for(lambda: self._closed)
                return
            self._closing = True
            self._condition.wait_for(lambda: self._active_calls == 0)
            try:
                for channel in self._channels.values():
                    channel.close()
            finally:
                self._closed = True
                self._condition.notify_all()

    @contextmanager
    def _call_scope(self, node_id):
        with self._condition:
            if not self._started or self._closing or self._closed:
                raise RuntimeError("StorageClient is not running")
            if node_id not in self._stubs:
                raise ValueError("Storage node is not configured")
            self._active_calls += 1
            stub = self._stubs[node_id]
        try:
            yield stub
        finally:
            with self._condition:
                self._active_calls -= 1
                self._condition.notify_all()

    @staticmethod
    def _check_cancel(method, node_id, attempt, cancel):
        if cancel is not None and cancel.is_set():
            raise StorageRpcError(method, node_id, grpc.StatusCode.CANCELLED, attempt)

    @staticmethod
    def _wait_result(future, method, node_id, attempt, cancel):
        try:
            if cancel is None:
                return future.result()
            while True:
                if cancel.is_set():
                    future.cancel()
                    raise StorageRpcError(method, node_id, grpc.StatusCode.CANCELLED, attempt)
                try:
                    return future.result(timeout=0.05)
                except grpc.FutureTimeoutError:
                    continue
        except grpc.FutureCancelledError:
            raise StorageRpcError(method, node_id, grpc.StatusCode.CANCELLED, attempt) from None

    def _invoke(self, node_id, method, request, cancel):
        with self._call_scope(node_id) as stub:
            for attempt in range(1, self._attempts + 1):
                self._check_cancel(method, node_id, attempt - 1, cancel)
                try:
                    future = getattr(stub, method).future(
                        request, timeout=self._timeout, wait_for_ready=False
                    )
                    response = self._wait_result(future, method, node_id, attempt, cancel)
                except grpc.RpcError as exc:
                    status = exc.code()
                    if status in TRANSIENT_CODES and attempt < self._attempts:
                        if cancel is None:
                            sleep(RETRY_BACKOFF_SECONDS)
                        elif cancel.wait(RETRY_BACKOFF_SECONDS):
                            self._check_cancel(method, node_id, attempt, cancel)
                        continue
                    if status == grpc.StatusCode.INTERNAL:
                        logger.warning("Storage %s failed on %s (INTERNAL)", method, node_id)
                    raise StorageRpcError(method, node_id, status, attempt) from None
                return response, attempt

    def store_chunk(
        self,
        node_id: str,
        chunk_id: str,
        data: bytes,
        checksum_sha256: str,
        *,
        cancel: Event | None = None,
    ) -> StoreAck:
        request = storage_pb2.StoreChunkRequest(
            chunk_id=chunk_id, data=data, checksum_sha256=checksum_sha256
        )
        response, attempts = self._invoke(node_id, "StoreChunk", request, cancel)
        if (
            response.chunk_id != request.chunk_id
            or response.size_bytes != len(request.data)
            or response.checksum_sha256 != request.checksum_sha256
        ):
            raise StorageRpcError("StoreChunk", node_id, None, attempts, reason="INVALID_STORE_ACK")
        return StoreAck(
            response.chunk_id,
            response.size_bytes,
            response.checksum_sha256,
            response.already_existed,
        )

    def get_chunk(
        self,
        node_id: str,
        chunk_id: str,
        size_bytes: int,
        checksum_sha256: str,
        *,
        cancel: Event | None = None,
    ) -> bytes:
        response, attempts = self._invoke(
            node_id, "GetChunk", storage_pb2.GetChunkRequest(chunk_id=chunk_id), cancel
        )
        actual_hash = hashlib.sha256(response.data).hexdigest()
        if (
            response.chunk_id != chunk_id
            or len(response.data) != size_bytes
            or actual_hash != checksum_sha256
            or response.checksum_sha256 != actual_hash
        ):
            raise StorageRpcError(
                "GetChunk", node_id, None, attempts, reason="INVALID_GET_RESPONSE"
            )
        return response.data

    def delete_chunk(
        self, node_id: str, chunk_id: str, *, cancel: Event | None = None
    ) -> DeleteAck:
        try:
            response, attempts = self._invoke(
                node_id, "DeleteChunk", storage_pb2.DeleteChunkRequest(chunk_id=chunk_id), cancel
            )
        except StorageRpcError as exc:
            # Compatibility with servers returning NOT_FOUND instead of OK existed=false.
            if exc.grpc_status == grpc.StatusCode.NOT_FOUND:
                return DeleteAck(chunk_id, False)
            raise
        if response.chunk_id != chunk_id:
            raise StorageRpcError(
                "DeleteChunk", node_id, None, attempts, reason="INVALID_DELETE_ACK"
            )
        return DeleteAck(response.chunk_id, response.existed)
