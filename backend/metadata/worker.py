"""One lifespan-owned scheduler for independent, bounded health polls."""

import logging
from concurrent.futures import ThreadPoolExecutor
from threading import TIMEOUT_MAX, Event, Lock, Thread
from time import monotonic

import grpc
import storage_pb2
import storage_pb2_grpc
from sqlalchemy import text

from metadata.health import HealthDetector, HealthSnapshot
from metadata.models import StorageNode

logger = logging.getLogger(__name__)


def history_snapshot(node):
    """DB history cannot establish a success in the current process."""
    return HealthSnapshot(
        last_success_at=node.last_success_at,
        last_error=node.last_error,
        capacity_bytes=node.capacity_bytes,
        available_bytes=node.available_bytes,
        used_bytes=node.used_bytes,
    )


class MetadataWorker:
    def __init__(self, session_factory, settings):
        self.sessions = session_factory
        self.nodes = [node.model_copy(deep=True) for node in settings.storage_nodes_json]
        self.interval = settings.health_interval_seconds
        self.rpc_timeout = settings.health_rpc_timeout_seconds
        self.grpc_options = settings.grpc_options()
        self.detectors = {
            node.node_id: HealthDetector(node, settings.node_down_after_seconds)
            for node in self.nodes
        }
        self._stop = Event()
        self._wake = Event()
        self._closed = Event()
        self._stop_lock = Lock()
        self._snapshot_lock = Lock()
        self._snapshots = {}
        self._last_log = {}
        self._channels = {}
        self._stubs = {}
        self._executor = None
        self._thread = None

    @property
    def snapshots(self):
        with self._snapshot_lock:
            return dict(self._snapshots)

    @property
    def running(self):
        return (
            self._thread is not None
            and self._thread.is_alive()
            and not self._stop.is_set()
            and not self._closed.is_set()
        )

    def start(self):
        if self._thread is not None or self._closed.is_set():
            raise RuntimeError("Create a new MetadataWorker for each lifespan")
        try:
            for node in self.nodes:
                host = (
                    f"[{node.host}]"
                    if ":" in node.host and not node.host.startswith("[")
                    else node.host
                )
                channel = grpc.insecure_channel(f"{host}:{node.port}", options=self.grpc_options)
                self._channels[node.node_id] = channel
                self._stubs[node.node_id] = storage_pb2_grpc.StorageServiceStub(channel)
            self._executor = ThreadPoolExecutor(
                max_workers=max(1, len(self.nodes)), thread_name_prefix="metadata-health"
            )
            self._thread = Thread(target=self._run, name="metadata-poll", daemon=True)
            self._thread.start()
        except Exception:
            self.stop()
            raise

    def stop(self):
        with self._stop_lock:
            if self._closed.is_set():
                return
            self._stop.set()
            self._wake.set()
            try:
                if self._thread is not None and self._thread.ident is not None:
                    self._thread.join()
                if self._executor is not None:
                    self._executor.shutdown(wait=True, cancel_futures=True)
            finally:
                for channel in self._channels.values():
                    channel.close()
                self._closed.set()

    def _run(self):
        try:
            self._schedule()
        except Exception as exc:
            self._stop.set()
            logger.error("Health scheduler stopped (%s)", type(exc).__name__)

    def _schedule(self):
        next_due = {node.node_id: monotonic() for node in self.nodes}
        running = {}
        while not self._stop.is_set():
            self._wake.clear()
            now = monotonic()
            for node in self.nodes:
                node_id = node.node_id
                future = running.get(node_id)
                if future is not None:
                    if not future.done():
                        continue
                    try:
                        future.result()
                    except Exception as exc:
                        self._log_failure(node_id, exc)
                    del running[node_id]
                if now >= next_due[node_id] and not self._stop.is_set():
                    running[node_id] = self._executor.submit(self._poll, node)
                    running[node_id].add_done_callback(lambda _: self._wake.set())
                    next_due[node_id] = now + self.interval
            pending_due = [due for node_id, due in next_due.items() if node_id not in running]
            timeout = max(0, min(pending_due) - monotonic()) if pending_due else self.interval
            # Finite configuration can still exceed the platform's lock timeout limit.
            self._wake.wait(min(timeout, TIMEOUT_MAX))

    @staticmethod
    def _matches(node, config):
        return node is not None and (node.host, node.port, node.failure_domain) == (
            config.host,
            config.port,
            config.failure_domain,
        )

    @staticmethod
    def _bound_transaction(session):
        # Bound DB waits too, so a row lock cannot hold graceful shutdown forever.
        session.execute(text("SET LOCAL statement_timeout = '5s'"))

    def _forget(self, node_id):
        with self._snapshot_lock:
            self._snapshots.pop(node_id, None)

    def _persist(self, config, candidate):
        accepted = False
        with self.sessions.begin() as session:
            self._bound_transaction(session)
            node = session.get(StorageNode, config.node_id, with_for_update=True)
            if self._matches(node, config):
                if not node.enabled:
                    candidate = HealthDetector.disabled(history_snapshot(node))
                for field in (
                    "status",
                    "last_success_at",
                    "last_error",
                    "capacity_bytes",
                    "available_bytes",
                    "used_bytes",
                ):
                    setattr(node, field, getattr(candidate, field))
                accepted = True
        # A late disabled/changed endpoint must not publish the completed RPC as ACTIVE.
        with self._snapshot_lock:
            if accepted:
                self._snapshots[config.node_id] = candidate
            else:
                self._snapshots.pop(config.node_id, None)

    def _poll(self, config):
        try:
            if self._stop.is_set():
                return
            with self.sessions.begin() as session:
                self._bound_transaction(session)
                node = session.get(StorageNode, config.node_id)
                if not self._matches(node, config):
                    self._forget(config.node_id)
                    return
                enabled = node.enabled
                with self._snapshot_lock:
                    previous = self._snapshots.get(config.node_id) or history_snapshot(node)
            detector = self.detectors[config.node_id]
            if not enabled:
                self._persist(config, detector.disabled(previous))
                return
            if self._stop.is_set():
                return
            try:
                response = self._stubs[config.node_id].HealthCheck(
                    storage_pb2.HealthCheckRequest(), timeout=self.rpc_timeout, wait_for_ready=False
                )
            except grpc.RpcError as exc:
                candidate = detector.failure(previous, exc.code())
            else:
                candidate = detector.success(previous, response)
            self._persist(config, candidate)
        except Exception as exc:
            self._log_failure(config.node_id, exc)

    def _log_failure(self, node_id, exc):
        now = monotonic()
        if now - self._last_log.get(node_id, float("-inf")) >= 30:
            self._last_log[node_id] = now
            logger.warning(
                "Health poll failed for node %s (%s); retry on next interval",
                node_id,
                type(exc).__name__,
            )
