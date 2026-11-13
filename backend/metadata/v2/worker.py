"""Bounded V2 job runtime. No HTTP adapter and no implicit migration/provisioning."""

import hashlib
import logging
import math
import os
import signal
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from threading import Event, Lock, Thread
from uuid import UUID, uuid4

import grpc
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from metadata.v2.health import HealthObservation
from metadata.v2.repository import DomainError, JobLease, Repository, positive
from metadata.v2.storage_client import RepairSourceError, StorageClient

log = logging.getLogger(__name__)
KINDS = ("CLEANUP", "RECONCILE", "FINALIZE", "REPAIR")
DEFINITIVE_DEST_STATUS = frozenset(
    {
        grpc.StatusCode.FAILED_PRECONDITION,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.DATA_LOSS,
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.ALREADY_EXISTS,
        grpc.StatusCode.NOT_FOUND,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.UNIMPLEMENTED,
    }
)
AMBIGUOUS_DEST_STATUS = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.CANCELLED,
        grpc.StatusCode.UNKNOWN,
        grpc.StatusCode.INTERNAL,
    }
)
# Prepare reset the dest after a broken StoreChunk; try the next source (or fail).
_RETRY_REPAIR_SOURCE = object()


@dataclass(frozen=True)
class WorkerConfig:
    slots: int = 2
    finalize_slots: int = 1
    page_size: int = 8
    max_attempts: int = 8
    poll_seconds: float = 0.25
    renew_seconds: float = 15
    job_seconds: float = 3600
    shutdown_seconds: float = 30
    read_bytes_per_second: int = 64 * 1024**2
    jobs_per_kind: int = 2
    repair_scan_seconds: float = 1.0

    def __post_init__(self):
        positive(self.slots, 8)
        positive(self.finalize_slots, self.slots - 1)
        positive(self.page_size, 8)
        positive(self.max_attempts, 100)
        positive(self.read_bytes_per_second)
        positive(self.jobs_per_kind, 24 // len(KINDS))
        for value in (
            self.poll_seconds,
            self.renew_seconds,
            self.job_seconds,
            self.shutdown_seconds,
            self.repair_scan_seconds,
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError("Worker durations must be finite and positive")
        if self.renew_seconds > 20:
            raise ValueError("Renew interval must leave margin within the 60s DB lease")


class ReadBudget:
    """FIFO read credit shared by all Work in one Worker, including retries.

    GetChunk prepays expected bytes in bounded turns. Cancellation/fallback
    never refunds credit; actual transport can still burst within a chunk/RPC.
    """

    def __init__(self, rate):
        self.rate = positive(rate)
        self.lock = Lock()
        self.due = 0.0

    def reserve(self, size):
        positive(size, 262144)
        with self.lock:
            self.due = max(self.due, time.monotonic()) + size / self.rate
            return self.due


class Work:
    def __init__(
        self, repo, lease, config, stopping, *, read_budget=None, shutdown_deadline=lambda: None
    ):
        self.repo, self.lease, self.config, self.stopping = repo, lease, config, stopping
        self.cancelled, self.done, self.lock = Event(), Event(), Lock()
        self.calls = set()
        self.started = time.monotonic()
        self.read_budget = read_budget or ReadBudget(config.read_bytes_per_second)
        self.shutdown_deadline = shutdown_deadline
        self.heartbeat = Thread(target=self._heartbeat, name="v2-lease", daemon=True)

    def _heartbeat(self):
        due = time.monotonic() + self.config.renew_seconds
        while not self.done.wait(min(0.1, self.config.renew_seconds)):
            if self.stopping.is_set() or time.monotonic() - self.started >= self.config.job_seconds:
                self.cancel()
                return
            if time.monotonic() >= due:
                try:
                    self.repo.renew_job(self.lease.job_id, self.lease.owner, self.lease.token)
                except Exception:
                    self.cancel()
                    return
                due = time.monotonic() + self.config.renew_seconds

    def cancel(self):
        self.cancelled.set()
        with self.lock:
            for call in self.calls:
                call.cancel()

    def check(self):
        if self.stopping.is_set() or self.cancelled.is_set():
            raise DomainError("WORK_CANCELLED")
        if time.monotonic() - self.started >= self.config.job_seconds:
            self.cancel()
            raise DomainError("WORK_BUDGET_EXCEEDED")

    def attach(self, call):
        with self.lock:
            self.calls.add(call)
            if self.stopping.is_set() or self.cancelled.is_set():
                call.cancel()

    def detach(self, call):
        with self.lock:
            self.calls.discard(call)

    def consume(self, size):
        self.check()
        due = self.read_budget.reserve(size)
        while (delay := due - time.monotonic()) > 0:
            self.cancelled.wait(min(delay, 0.1))
            self.check()

    def __enter__(self):
        self.check()
        self.heartbeat.start()
        return self

    def __exit__(self, *args):
        self.done.set()
        self.cancel()
        timeout = self.config.shutdown_seconds
        if (due := self.shutdown_deadline()) is not None:
            timeout = min(timeout, max(0, due - time.monotonic()))
        self.heartbeat.join(timeout=timeout)
        if self.heartbeat.is_alive():
            raise RuntimeError("V2 lease thread failed to drain")


class Worker:
    def __init__(self, repo, *, storage=None, config=None, hook=lambda point: None):
        self.repo, self.storage = repo, storage or StorageClient()
        self.config, self.hook = config or WorkerConfig(), hook
        self.owner, self.stopping, self.running = uuid4(), Event(), Event()
        self.executor = ThreadPoolExecutor(
            max_workers=self.config.slots, thread_name_prefix="v2-job"
        )
        self.health_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="v2-health")
        self.health_future = None
        self.futures = {}
        self.executing, self.resident = {}, {}
        self.queues = {kind: deque() for kind in KINDS}
        self.claim_cursor = self.dispatch_cursor = 0
        self.repair_cursor = None
        self.next_repair_scan = 0.0
        self.read_budget = ReadBudget(self.config.read_bytes_per_second)
        self.shutdown_due = None
        self.consumer_slot = Lock()
        self.consumer_done = Event()
        self.consumer_done.set()

    def ready(self):
        return self.running.is_set() and not self.stopping.is_set() and self.repo.ready()

    def execute(self, job, *, consumer_owner=None):
        """Drain a job synchronously for the dedicated internal consumer/direct caller."""
        steps = self.steps(job, consumer_owner=consumer_owner)
        try:
            while True:
                done, value = self.advance(steps)
                if done:
                    return value
        finally:
            steps.close()

    @staticmethod
    def advance(steps):
        try:
            next(steps)
            return False, None
        except StopIteration as completed:
            return True, completed.value

    def steps(self, job, *, consumer_owner=None):
        lease = JobLease(job["job_id"], self.owner, job["fencing_token"])
        try:
            with Work(
                self.repo,
                lease,
                self.config,
                self.stopping,
                read_budget=self.read_budget,
                shutdown_deadline=lambda: self.shutdown_due,
            ) as work:
                # Prime on claim: paused continuations renew their lease and keep
                # the original wall-clock job deadline, without holding a thread.
                yield
                work.check()
                self.hook("claimed")
                snapshot = self.repo.worker_snapshot(job, lease)
                session = snapshot["session"]
                if job["kind"] == "REPAIR":
                    file = snapshot["file"]
                    if file["state"] != "AVAILABLE" or (
                        job["payload"].get("file_id"),
                        job["payload"].get("version_id"),
                    ) != (str(file["file_id"]), str(file["version_id"])):
                        self.repo.finish_job(lease.job_id, lease.owner, lease.token)
                        return None
                elif job["kind"] != "CLEANUP" and session["state"] in (
                    "ABORTED",
                    "EXPIRED",
                    "FAILED",
                    "COMPLETED",
                ):
                    self.repo.finish_job(lease.job_id, lease.owner, lease.token)
                    return None
                if job["attempts"] > self.config.max_attempts:
                    raise DomainError("RETRY_BUDGET_EXHAUSTED")
                if job["kind"] == "RECONCILE":
                    return (yield from self.reconcile(snapshot, lease, work, consumer_owner))
                if job["kind"] == "CLEANUP":
                    if "allocation" in snapshot:
                        return self.cleanup_allocation(snapshot["allocation"], lease, work)
                    return (yield from self.cleanup(session, lease, work))
                if job["kind"] == "FINALIZE":
                    return (yield from self.finalize(session, lease, work))
                if job["kind"] == "REPAIR":
                    return (yield from self.repair(snapshot, lease, work))
                raise DomainError("UNSUPPORTED_JOB")
        except (DomainError, grpc.RpcError, grpc.FutureCancelledError, SQLAlchemyError) as exc:
            if self.stopping.is_set():
                return None  # Leave durable intent; another process reclaims after expiry.
            code = exc.code if isinstance(exc, DomainError) else "STORAGE_OR_DB_UNAVAILABLE"
            if code == "STALE_JOB":
                return None
            try:
                self.repo.fail_worker_job(
                    job, lease, error=code, max_attempts=self.config.max_attempts
                )
            except (DomainError, SQLAlchemyError):
                log.warning("V2 job outcome retained for lease recovery: %s", job["job_id"])
            return None

    def reconcile(self, snapshot, lease, work, consumer_owner):
        part, upload = snapshot["part"], snapshot["session"]["upload_id"]
        proofs = []
        for d in snapshot["destinations"]:
            work.check()
            try:
                proofs.append(self.storage.unary("InspectAllocation", d, work))
            except grpc.RpcError as exc:
                # A previous Worker may have committed generation intent and died
                # before sending every Prepare. Exact Inspect cannot prove that
                # older ledger row. Only a consumer may advance/fence it again;
                # Prepare still enforces scope/spec and durable drain on each node.
                allowed = (grpc.StatusCode.NOT_FOUND,)
                if consumer_owner is not None:
                    allowed += (grpc.StatusCode.FAILED_PRECONDITION,)
                if exc.code() not in allowed:
                    raise
            yield
        if (
            len(proofs) == len(snapshot["destinations"])
            and proofs
            and all(p.state == "STORED" for p in proofs)
        ):
            work.check()
            self.hook("before_reconcile")
            return self.repo.reconcile_part(
                upload, part["part_index"], part["revision"], proofs, lease
            )
        if consumer_owner is None:
            raise DomainError("OUTCOME_PENDING")
        work.check()
        intent = self.repo.authorize_part_retry(
            upload, part["part_index"], part["revision"], consumer_owner, lease
        )
        self.hook("retry_intent")
        proofs = []
        for d in intent["destinations"]:
            work.check()
            proofs.append(self.storage.unary("PrepareAllocation", d, work))
            yield
        work.check()
        self.hook("before_retry")
        result = self.repo.retry_part(
            upload, part["part_index"], intent["claim_token"], intent["revision"], proofs, lease
        )
        self.hook("retry_committed")
        return result

    def retry_for_consumer(self, chunk_id, owner):
        """Synchronous C3 handoff. Caller owns the returned claim; never background it.

        A lost response leaves a lease tracked by sweep_claims, just like Transfer death.
        Use a separate Worker instance so this call cannot exceed a running loop's slots.
        """
        if not isinstance(owner, UUID) or not self.consumer_slot.acquire(blocking=False):
            raise ValueError("Consumer UUID and a dedicated Worker are required")
        self.consumer_done.clear()
        try:
            if self.stopping.is_set() or self.resident:
                raise ValueError("A live dedicated consumer Worker is required")
            jobs = self.repo.claim_jobs(self.owner, 1, kinds=("RECONCILE",), target=chunk_id)
            return self.execute(jobs[0], consumer_owner=owner) if jobs else None
        finally:
            self.consumer_done.set()
            self.consumer_slot.release()

    def cleanup(self, session, lease, work):
        cursor = -1
        while True:
            work.check()
            page = self.repo.authorize_cleanup(
                session["upload_id"], lease, after_index=cursor, limit=self.config.page_size
            )
            self.hook("cleanup_intent")
            for a in page["allocations"]:
                work.check()
                d = self.repo.cleanup_endpoint(a)
                proof = self.storage.unary("DeleteAllocation", d, work)
                self.hook("delete_ack")
                work.check()
                self.repo.release_allocation(proof, lease)
                self.hook("release_committed")
                # One allocation/RPC is a bounded cleanup quantum. No transaction
                # or RPC survives the yield; durable release precedes rotation.
                yield
            if not page["has_more"]:
                break
            cursor = page["next_after_index"]
        work.check()
        self.repo.finish_cleanup(session["upload_id"], lease)

    def cleanup_allocation(self, allocation, lease, work):
        work.check()
        if allocation["state"] == "RELEASED":
            self.repo.finish_released_cleanup(allocation["allocation_id"], lease)
            return False
        self.hook("cleanup_intent")
        proof = self.storage.unary("DeleteAllocation", allocation, work)
        self.hook("delete_ack")
        work.check()
        released = self.repo.release_abandoned_allocation(proof, lease)
        self.hook("release_committed")
        return released

    @staticmethod
    def _is_definitive_dest_error(exc):
        if isinstance(exc, RepairSourceError):
            return False
        if isinstance(exc, grpc.RpcError):
            return exc.code() in DEFINITIVE_DEST_STATUS
        return isinstance(exc, DomainError) and exc.code in (
            "INVALID_STORAGE_ACK",
            "INVALID_STORAGE_STREAM",
        )

    @staticmethod
    def _is_ambiguous_dest_error(exc):
        if isinstance(exc, RepairSourceError):
            return False
        return isinstance(exc, grpc.RpcError) and exc.code() in AMBIGUOUS_DEST_STATUS

    def _should_abandon_repair_target(self, exc, chunk_id, version_id, attempted):
        if self._is_definitive_dest_error(exc):
            return True
        if self._is_ambiguous_dest_error(exc):
            return self.repo.repair_can_continue(chunk_id, version_id, exclude=attempted)
        return False

    def _abandon_repair_target(self, chunk_id, version_id, dest, lease, work):
        work.check()
        abandoned = self.repo.abandon_repair_destination(
            chunk_id, version_id, lease, dest["allocation_id"]
        )
        self.hook("repair_abandoned")
        try:
            proof = self.storage.unary("DeleteAllocation", abandoned, work)
            work.check()
            self.repo.release_abandoned_allocation(
                proof, lease, cleanup_job_id=abandoned["cleanup_job_id"]
            )
            self.hook("repair_cleanup_released")
        except (grpc.RpcError, grpc.FutureCancelledError):
            # Unreachable dest keeps DELETE_PENDING charge; CLEANUP retries later.
            work.check()
        except DomainError as exc:
            work.check()
            if exc.code in ("STALE_JOB", "REPAIR_NOT_AVAILABLE", "REPAIR_INTENT_CONFLICT"):
                raise
            log.warning(
                "V2 deferred repair destination cleanup: allocation=%s error=%s",
                dest["allocation_id"],
                exc.code,
            )
        yield

    def _reclaim_opened_repair_store(self, part, version_id, dest, lease, work, attempted):
        """After StoreChunk opened, recover a durable STORED ack or abandon the dest.

        Returns the STORED proof, None when the destination was abandoned and another
        target remains, or _RETRY_REPAIR_SOURCE when Prepare reset the dest.
        """
        try:
            prepared = self.storage.unary("PrepareAllocation", dest, work)
            if prepared.state == "STORED":
                self.hook("repair_copied")
                return prepared
        except (grpc.RpcError, grpc.FutureCancelledError, DomainError) as prep_exc:
            work.check()
            if self._should_abandon_repair_target(
                prep_exc, part["chunk_id"], version_id, attempted
            ):
                yield from self._abandon_repair_target(
                    part["chunk_id"], version_id, dest, lease, work
                )
                if self.repo.repair_can_continue(part["chunk_id"], version_id, exclude=attempted):
                    return None
            raise
        yield
        return _RETRY_REPAIR_SOURCE

    def _copy_chunk(self, part, version_id, dest, lease, work, attempted):
        sources = self.repo.repair_sources(part["chunk_id"], version_id, lease)
        if not sources:
            raise DomainError("REPAIR_SOURCE_UNAVAILABLE")
        for idx, source in enumerate(sources):
            try:
                work.check()
                proof = self.storage.copy(source, dest, work)
                self.hook("repair_copied")
                return proof
            except RepairSourceError as exc:
                work.check()
                if exc.store_opened:
                    reclaimed = yield from self._reclaim_opened_repair_store(
                        part, version_id, dest, lease, work, attempted
                    )
                    if reclaimed is not _RETRY_REPAIR_SOURCE:
                        return reclaimed
                if idx + 1 < len(sources):
                    continue
                raise DomainError("REPAIR_SOURCE_UNAVAILABLE") from exc
            except (grpc.RpcError, grpc.FutureCancelledError, DomainError) as exc:
                work.check()
                if self._is_ambiguous_dest_error(exc):
                    try:
                        inspected = self.storage.unary("InspectAllocation", dest, work)
                        if inspected.state == "STORED":
                            self.hook("repair_copied")
                            return inspected
                    except (grpc.RpcError, grpc.FutureCancelledError, DomainError):
                        work.check()
                if self._should_abandon_repair_target(exc, part["chunk_id"], version_id, attempted):
                    yield from self._abandon_repair_target(
                        part["chunk_id"], version_id, dest, lease, work
                    )
                    if self.repo.repair_can_continue(
                        part["chunk_id"], version_id, exclude=attempted
                    ):
                        return None
                raise

    def repair(self, snapshot, lease, work):
        part = snapshot["part"]
        version_id = snapshot["file"]["version_id"]
        attempted = set()
        try:
            while True:
                work.check()
                dest = self.repo.reserve_repair(
                    part["chunk_id"], version_id, lease, exclude_nodes=attempted
                )
                if dest is None:
                    return None
                attempted.add(dest["node_id"])
                self.hook("repair_intent")
                try:
                    work.check()
                    prepared = self.storage.unary("PrepareAllocation", dest, work)
                except (grpc.RpcError, grpc.FutureCancelledError, DomainError) as exc:
                    work.check()
                    if self._should_abandon_repair_target(
                        exc, part["chunk_id"], version_id, attempted
                    ):
                        yield from self._abandon_repair_target(
                            part["chunk_id"], version_id, dest, lease, work
                        )
                        if self.repo.repair_can_continue(
                            part["chunk_id"], version_id, exclude=attempted
                        ):
                            continue
                    raise
                yield
                if prepared.state == "STORED":
                    proof = prepared
                else:
                    proof = yield from self._copy_chunk(
                        part, version_id, dest, lease, work, attempted
                    )
                    if proof is None:
                        continue
                work.check()
                self.hook("before_confirm")
                proof = self.repo.confirm_repair(part["chunk_id"], version_id, lease, proof)
                self.hook("repair_committed")
                return proof
        except DomainError as exc:
            if exc.code == "REPAIR_NOT_AVAILABLE":
                self.repo.finish_job(lease.job_id, lease.owner, lease.token)
                return None
            raise

    def finalize(self, session, lease, work):
        digest, total, cursor = hashlib.sha256(), 0, -1
        while True:
            work.check()
            parts = self.repo.finalize_page(
                session["upload_id"], lease, after_index=cursor, limit=self.config.page_size
            )
            if not parts:
                break
            for part in parts:
                work.check()
                if part["part_index"] != cursor + 1:
                    raise DomainError("INVALID_MANIFEST")
                for d in part["destinations"]:
                    try:
                        updated, size = self.storage.read(d, digest, work)
                        break
                    except (grpc.RpcError, DomainError):
                        work.check()
                else:
                    raise DomainError("STORAGE_UNAVAILABLE")
                digest, total, cursor = updated, total + size, part["part_index"]
                self.hook("chunk_verified")
                # Preserve only hash/cursor + one bounded manifest page in RAM.
                # A process crash discards this state and rehashes from byte zero.
                yield
        work.check()
        self.hook("before_publish")
        result = self.repo.publish_finalize(
            session["upload_id"], session["revision"], total, digest.hexdigest(), lease
        )
        self.hook("published")
        return result

    def _scan_repairs(self):
        if self.stopping.is_set():
            return
        if self.repair_cursor is None and time.monotonic() < self.next_repair_scan:
            return
        page = self.repo.repair_scan_page(after=self.repair_cursor, limit=self.config.page_size)
        for deficit in page["deficits"]:
            if self.stopping.is_set():
                return
            if deficit["eligible_nodes"] <= 0:
                log.warning(
                    "V2 chunk has zero eligible repair sources: file=%s chunk=%s",
                    deficit["file_id"],
                    deficit["chunk_id"],
                )
                continue
            self.repo.enqueue_repair(deficit["file_id"], deficit["version_id"], deficit["chunk_id"])
        self.repair_cursor = page["next_after"]
        if self.repair_cursor is None:
            self.next_repair_scan = time.monotonic() + self.config.repair_scan_seconds

    def tick(self):
        for future in list(self.futures):
            if future.done():
                kind = self.futures.pop(future)
                job_id = self.executing.pop(future)
                done, _ = future.result()  # Unexpected failures stop readiness.
                if done:
                    _, steps = self.resident.pop(job_id)
                    steps.close()
                else:
                    self.queues[kind].append(job_id)
        if self.stopping.is_set():
            return
        self.repo.expire_sessions(self.config.page_size)
        self.repo.sweep_claims(self.config.page_size)
        self._scan_repairs()
        # Per-kind claims prevent an older, continuously replenished backlog
        # from hiding the other kinds. Both claims and continuations are bounded.
        for offset in range(len(KINDS)):
            kind = KINDS[(self.claim_cursor + offset) % len(KINDS)]
            if sum(k == kind for k, _ in self.resident.values()) >= self.config.jobs_per_kind:
                continue
            jobs = self.repo.claim_jobs(self.owner, 1, kinds=(kind,), exclude=list(self.resident))
            if jobs:
                job = jobs[0]
                steps = self.steps(job)
                done, _ = self.advance(steps)
                if not done:
                    self.resident[job["job_id"]] = kind, steps
                    self.queues[kind].append(job["job_id"])
        self.claim_cursor = (self.claim_cursor + 1) % len(KINDS)
        while len(self.futures) < self.config.slots:
            for offset in range(len(KINDS)):
                index = (self.dispatch_cursor + offset) % len(KINDS)
                kind = KINDS[index]
                cap = self.config.finalize_slots if kind == "FINALIZE" else self.config.slots - 1
                if self.queues[kind] and list(self.futures.values()).count(kind) < cap:
                    job_id = self.queues[kind].popleft()
                    future = self.executor.submit(self.advance, self.resident[job_id][1])
                    self.futures[future], self.executing[future] = kind, job_id
                    self.dispatch_cursor = (index + 1) % len(KINDS)
                    break
            else:
                break

    def run(self):
        if not self.consumer_slot.acquire(blocking=False):
            raise ValueError("Worker already running a consumer")
        try:
            self.repo.ready()
            self.running.set()
            while not self.stopping.is_set():
                if self.health_future is not None and self.health_future.done():
                    self.health_future.result()
                    self.health_future = None
                if self.health_future is None:
                    self.health_future = self.health_executor.submit(self.poll_health)
                self.tick()
                self.stopping.wait(self.config.poll_seconds)
        finally:
            self.running.clear()
            try:
                self.close()
            finally:
                self.consumer_slot.release()

    def rejoin_node(self, ticket, observation=None):
        if self.stopping.is_set():
            return "STOPPED"
        if observation is None:
            observation = self.storage.health(ticket, self.stopping)
            if observation is None or self.stopping.is_set():
                return "STOPPED"
        if observation.outcome != "reachable" or ticket["health_state"] != "DOWN":
            self.repo.apply_health_poll(ticket, observation)
            return (
                "QUARANTINED" if observation.outcome == "scope_mismatch" else ticket["health_state"]
            )
        after_sequence = 0
        upper_sequence = None
        while not self.stopping.is_set():
            try:
                page = self.storage.list_allocations(
                    ticket,
                    after_sequence=after_sequence,
                    upper_sequence=upper_sequence,
                    limit=self.config.page_size,
                    stopping=self.stopping,
                )
            except DomainError as exc:
                outcome = (
                    "scope_mismatch"
                    if exc.code in ("NODE_SCOPE_MISMATCH", "INVALID_STORAGE_INVENTORY")
                    else "unreachable"
                )
                self.repo.apply_health_poll(ticket, HealthObservation(outcome))
                self.next_repair_scan = 0.0
                return "QUARANTINED" if outcome == "scope_mismatch" else "DOWN"
            if page is None or self.stopping.is_set():
                return "STOPPED"
            try:
                status = self.repo.reconcile_rejoin_page(
                    ticket,
                    page,
                    after_sequence=after_sequence,
                    upper_sequence=upper_sequence,
                    available_bytes=observation.available_bytes,
                    writable=observation.writable,
                    recovery_required=observation.recovery_required,
                )
            except DomainError as exc:
                if exc.code == "INVALID_STORAGE_INVENTORY":
                    self.repo.apply_health_poll(ticket, HealthObservation("scope_mismatch"))
                    self.next_repair_scan = 0.0
                    return "QUARANTINED"
                raise
            if status != "CONTINUE":
                if status in ("ACTIVE", "QUARANTINED"):
                    self.next_repair_scan = 0.0
                return status
            upper_sequence = page.upper_sequence
            after_sequence = page.next_after_sequence
        return "STOPPED"

    def poll_health(self, *, node_id=None):
        if self.stopping.is_set():
            return None
        try:
            ticket = (
                self.repo.claim_health_poll()
                if node_id is None
                else self.repo.claim_health_poll(node_id=node_id)
            )
            if ticket is None or self.stopping.is_set():
                return None
            observation = self.storage.health(ticket, self.stopping)
            if observation is None or self.stopping.is_set():
                return None
            if ticket.get("health_state") == "DOWN" and observation.outcome == "reachable":
                return self.rejoin_node(ticket, observation)
            self.repo.apply_health_poll(ticket, observation)
            if observation.outcome != "reachable":
                self.next_repair_scan = 0.0
            return observation.outcome
        except (DomainError, SQLAlchemyError) as exc:
            log.warning("V2 node health poll deferred: %s", type(exc).__name__)
            return None

    def close(self):
        self.stopping.set()
        due = time.monotonic() + self.config.shutdown_seconds
        self.shutdown_due = due
        self.executor.shutdown(wait=False, cancel_futures=True)
        self.health_executor.shutdown(wait=False, cancel_futures=True)
        # Executor shutdown can leave a queued Future in CANCELLED (not yet
        # CANCELLED_AND_NOTIFIED). wait() would retain it until timeout despite
        # cancelled()/done() being true; it owns no running quantum to drain.
        active = [future for future in self.futures if not future.cancelled()]
        _, pending = wait(active, timeout=max(0, due - time.monotonic()))
        busy = {self.executing[future] for future in pending}
        for job_id, (_, steps) in list(self.resident.items()):
            if job_id not in busy:
                steps.close()
                self.resident.pop(job_id)
        if pending:
            raise RuntimeError("V2 Worker did not drain within shutdown budget")
        if self.health_future is not None and not self.health_future.done():
            _, pending_health = wait([self.health_future], timeout=max(0, due - time.monotonic()))
            if pending_health:
                raise RuntimeError("V2 health poll did not drain within shutdown budget")
        if not self.consumer_done.wait(max(0, due - time.monotonic())):
            raise RuntimeError("V2 consumer did not drain within shutdown budget")
        self.futures.clear()
        self.executing.clear()
        for queue in self.queues.values():
            queue.clear()
        self.running.clear()


def main():
    logging.basicConfig(level=logging.INFO)
    engine = create_engine(
        os.environ["V2_DATABASE_URL"],
        pool_size=8,
        max_overflow=0,
        pool_timeout=5,
        connect_args={"connect_timeout": 5},
    )
    worker = Worker(
        Repository(
            engine,
            UUID(os.environ["V2_NAMESPACE_ID"]),
            UUID(os.environ["V2_RECOVERY_EPOCH"]),
            schema=os.environ.get("V2_DATABASE_SCHEMA", "public"),
        )
    )
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: worker.stopping.set())
    try:
        worker.run()
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
