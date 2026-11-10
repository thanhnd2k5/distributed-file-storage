"""Short V2 domain transactions. No network/filesystem I/O in transactions.

This is an internal foundation, not HTTP/Transfer/Worker runtime. Callers do I/O
only after methods return (commit), and submit verified wire evidence separately.
"""

import hashlib
import json
import re
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from uuid import UUID, uuid4

from sqlalchemy import text

from metadata.v2.health import next_health


class DomainError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def execute(db, query, **parameters):
    return db.execute(text(query), parameters)


def positive(value, maximum=2**63 - 1, *, zero=False):
    if type(value) is not int or not (0 if zero else 1) <= value <= maximum:
        raise ValueError("Integer outside configured range")
    return value


def checksum(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError("Invalid SHA-256")
    return value


@dataclass(frozen=True)
class Policy:
    chunk_size_bytes: int = 8388608
    replication_factor: int = 2
    min_distinct_domains: int = 2
    max_parallel_parts: int = 2
    max_file_bytes: int = 64 * 1024**3
    max_parts: int = 131072

    def __post_init__(self):
        for value in asdict(self).values():
            positive(value)
        if self.min_distinct_domains > self.replication_factor:
            raise ValueError("Domain count exceeds RF")


@dataclass(frozen=True)
class DeleteEvidence:
    namespace_id: UUID
    recovery_epoch: UUID
    node_id: str
    incarnation: UUID
    allocation_id: UUID
    generation: int
    state: str
    cleanup_complete: bool


@dataclass(frozen=True)
class JobLease:
    job_id: UUID
    owner: UUID
    token: int


@dataclass(frozen=True)
class AllocationEvidence:
    """Verified Prepare/Store/Inspect response, supplied by an internal caller.

    A PREPARED Prepare response is a drain acknowledgement, not a raw Inspect
    observation. Callers must perform that RPC outside the DB transaction.
    """

    namespace_id: UUID
    recovery_epoch: UUID
    node_id: str
    incarnation: UUID
    allocation_id: UUID
    generation: int
    chunk_id: UUID
    size_bytes: int
    checksum_sha256: str
    state: str
    actual_size: int | None = None
    actual_hash: str | None = None

    def __post_init__(self):
        for value in (
            self.namespace_id,
            self.recovery_epoch,
            self.incarnation,
            self.allocation_id,
            self.chunk_id,
        ):
            if not isinstance(value, UUID):
                raise ValueError("Evidence IDs must be UUIDs")
        positive(self.generation)
        positive(self.size_bytes)
        checksum(self.checksum_sha256)
        if self.actual_size is not None:
            positive(self.actual_size, zero=True)
        if self.actual_hash is not None:
            checksum(self.actual_hash)


class Repository:
    def __init__(
        self,
        engine,
        namespace_id: UUID,
        recovery_epoch: UUID,
        *,
        schema="public",
        policy=None,
        max_job_backlog=1024,
    ):
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", schema):
            raise ValueError("Invalid schema")
        self.engine = engine
        self.namespace_id = namespace_id
        self.epoch = recovery_epoch
        self.schema = schema
        self.policy = policy or Policy()
        self.max_job_backlog = positive(max_job_backlog)

    @contextmanager
    def transaction(self):
        with self.engine.begin() as db:
            execute(db, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            execute(db, f'SET LOCAL search_path TO "{self.schema}"')
            execute(db, "SET LOCAL lock_timeout='3s'")
            execute(db, "SET LOCAL statement_timeout='10s'")
            control = (
                execute(db, "SELECT * FROM cluster_control WHERE singleton=1 FOR SHARE")
                .mappings()
                .one()
            )
            if (control["namespace_id"], control["recovery_epoch"], control["mode"]) != (
                self.namespace_id,
                self.epoch,
                "ACTIVE",
            ):
                raise DomainError("RECOVERY_REQUIRED")
            yield db

    def ready(self):
        with self.transaction():
            return True

    @staticmethod
    def _file(db, file_id, *, lock=False):
        row = (
            execute(
                db,
                "SELECT * FROM files WHERE file_id=:file" + (" FOR UPDATE" if lock else ""),
                file=file_id,
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise DomainError("FILE_NOT_FOUND")
        return dict(row)

    @staticmethod
    def _available(file):
        if file["state"] in ("DELETING", "DELETED"):
            raise DomainError("FILE_NOT_FOUND")
        if file["state"] != "AVAILABLE":
            raise DomainError("FILE_NOT_READY")

    @staticmethod
    def _download_file(db, file):
        policy = (
            execute(
                db,
                "SELECT chunk_size_bytes,part_count,replication_factor FROM upload_sessions "
                "WHERE file_id=:file",
                file=file["file_id"],
            )
            .mappings()
            .one()
        )
        return dict(file, **policy)

    def resolve_download(self, file_id):
        """Metadata for HEAD/preconditions; acquire must recheck before sending GET bytes."""
        with self.transaction() as db:
            file = self._file(db, file_id)
            self._available(file)
            return self._download_file(db, file)

    def file_record(self, file_id):
        with self.transaction() as db:
            file = self._file(db, file_id)
            if file["state"] in ("DELETING", "DELETED"):
                raise DomainError("FILE_NOT_FOUND")
            return file

    def file_catalog(self, *, after=None, limit=50):
        positive(limit, 100)
        with self.transaction() as db:
            rows = [
                dict(row)
                for row in execute(
                    db,
                    "SELECT * FROM files WHERE state='AVAILABLE' "
                    + ("AND (created_at,file_id)<(:created,:file) " if after else "")
                    + "ORDER BY created_at DESC,file_id DESC LIMIT :limit",
                    limit=limit + 1,
                    **(dict(created=after[0], file=after[1]) if after else {}),
                ).mappings()
            ]
            return rows[:limit], len(rows) > limit

    def catalog_manifest(self, file_id, *, after_index=-1, limit=100):
        positive(limit, 1000)
        if type(after_index) is not int or not -1 <= after_index <= 2**53 - 1:
            raise ValueError("Invalid manifest cursor")
        with self.transaction() as db:
            file = self._file(db, file_id, lock=True)
            self._available(file)
            metadata = self._download_file(db, file)
            rows = [
                dict(row)
                for row in execute(
                    db,
                    "SELECT * FROM manifest_parts WHERE version_id=:version "
                    "AND part_index>:after ORDER BY part_index LIMIT :limit",
                    version=file["version_id"],
                    after=after_index,
                    limit=limit + 1,
                ).mappings()
            ]
            for part in rows[:limit]:
                part["replicas"] = [
                    dict(row)
                    for row in execute(
                        db,
                        """SELECT DISTINCT ON (a.node_id) a.node_id,
                        a.failure_domain_snapshot AS failure_domain,a.state,a.replica_health,
                        a.observed_at,n.health_state AS node_health FROM allocations a
                        JOIN storage_nodes n ON n.node_id=a.node_id
                        WHERE a.chunk_id=:chunk AND a.state='STORED'
                        ORDER BY a.node_id,a.allocation_id LIMIT :rf""",
                        chunk=part["chunk_id"],
                        rf=metadata["replication_factor"],
                    ).mappings()
                ]
            return dict(
                file_id=file_id,
                version_id=file["version_id"],
                items=rows[:limit],
                has_more=len(rows) > limit,
                next_after_index=rows[limit - 1]["part_index"] if len(rows) > limit else None,
            )

    def acquire_read(self, file_id, *, version_id=None):
        with self.transaction() as db:
            file = self._file(db, file_id, lock=True)
            self._available(file)
            if version_id is not None and file["version_id"] != version_id:
                raise DomainError("PRECONDITION_FAILED")
            metadata = self._download_file(db, file)
            # Sample the DB clock AFTER the file lock and metadata queries.
            now = execute(db, "SELECT clock_timestamp()").scalar_one()
            row = (
                execute(
                    db,
                    """INSERT INTO download_leases (read_id,file_id,version_id,lease_token,
                    state,created_at,expires_at,absolute_expires_at) VALUES
                    (:read,:file,:version,:token,'ACTIVE',:now,
                    :now+interval '30 seconds',:now+interval '24 hours') RETURNING *""",
                    read=uuid4(),
                    file=file_id,
                    version=file["version_id"],
                    token=uuid4(),
                    now=now,
                )
                .mappings()
                .one()
            )
            return dict(row, file=metadata, db_now=now)

    def _lock_read(self, db, read_id, token):
        file_id = execute(
            db, "SELECT file_id FROM download_leases WHERE read_id=:read", read=read_id
        ).scalar_one_or_none()
        if file_id is None:
            raise DomainError("READ_LEASE_NOT_FOUND")
        file = self._file(db, file_id, lock=True)
        row = (
            execute(
                db,
                "SELECT * FROM download_leases WHERE read_id=:read FOR UPDATE",
                read=read_id,
            )
            .mappings()
            .one()
        )
        if row["lease_token"] != token or row["version_id"] != file["version_id"]:
            raise DomainError("STALE_READ_LEASE")
        return file, dict(row)

    @staticmethod
    def _live_read(db, file, lease):
        now = execute(db, "SELECT clock_timestamp()").scalar_one()
        if (
            file["state"] not in ("AVAILABLE", "DELETING")
            or lease["state"] != "ACTIVE"
            or now >= min(lease["expires_at"], lease["absolute_expires_at"])
        ):
            raise DomainError("STALE_READ_LEASE")
        return now

    def renew_read(self, read_id, token):
        with self.transaction() as db:
            file, lease = self._lock_read(db, read_id, token)
            self._live_read(db, file, lease)
            row = (
                execute(
                    db,
                    """UPDATE download_leases SET expires_at=LEAST(absolute_expires_at,
                    clock_timestamp()+interval '30 seconds') WHERE read_id=:read
                    AND expires_at>clock_timestamp() AND absolute_expires_at>clock_timestamp()
                    RETURNING *""",
                    read=read_id,
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise DomainError("STALE_READ_LEASE")
            now = self._live_read(db, file, row)
            return dict(row, db_now=now)

    def release_read(self, read_id, token):
        """Idempotent for the same token, including an expired lease; never renews it."""
        with self.transaction() as db:
            _, lease = self._lock_read(db, read_id, token)
            if lease["state"] == "RELEASED":
                return False
            execute(
                db,
                "UPDATE download_leases SET state='RELEASED',released_at=clock_timestamp() "
                "WHERE read_id=:read",
                read=read_id,
            )
            return True

    def read_manifest(self, read_id, token, *, after_index=-1, limit=100):
        positive(limit, 1000)
        if type(after_index) is not int or not -1 <= after_index <= 2**53 - 1:
            raise ValueError("Invalid manifest cursor")
        with self.transaction() as db:
            file, lease = self._lock_read(db, read_id, token)
            self._live_read(db, file, lease)
            metadata = self._download_file(db, file)
            rows = [
                dict(row)
                for row in execute(
                    db,
                    "SELECT * FROM manifest_parts WHERE version_id=:version "
                    "AND part_index>:after ORDER BY part_index LIMIT :limit",
                    version=lease["version_id"],
                    after=after_index,
                    limit=limit + 1,
                ).mappings()
            ]
            parts = rows[:limit]
            for part in parts:
                # One allocation per current node, capped by the immutable RF.
                # History/observations may change; the manifest spec never does.
                part["destinations"] = [
                    dict(
                        row,
                        namespace_id=self.namespace_id,
                        chunk_id=part["chunk_id"],
                        size_bytes=part["size_bytes"],
                        checksum_sha256=part["checksum_sha256"],
                    )
                    for row in execute(
                        db,
                        """SELECT DISTINCT ON (a.node_id) a.allocation_id,a.node_id,
                        a.incarnation,a.recovery_epoch,a.generation,n.host,n.port,
                        a.failure_domain_snapshot FROM allocations a JOIN storage_nodes n
                        ON n.node_id=a.node_id AND n.active_incarnation=a.incarnation
                        JOIN node_incarnations i ON i.node_id=a.node_id
                        AND i.incarnation=a.incarnation
                        WHERE a.chunk_id=:chunk AND a.state='STORED' AND a.replica_health='VERIFIED'
                        AND a.ack_size=:size AND a.ack_hash=:hash
                        AND a.recovery_epoch=:epoch AND i.recovery_epoch=:epoch
                        AND n.enabled AND n.health_state='ACTIVE' AND i.state='ACTIVE'
                        ORDER BY a.node_id,a.allocation_id LIMIT :replicas""",
                        chunk=part["chunk_id"],
                        size=part["size_bytes"],
                        hash=part["checksum_sha256"],
                        epoch=self.epoch,
                        replicas=metadata["replication_factor"],
                    ).mappings()
                ]
            now = self._live_read(db, file, lease)
            return dict(
                version_id=lease["version_id"],
                items=parts,
                has_more=len(rows) > limit,
                next_after_index=parts[-1]["part_index"] if len(rows) > limit else None,
                expires_at=lease["expires_at"],
                db_now=now,
            )

    @staticmethod
    def _has_live_reads(db, file_id):
        return execute(
            db,
            "SELECT EXISTS(SELECT 1 FROM download_leases WHERE file_id=:file "
            "AND state='ACTIVE' AND expires_at>clock_timestamp())",
            file=file_id,
        ).scalar_one()

    def delete_file(self, file_id):
        """Tombstone and enqueue atomically; physical fences/credit belong to cleanup."""
        with self.transaction() as db:
            execute(
                db,
                "SELECT resource_key FROM admission_counters WHERE "
                "resource_key='upload_sessions' FOR UPDATE",
            ).one()
            file = self._file(db, file_id, lock=True)
            if file["state"] not in ("DELETING", "DELETED"):
                upload_id = execute(
                    db, "SELECT upload_id FROM upload_sessions WHERE file_id=:file", file=file_id
                ).scalar_one()
                session = self._lock_session(db, upload_id)
                execute(
                    db,
                    "UPDATE files SET state='DELETING',revision=revision+1,"
                    "deleted_at=clock_timestamp() WHERE file_id=:file",
                    file=file_id,
                )
                if session["state"] in ("OPEN", "FINALIZING"):
                    # Terminal session/revision fences confirm, retry and late publication.
                    # Keep attempts/charges until durable Storage Delete acknowledgements.
                    execute(
                        db,
                        "UPDATE upload_sessions SET state='ABORTED',revision=revision+1 "
                        "WHERE upload_id=:upload",
                        upload=upload_id,
                    )
                if session["admission_released_at"] is None:
                    execute(
                        db,
                        "UPDATE upload_sessions SET admission_released_at=clock_timestamp() "
                        "WHERE upload_id=:upload",
                        upload=upload_id,
                    )
                    execute(
                        db,
                        "UPDATE admission_counters SET held_count=held_count-1,"
                        "revision=revision+1 WHERE resource_key='upload_sessions'",
                    )
                self.enqueue(db, "CLEANUP", file_id)
                file = self._file(db, file_id)
            return dict(
                file_id=file_id, state=file["state"], cleanup_pending=file["state"] != "DELETED"
            )

    def _cleanup_session(self, db, session):
        if session["state"] in ("ABORTED", "EXPIRED", "FAILED"):
            return
        file = self._file(db, session["file_id"])
        if session["state"] != "COMPLETED" or file["state"] not in ("DELETING", "DELETED"):
            raise DomainError("UPLOAD_STATE_CONFLICT")

    @staticmethod
    def session(db, upload_id):
        row = (
            execute(
                db,
                """SELECT s.*,f.version_id,f.original_name,f.content_type,
            f.size_bytes,f.expected_sha256,f.created_at FROM upload_sessions s
            JOIN files f USING(file_id) WHERE upload_id=:upload""",
                upload=upload_id,
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise DomainError("UPLOAD_NOT_FOUND")
        return dict(row)

    def get_session(self, upload_id):
        with self.transaction() as db:
            return self.session(db, upload_id)

    def upload_status(self, upload_id):
        """One bounded observation; never expose claim tokens or materialize a manifest."""
        with self.transaction() as db:
            session = self.session(db, upload_id)
            counts = (
                execute(
                    db,
                    "SELECT count(*) AS confirmed_parts,COALESCE(sum(size_bytes),0) "
                    "AS confirmed_bytes FROM upload_parts WHERE upload_id=:upload "
                    "AND state='VERIFIED'",
                    upload=upload_id,
                )
                .mappings()
                .one()
            )
            pending = execute(
                db,
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE kind='CLEANUP' "
                "AND target_id=:file AND state!='DONE')",
                file=session["file_id"],
            ).scalar_one()
            return dict(session, **{k: int(v) for k, v in counts.items()}, cleanup_pending=pending)

    def part_page(self, upload_id, *, after_index=-1, limit=100):
        positive(after_index + 1, 2**53, zero=True)
        positive(limit, 1000)
        with self.transaction() as db:
            self.session(db, upload_id)
            rows = (
                execute(
                    db,
                    "SELECT * FROM upload_parts WHERE upload_id=:upload "
                    "AND part_index>:after ORDER BY part_index LIMIT :limit",
                    upload=upload_id,
                    after=after_index,
                    limit=limit + 1,
                )
                .mappings()
                .all()
            )
            return [dict(row) for row in rows[:limit]], len(rows) > limit

    def part_record(self, upload_id, index):
        with self.transaction() as db:
            self.session(db, upload_id)
            row = (
                execute(
                    db,
                    "SELECT * FROM upload_parts WHERE upload_id=:upload AND part_index=:index",
                    upload=upload_id,
                    index=index,
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise DomainError("PARTS_INCOMPLETE")
            return dict(row)

    def create_upload(
        self,
        key: UUID,
        *,
        original_name,
        size_bytes,
        expected_sha256,
        content_type="application/octet-stream",
    ):
        if not isinstance(key, UUID):
            raise ValueError("Idempotency key must be UUID")
        positive(size_bytes, 2**53 - 1, zero=True)
        checksum(expected_sha256)
        if (
            not isinstance(original_name, str)
            or not 1 <= len(original_name) <= 255
            or any(ord(c) < 32 or ord(c) == 127 or c in "/\\" for c in original_name)
        ):
            raise ValueError("Invalid basename")
        if (
            not isinstance(content_type, str)
            or not 1 <= len(content_type) <= 255
            or any(ord(c) < 32 or ord(c) == 127 for c in content_type)
        ):
            raise ValueError("Invalid content type")
        request = dict(
            original_name=original_name,
            content_type=content_type,
            size_bytes=size_bytes,
            expected_sha256=expected_sha256,
        )
        digest = hashlib.sha256(
            json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        with self.transaction() as db:
            # Admission row serializes create and terminal slot release. Replay
            # precedes admission availability and current policy size checks.
            counter = (
                execute(
                    db,
                    "SELECT * FROM admission_counters WHERE "
                    "resource_key='upload_sessions' FOR UPDATE",
                )
                .mappings()
                .one()
            )
            previous = (
                execute(
                    db,
                    "SELECT * FROM idempotency_records WHERE "
                    "operation='CREATE_UPLOAD' AND key=:key",
                    key=key,
                )
                .mappings()
                .one_or_none()
            )
            if previous:
                if previous["request_sha256"] != digest:
                    raise DomainError("IDEMPOTENCY_CONFLICT")
                return self.session(db, previous["upload_id"]), True
            backlog = execute(
                db,
                "SELECT count(*) FROM (SELECT 1 FROM jobs WHERE state!='DONE' "
                "LIMIT :limit) pending",
                limit=self.max_job_backlog,
            ).scalar_one()
            if backlog >= self.max_job_backlog:
                raise DomainError("RESOURCE_LIMIT")
            policy = self.policy
            count = (size_bytes + policy.chunk_size_bytes - 1) // policy.chunk_size_bytes
            if size_bytes > policy.max_file_bytes or count > policy.max_parts:
                raise DomainError("FILE_TOO_LARGE")
            if counter["held_count"] >= counter["limit_count"]:
                raise DomainError("RESOURCE_LIMIT")
            upload, file_id, version = uuid4(), uuid4(), uuid4()
            execute(
                db,
                """INSERT INTO files (file_id,version_id,original_name,content_type,
                size_bytes,expected_sha256,state,revision)
                VALUES (:file,:version,:original_name,:content_type,:size_bytes,
                :expected_sha256,'UPLOADING',1)""",
                file=file_id,
                version=version,
                **request,
            )
            execute(
                db,
                """INSERT INTO upload_sessions (upload_id,file_id,state,revision,
                chunk_size_bytes,part_count,replication_factor,min_distinct_domains,
                max_parallel_parts,policy,active_claims,expires_at)
                VALUES (:upload,:file,'OPEN',1,:chunk,:count,:rf,:domains,:parallel,
                CAST(:policy AS jsonb),0,clock_timestamp()+interval '24 hours')""",
                upload=upload,
                file=file_id,
                chunk=policy.chunk_size_bytes,
                count=count,
                rf=policy.replication_factor,
                domains=policy.min_distinct_domains,
                parallel=policy.max_parallel_parts,
                policy=json.dumps(asdict(policy)),
            )
            execute(
                db,
                "INSERT INTO idempotency_records "
                "(operation,key,request_sha256,upload_id) VALUES "
                "('CREATE_UPLOAD',:key,:digest,:upload)",
                key=key,
                digest=digest,
                upload=upload,
            )
            execute(
                db,
                "UPDATE admission_counters SET "
                "held_count=held_count+1,revision=revision+1 WHERE "
                "resource_key='upload_sessions'",
            )
            return self.session(db, upload), False

    def provision_node(self, node_id, incarnation: UUID, *, host, port, failure_domain, budget):
        """Administrative registration. A new/replaced identity cannot self-activate."""
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", node_id):
            raise ValueError("Invalid node ID")
        positive(port, 65535)
        positive(budget, zero=True)
        if not 1 <= len(host) <= 255 or not 1 <= len(failure_domain) <= 255:
            raise ValueError("Invalid node endpoint/domain")
        with self.transaction() as db:
            old = (
                execute(
                    db, "SELECT * FROM storage_nodes WHERE node_id=:node FOR UPDATE", node=node_id
                )
                .mappings()
                .one_or_none()
            )
            if old is None:
                execute(
                    db,
                    """INSERT INTO storage_nodes VALUES
                    (:node,:host,:port,:domain,:inc,true,'QUARANTINED',0,NULL,NULL)""",
                    node=node_id,
                    host=host,
                    port=port,
                    domain=failure_domain,
                    inc=incarnation,
                )
            elif old["active_incarnation"] != incarnation:
                execute(
                    db,
                    "UPDATE storage_nodes SET "
                    "health_state='QUARANTINED',health_generation=health_generation+1 "
                    "WHERE node_id=:node",
                    node=node_id,
                )
            elif (old["host"], old["port"], old["failure_domain"]) != (host, port, failure_domain):
                raise DomainError("NODE_CONFIGURATION_CONFLICT")
            execute(
                db,
                """INSERT INTO node_incarnations (node_id,incarnation,recovery_epoch,state)
                VALUES (:node,:inc,:epoch,'JOINING') ON CONFLICT DO NOTHING""",
                node=node_id,
                inc=incarnation,
                epoch=self.epoch,
            )
            execute(
                db,
                """INSERT INTO node_budgets VALUES (:node,:inc,:budget,0,1)
                ON CONFLICT DO NOTHING""",
                node=node_id,
                inc=incarnation,
                budget=budget,
            )
            return old is None

    def activate_node(self, node_id, incarnation, *, observed_namespace, observed_epoch):
        """Explicit administrative approval after scope/ledger reconciliation.

        These arguments are verified Health/maintenance evidence from the caller,
        not an automatic health-poll promotion of a replacement disk.
        """
        if observed_namespace != self.namespace_id or observed_epoch != self.epoch:
            raise DomainError("NODE_SCOPE_MISMATCH")
        with self.transaction() as db:
            node = (
                execute(
                    db, "SELECT * FROM storage_nodes WHERE node_id=:node FOR UPDATE", node=node_id
                )
                .mappings()
                .one()
            )
            inc = (
                execute(
                    db,
                    "SELECT * FROM node_incarnations WHERE node_id=:node AND "
                    "incarnation=:inc FOR UPDATE",
                    node=node_id,
                    inc=incarnation,
                )
                .mappings()
                .one()
            )
            if inc["recovery_epoch"] != self.epoch or inc["state"] in ("RETIRED", "QUARANTINED"):
                raise DomainError("NODE_SCOPE_MISMATCH")
            if node["active_incarnation"] != incarnation:
                execute(
                    db,
                    "UPDATE node_incarnations SET "
                    "state='RETIRED',retired_at=clock_timestamp() WHERE node_id=:node "
                    "AND state='ACTIVE'",
                    node=node_id,
                )
            execute(
                db,
                "UPDATE node_incarnations SET state='ACTIVE' WHERE node_id=:node "
                "AND incarnation=:inc",
                node=node_id,
                inc=incarnation,
            )
            execute(
                db,
                "UPDATE storage_nodes SET "
                "active_incarnation=:inc,health_state='ACTIVE',"
                "health_generation=health_generation+1,"
                "health_observed_at=clock_timestamp(),available_bytes=NULL WHERE node_id=:node",
                node=node_id,
                inc=incarnation,
            )
            execute(
                db,
                """INSERT INTO node_health_polls (node_id) VALUES (:node)
                ON CONFLICT (node_id) DO UPDATE SET
                    next_poll_at=clock_timestamp(),ticket_expires_at=NULL,
                    consecutive_failures=0,consecutive_successes=0,writable=NULL""",
                node=node_id,
            )

    def claim_health_poll(self):
        """Claim one due node. Lock node before the poll row, including on activation."""
        with self.transaction() as db:
            node = (
                execute(
                    db,
                    """SELECT n.node_id,n.host,n.port,n.failure_domain,n.health_state,
                              n.active_incarnation AS incarnation
                    FROM node_health_polls p JOIN storage_nodes n USING (node_id)
                    JOIN node_incarnations i ON i.node_id=n.node_id
                        AND i.incarnation=n.active_incarnation
                    WHERE p.next_poll_at<=clock_timestamp() AND n.enabled
                        AND n.health_state!='QUARANTINED'
                        AND i.state='ACTIVE' AND i.recovery_epoch=:epoch
                    ORDER BY p.next_poll_at,n.node_id LIMIT 1 FOR UPDATE OF n SKIP LOCKED""",
                    epoch=self.epoch,
                )
                .mappings()
                .one_or_none()
            )
            if node is None:
                return None
            # The joined schedule could have changed before the node lock was acquired.
            if not execute(
                db,
                """UPDATE node_health_polls
                SET ticket_expires_at=statement_timestamp()+interval '5 seconds',
                    next_poll_at=statement_timestamp()+interval '5 seconds'
                WHERE node_id=:node AND next_poll_at<=clock_timestamp()""",
                node=node["node_id"],
            ).rowcount:
                return None
            generation = execute(
                db,
                """UPDATE storage_nodes SET health_generation=health_generation+1
                WHERE node_id=:node RETURNING health_generation""",
                node=node["node_id"],
            ).scalar_one()
            return dict(
                node,
                namespace_id=self.namespace_id,
                recovery_epoch=self.epoch,
                generation=generation,
            )

    def apply_health_poll(self, ticket, observation):
        if (ticket["namespace_id"], ticket["recovery_epoch"]) != (self.namespace_id, self.epoch):
            return False
        with self.transaction() as db:
            node = execute(
                db,
                """SELECT n.node_id FROM storage_nodes n
                JOIN node_incarnations i ON i.node_id=n.node_id
                    AND i.incarnation=n.active_incarnation
                WHERE n.node_id=:node_id AND n.enabled AND n.health_state=:health_state
                    AND n.host=:host AND n.port=:port AND n.failure_domain=:failure_domain
                    AND n.active_incarnation=:incarnation AND n.health_generation=:generation
                    AND i.state='ACTIVE' AND i.recovery_epoch=:recovery_epoch
                FOR UPDATE OF n""",
                **ticket,
            ).one_or_none()
            if node is None:
                return False
            poll = execute(
                db,
                """SELECT consecutive_failures,consecutive_successes FROM node_health_polls
                WHERE node_id=:node AND ticket_expires_at>clock_timestamp()""",
                node=ticket["node_id"],
            ).one_or_none()
            if poll is None:
                return False
            state, failures, successes = next_health(ticket["health_state"], *poll, observation)
            execute(
                db,
                """UPDATE node_health_polls SET ticket_expires_at=NULL,
                    next_poll_at=clock_timestamp()+interval '10 seconds',
                    consecutive_failures=:failures,consecutive_successes=:successes,
                    writable=:writable WHERE node_id=:node""",
                failures=failures,
                successes=successes,
                writable=observation.writable,
                node=ticket["node_id"],
            )
            execute(
                db,
                """UPDATE storage_nodes
                SET health_state=:state,health_observed_at=clock_timestamp(),
                    available_bytes=:available WHERE node_id=:node""",
                state=state,
                available=observation.available_bytes,
                node=ticket["node_id"],
            )
            return True

    def set_budget(self, node_id, incarnation, budget):
        positive(budget, zero=True)
        with self.transaction() as db:
            # Node configuration uses the same node->budget lock order as placement.
            execute(
                db, "SELECT node_id FROM storage_nodes WHERE node_id=:node FOR UPDATE", node=node_id
            ).one()
            row = (
                execute(
                    db,
                    "SELECT * FROM node_budgets WHERE node_id=:node AND "
                    "incarnation=:inc FOR UPDATE",
                    node=node_id,
                    inc=incarnation,
                )
                .mappings()
                .one()
            )
            if budget < row["charged_bytes"]:
                execute(
                    db,
                    "UPDATE storage_nodes SET "
                    "health_state='QUARANTINED',health_generation=health_generation+1 "
                    "WHERE node_id=:node",
                    node=node_id,
                )
                return False
            execute(
                db,
                "UPDATE node_budgets SET "
                "allocatable_bytes=:budget,revision=revision+1 WHERE "
                "node_id=:node AND incarnation=:inc",
                node=node_id,
                inc=incarnation,
                budget=budget,
            )
            return True

    def _lock_session(self, db, upload_id):
        found = execute(
            db, "SELECT file_id FROM upload_sessions WHERE upload_id=:upload", upload=upload_id
        ).scalar_one_or_none()
        if found is None:
            raise DomainError("UPLOAD_NOT_FOUND")
        execute(db, "SELECT file_id FROM files WHERE file_id=:file FOR UPDATE", file=found).one()
        row = (
            execute(
                db,
                "SELECT *,expires_at>clock_timestamp() AS live FROM "
                "upload_sessions WHERE upload_id=:upload FOR UPDATE",
                upload=upload_id,
            )
            .mappings()
            .one()
        )
        return dict(row)

    @staticmethod
    def _open(session):
        if session["state"] != "OPEN":
            raise DomainError("UPLOAD_STATE_CONFLICT")
        if not session["live"]:
            raise DomainError("UPLOAD_EXPIRED")

    def claim_part(self, upload_id, index, size_bytes, sha256, owner: UUID):
        positive(index, 2**53 - 1, zero=True)
        positive(size_bytes)
        checksum(sha256)
        with self.transaction() as db:
            session = self._lock_session(db, upload_id)
            self._open(session)
            if index >= session["part_count"]:
                raise ValueError("Part index outside manifest")
            file_size = execute(
                db, "SELECT size_bytes FROM files WHERE file_id=:file", file=session["file_id"]
            ).scalar_one()
            expected = min(
                session["chunk_size_bytes"], file_size - index * session["chunk_size_bytes"]
            )
            if size_bytes != expected:
                raise DomainError("PART_SIZE_MISMATCH")
            part = (
                execute(
                    db,
                    "SELECT * FROM upload_parts WHERE upload_id=:upload AND "
                    "part_index=:index FOR UPDATE",
                    upload=upload_id,
                    index=index,
                )
                .mappings()
                .one_or_none()
            )
            if part:
                if (part["size_bytes"], part["checksum_sha256"]) != (size_bytes, sha256):
                    raise DomainError("PART_CONFLICT")
                if part["state"] == "VERIFIED":
                    return dict(part, destinations=[])
                # Reconciliation must resolve/fence the existing allocations first.
                raise DomainError(
                    "OUTCOME_PENDING" if part["state"] == "RECONCILING" else "PART_BUSY"
                )
            if session["active_claims"] >= session["max_parallel_parts"]:
                raise DomainError("RESOURCE_LIMIT")
            # Lock eligible nodes first (ordered), then all their budgets in the
            # prescribed order. No health observation can grant new capacity.
            nodes = (
                execute(
                    db,
                    """SELECT n.* FROM storage_nodes n JOIN node_incarnations i
                ON i.node_id=n.node_id AND i.incarnation=n.active_incarnation
                WHERE n.enabled AND n.health_state='ACTIVE' AND i.state='ACTIVE'
                AND i.recovery_epoch=:epoch ORDER BY n.node_id FOR SHARE OF n,i""",
                    epoch=self.epoch,
                )
                .mappings()
                .all()
            )
            candidates = []
            for node in nodes:
                budget = (
                    execute(
                        db,
                        "SELECT * FROM node_budgets WHERE node_id=:node AND "
                        "incarnation=:inc FOR UPDATE",
                        node=node["node_id"],
                        inc=node["active_incarnation"],
                    )
                    .mappings()
                    .one()
                )
                if budget["allocatable_bytes"] - budget["charged_bytes"] >= size_bytes:
                    candidates.append(node)
            selected = []
            domains = set()
            for node in candidates:
                if node["failure_domain"] not in domains:
                    selected.append(node)
                    domains.add(node["failure_domain"])
                    if len(domains) == session["min_distinct_domains"]:
                        break
            if len(domains) < session["min_distinct_domains"]:
                raise DomainError("INSUFFICIENT_DOMAINS")
            for node in candidates:
                if len(selected) >= session["replication_factor"]:
                    break
                if node not in selected:
                    selected.append(node)
            if len(selected) < session["replication_factor"]:
                raise DomainError("CAPACITY_UNAVAILABLE")
            chunk, token = uuid4(), uuid4()
            execute(
                db,
                """INSERT INTO upload_parts (chunk_id,upload_id,part_index,size_bytes,
                checksum_sha256,state,revision,claim_held,claim_token,claim_owner,claim_expires_at)
                VALUES (:chunk,:upload,:index,:size,:hash,'CLAIMED',1,true,:token,:owner,
                clock_timestamp()+interval '60 seconds')""",
                chunk=chunk,
                upload=upload_id,
                index=index,
                size=size_bytes,
                hash=sha256,
                token=token,
                owner=owner,
            )
            destinations = []
            for node in selected:
                aid = uuid4()
                execute(
                    db,
                    """UPDATE node_budgets SET charged_bytes=charged_bytes+:size,
                    revision=revision+1 WHERE node_id=:node AND incarnation=:inc""",
                    size=size_bytes,
                    node=node["node_id"],
                    inc=node["active_incarnation"],
                )
                execute(
                    db,
                    """INSERT INTO allocations (allocation_id,chunk_id,node_id,
                    incarnation,recovery_epoch,generation,failure_domain_snapshot,charged_bytes,
                    state,replica_health,observation_generation)
                    VALUES (:aid,:chunk,:node,:inc,:epoch,1,:domain,:size,
                    'RESERVED','UNKNOWN',0)""",
                    aid=aid,
                    chunk=chunk,
                    node=node["node_id"],
                    inc=node["active_incarnation"],
                    epoch=self.epoch,
                    domain=node["failure_domain"],
                    size=size_bytes,
                )
                execute(
                    db,
                    """INSERT INTO write_attempts (attempt_id,allocation_id,generation,
                    owner_token,state,lease_expires_at) VALUES (:id,:aid,1,:token,'INTENDED',
                    clock_timestamp()+interval '60 seconds')""",
                    id=uuid4(),
                    aid=aid,
                    token=token,
                )
                destinations.append(
                    dict(
                        node_id=node["node_id"],
                        incarnation=node["active_incarnation"],
                        allocation_id=aid,
                        generation=1,
                        recovery_epoch=self.epoch,
                        namespace_id=self.namespace_id,
                        chunk_id=chunk,
                        size_bytes=size_bytes,
                        checksum_sha256=sha256,
                        host=node["host"],
                        port=node["port"],
                    )
                )
            execute(
                db,
                "UPDATE upload_sessions SET active_claims=active_claims+1 WHERE upload_id=:upload",
                upload=upload_id,
            )
            part = (
                execute(db, "SELECT * FROM upload_parts WHERE chunk_id=:chunk", chunk=chunk)
                .mappings()
                .one()
            )
            return dict(part, destinations=destinations)

    @staticmethod
    def _lock_part(db, upload_id, index):
        positive(index, 2**53 - 1, zero=True)
        part = (
            execute(
                db,
                "SELECT * FROM upload_parts WHERE upload_id=:upload "
                "AND part_index=:index FOR UPDATE",
                upload=upload_id,
                index=index,
            )
            .mappings()
            .one_or_none()
        )
        if part is None:
            raise DomainError("PARTS_INCOMPLETE")
        return dict(part)

    def _live_claim(self, db, session, part, token, revision, *, state="CLAIMED"):
        positive(revision)
        if (part["claim_token"], part["revision"], part["state"]) != (token, revision, state):
            raise DomainError("STALE_CLAIM")
        # Recheck the DB clock after domain locks, never a timestamp from before a wait.
        self._open(self._lock_session(db, session["upload_id"]))
        live = execute(
            db,
            "SELECT claim_expires_at>clock_timestamp() FROM upload_parts WHERE chunk_id=:chunk",
            chunk=part["chunk_id"],
        ).scalar_one()
        if not live or (state == "CLAIMED" and not part["claim_held"]):
            raise DomainError("STALE_CLAIM")

    def renew_part(self, upload_id, index, token, revision):
        with self.transaction() as db:
            session = self._lock_session(db, upload_id)
            self._open(session)
            part = self._lock_part(db, upload_id, index)
            self._live_claim(db, session, part, token, revision)
            expires = execute(
                db,
                "UPDATE upload_parts SET claim_expires_at=LEAST("
                "clock_timestamp()+interval '60 seconds',:session_expiry) "
                "WHERE chunk_id=:chunk RETURNING claim_expires_at",
                session_expiry=session["expires_at"],
                chunk=part["chunk_id"],
            ).scalar_one()
            execute(
                db,
                "UPDATE write_attempts SET lease_expires_at=:expires WHERE "
                "allocation_id IN (SELECT allocation_id FROM allocations WHERE chunk_id=:chunk) "
                "AND owner_token=:token AND state IN ('INTENDED','IN_FLIGHT')",
                expires=expires,
                chunk=part["chunk_id"],
                token=token,
            )
            return expires

    def _lock_replica_nodes(self, db, upload_id):
        # Configuration/health cannot change while eligibility is checked. File/session
        # ownership serializes allocation mutations; node locks precede allocation locks.
        execute(
            db,
            """SELECT n.node_id FROM storage_nodes n JOIN node_incarnations i
            ON i.node_id=n.node_id AND i.incarnation=n.active_incarnation
            WHERE EXISTS (SELECT 1 FROM allocations a JOIN upload_parts p USING(chunk_id)
            WHERE p.upload_id=:upload AND a.node_id=n.node_id)
            ORDER BY n.node_id FOR SHARE OF n,i""",
            upload=upload_id,
        ).all()

    @staticmethod
    def _allocations(db, part):
        return [
            dict(row)
            for row in execute(
                db,
                "SELECT * FROM allocations WHERE "
                "chunk_id=:chunk AND state!='RELEASED' ORDER BY allocation_id FOR UPDATE",
                chunk=part["chunk_id"],
            ).mappings()
        ]

    def _check_acks(self, db, part, allocations, proofs, *, prepared=False):
        if len(proofs) != len(allocations) or len({p.allocation_id for p in proofs}) != len(proofs):
            raise DomainError("INVALID_ALLOCATION_ACK")
        by_id = {p.allocation_id: p for p in proofs}
        for allocation in allocations:
            proof = by_id.get(allocation["allocation_id"])
            if (
                proof is None
                or (
                    proof.namespace_id,
                    proof.recovery_epoch,
                    proof.node_id,
                    proof.incarnation,
                    proof.allocation_id,
                    proof.generation,
                    proof.chunk_id,
                    proof.size_bytes,
                    proof.checksum_sha256,
                )
                != (
                    self.namespace_id,
                    self.epoch,
                    allocation["node_id"],
                    allocation["incarnation"],
                    allocation["allocation_id"],
                    allocation["generation"],
                    part["chunk_id"],
                    part["size_bytes"],
                    part["checksum_sha256"],
                )
                or allocation["recovery_epoch"] != self.epoch
                or allocation["state"] not in ("RESERVED", "WRITING", "UNKNOWN", "STORED")
            ):
                raise DomainError("INVALID_ALLOCATION_ACK")
            if proof.state == "STORED":
                if (proof.actual_size, proof.actual_hash) != (
                    part["size_bytes"],
                    part["checksum_sha256"],
                ):
                    raise DomainError("INVALID_ALLOCATION_ACK")
            elif not prepared or proof.state != "PREPARED":
                raise DomainError("INVALID_ALLOCATION_ACK")
            attempt = (
                execute(
                    db,
                    "SELECT owner_token,state FROM write_attempts WHERE "
                    "allocation_id=:aid AND generation=:generation FOR UPDATE",
                    aid=proof.allocation_id,
                    generation=proof.generation,
                )
                .mappings()
                .one_or_none()
            )
            if attempt is None or attempt["owner_token"] != part["claim_token"]:
                raise DomainError("INVALID_ALLOCATION_ACK")
            if prepared and (allocation["state"] != "UNKNOWN" or attempt["state"] != "INTENDED"):
                raise DomainError("INVALID_ALLOCATION_ACK")
        return by_id

    def _enough_replicas(self, db, session, chunk_id):
        counts = (
            execute(
                db,
                """SELECT count(DISTINCT a.node_id) AS nodes,
            count(DISTINCT a.failure_domain_snapshot) AS domains
            FROM allocations a JOIN upload_parts p USING(chunk_id)
            JOIN storage_nodes n ON n.node_id=a.node_id AND n.active_incarnation=a.incarnation
            JOIN node_incarnations i ON i.node_id=a.node_id AND i.incarnation=a.incarnation
            WHERE a.chunk_id=:chunk AND a.recovery_epoch=:epoch AND i.recovery_epoch=:epoch
            AND n.enabled AND n.health_state='ACTIVE' AND i.state='ACTIVE'
            AND a.state='STORED' AND a.replica_health='VERIFIED'
            AND a.ack_size=p.size_bytes AND a.ack_hash=p.checksum_sha256""",
                chunk=chunk_id,
                epoch=self.epoch,
            )
            .mappings()
            .one()
        )
        return (
            counts["nodes"] >= session["replication_factor"]
            and counts["domains"] >= session["min_distinct_domains"]
        )

    def _record_acks(self, db, part, allocations, by_id):
        for allocation in allocations:
            proof = by_id[allocation["allocation_id"]]
            is_stored = proof.state == "STORED"
            execute(
                db,
                """UPDATE allocations SET state=:state,replica_health=:health,
                ack_size=:size,ack_hash=:hash,acked_at=CASE WHEN :stored THEN clock_timestamp()
                ELSE NULL END,observation_generation=generation,observed_at=clock_timestamp()
                WHERE allocation_id=:aid""",
                state="STORED" if is_stored else "RESERVED",
                health="VERIFIED" if is_stored else "UNKNOWN",
                size=proof.actual_size if is_stored else None,
                hash=proof.actual_hash if is_stored else None,
                stored=is_stored,
                aid=proof.allocation_id,
            )
            execute(
                db,
                "UPDATE write_attempts SET state=:state,finished_at=CASE WHEN :stored "
                "THEN clock_timestamp() ELSE NULL END WHERE allocation_id=:aid AND generation=:gen",
                state="ACKED" if is_stored else "INTENDED",
                stored=is_stored,
                aid=proof.allocation_id,
                gen=proof.generation,
            )
            if is_stored:
                execute(
                    db,
                    "UPDATE write_attempts SET state='CLOSED',finished_at=clock_timestamp() "
                    "WHERE allocation_id=:aid AND generation<:gen",
                    aid=proof.allocation_id,
                    gen=proof.generation,
                )

    def _verify_part(self, db, session, part, proofs):
        self._lock_replica_nodes(db, session["upload_id"])
        allocations = self._allocations(db, part)
        by_id = self._check_acks(db, part, allocations, proofs)
        self._record_acks(db, part, allocations, by_id)
        if not self._enough_replicas(db, session, part["chunk_id"]):
            raise DomainError("STORAGE_UNAVAILABLE")
        execute(
            db,
            "UPDATE upload_parts SET state='VERIFIED',claim_held=false,"
            "confirmed_at=clock_timestamp() WHERE chunk_id=:chunk",
            chunk=part["chunk_id"],
        )
        if part["claim_held"]:
            execute(
                db,
                "UPDATE upload_sessions SET active_claims=active_claims-1 WHERE upload_id=:upload",
                upload=session["upload_id"],
            )
        return dict(
            execute(db, "SELECT * FROM upload_parts WHERE chunk_id=:chunk", chunk=part["chunk_id"])
            .mappings()
            .one()
        )

    def confirm_part(self, upload_id, index, token, revision, proofs):
        positive(revision)
        with self.transaction() as db:
            session = self._lock_session(db, upload_id)
            self._open(session)
            part = self._lock_part(db, upload_id, index)
            if (part["claim_token"], part["revision"]) != (token, revision):
                raise DomainError("STALE_CLAIM")
            if part["state"] == "VERIFIED":
                self._check_acks(db, part, self._allocations(db, part), proofs)
                return part  # Same committed confirmation, even after the claim TTL.
            self._live_claim(db, session, part, token, revision)
            result = self._verify_part(db, session, part, proofs)
            self._live_claim(db, session, part, token, revision)
            return result

    def reconcile_part(self, upload_id, index, revision, proofs, lease: JobLease):
        with self.transaction() as db:
            session = self._lock_session(db, upload_id)
            self._open(session)
            part = self._lock_part(db, upload_id, index)
            if part["state"] != "RECONCILING" or part["revision"] != revision:
                raise DomainError("STALE_CLAIM")
            result = self._verify_part(db, session, part, proofs)
            execute(
                db,
                "UPDATE upload_parts SET revision=revision+1 WHERE chunk_id=:chunk",
                chunk=part["chunk_id"],
            )
            self._open(self._lock_session(db, upload_id))
            self._finish_domain_job(db, lease, "RECONCILE", part["chunk_id"])
            return dict(result, revision=revision + 1)

    def _destinations(self, db, part):
        return [
            dict(
                row,
                namespace_id=self.namespace_id,
                chunk_id=part["chunk_id"],
                size_bytes=part["size_bytes"],
                checksum_sha256=part["checksum_sha256"],
            )
            for row in execute(
                db,
                """SELECT a.allocation_id,a.node_id,a.incarnation,
                    a.recovery_epoch,a.generation,n.host,n.port FROM allocations a
                    JOIN storage_nodes n USING(node_id) WHERE a.chunk_id=:chunk
                    AND a.state!='RELEASED' ORDER BY a.allocation_id""",
                chunk=part["chunk_id"],
            ).mappings()
        ]

    def authorize_part_retry(self, upload_id, index, revision, owner, lease: JobLease):
        """Commit generation/attempt intents BEFORE a Worker sends fencing Prepare.

        No writer slot is granted here. retry_part requires the resulting drain
        ACKs; a crash/lost response can reauthorize at a higher generation.
        """
        positive(revision)
        if not isinstance(owner, UUID):
            raise ValueError("Owner must be UUID")
        with self.transaction() as db:
            session = self._lock_session(db, upload_id)
            self._open(session)
            part = self._lock_part(db, upload_id, index)
            if part["state"] != "RECONCILING" or part["revision"] != revision or part["claim_held"]:
                raise DomainError("STALE_CLAIM")
            self._lock_replica_nodes(db, upload_id)
            allocations = self._allocations(db, part)
            if not allocations or any(
                a["state"] in ("DELETE_PENDING", "RELEASED") for a in allocations
            ):
                raise DomainError("OUTCOME_PENDING")
            token = uuid4()
            for allocation in allocations:
                generation = positive(allocation["generation"] + 1)
                execute(
                    db,
                    "UPDATE allocations SET generation=:gen,state='UNKNOWN',"
                    "replica_health='UNKNOWN' WHERE allocation_id=:aid",
                    gen=generation,
                    aid=allocation["allocation_id"],
                )
                execute(
                    db,
                    "UPDATE write_attempts SET state='UNKNOWN' WHERE allocation_id=:aid "
                    "AND state IN ('INTENDED','IN_FLIGHT')",
                    aid=allocation["allocation_id"],
                )
                execute(
                    db,
                    """INSERT INTO write_attempts (attempt_id,allocation_id,generation,
                    owner_token,state,lease_expires_at) VALUES (:id,:aid,:gen,:token,'INTENDED',
                    clock_timestamp()+interval '60 seconds')""",
                    id=uuid4(),
                    aid=allocation["allocation_id"],
                    gen=generation,
                    token=token,
                )
            result = dict(
                execute(
                    db,
                    """UPDATE upload_parts SET revision=revision+1,
                claim_token=:token,claim_owner=:owner,claim_expires_at=LEAST(:expiry,
                clock_timestamp()+interval '60 seconds') WHERE chunk_id=:chunk RETURNING *""",
                    token=token,
                    owner=owner,
                    expiry=session["expires_at"],
                    chunk=part["chunk_id"],
                )
                .mappings()
                .one()
            )
            self._open(self._lock_session(db, upload_id))
            self._validate_job(db, lease, "RECONCILE", part["chunk_id"])
            return dict(result, destinations=self._destinations(db, result))

    def retry_part(self, upload_id, index, token, revision, proofs, lease: JobLease):
        """Grant a new claim only after every old writer has been fenced/drained."""
        with self.transaction() as db:
            session = self._lock_session(db, upload_id)
            self._open(session)
            part = self._lock_part(db, upload_id, index)
            self._live_claim(db, session, part, token, revision, state="RECONCILING")
            if session["active_claims"] >= session["max_parallel_parts"]:
                raise DomainError("RESOURCE_LIMIT")
            self._lock_replica_nodes(db, upload_id)
            allocations = self._allocations(db, part)
            by_id = self._check_acks(db, part, allocations, proofs, prepared=True)
            self._record_acks(db, part, allocations, by_id)
            for allocation in allocations:
                execute(
                    db,
                    "UPDATE write_attempts SET state='CLOSED',finished_at=clock_timestamp() "
                    "WHERE allocation_id=:aid AND generation<:gen",
                    aid=allocation["allocation_id"],
                    gen=allocation["generation"],
                )
            # Recheck after all row-lock waits and do not revive an expired intent.
            self._live_claim(db, session, part, token, revision, state="RECONCILING")
            result = dict(
                execute(
                    db,
                    """UPDATE upload_parts SET state='CLAIMED',claim_held=true,
                claim_expires_at=LEAST(:expiry,clock_timestamp()+interval '60 seconds')
                WHERE chunk_id=:chunk RETURNING *""",
                    expiry=session["expires_at"],
                    chunk=part["chunk_id"],
                )
                .mappings()
                .one()
            )
            execute(
                db,
                "UPDATE upload_sessions SET active_claims=active_claims+1 WHERE upload_id=:upload",
                upload=upload_id,
            )
            self._finish_domain_job(db, lease, "RECONCILE", part["chunk_id"])
            return dict(result, destinations=self._destinations(db, result))

    def _ready_parts(self, db, session, file):
        self._lock_replica_nodes(db, session["upload_id"])
        if execute(
            db,
            """SELECT EXISTS (SELECT 1 FROM write_attempts w
            JOIN allocations a USING(allocation_id) JOIN upload_parts p USING(chunk_id)
            WHERE p.upload_id=:upload AND w.state IN ('INTENDED','IN_FLIGHT','UNKNOWN'))
            OR EXISTS (SELECT 1 FROM allocations a JOIN upload_parts p USING(chunk_id)
            WHERE p.upload_id=:upload AND a.state IN ('RESERVED','WRITING','UNKNOWN'))""",
            upload=session["upload_id"],
        ).scalar_one():
            raise DomainError("OUTCOME_PENDING")
        summary = (
            execute(
                db,
                """WITH eligible AS (
            SELECT a.chunk_id,count(DISTINCT a.node_id) AS nodes,
            count(DISTINCT a.failure_domain_snapshot) AS domains
            FROM allocations a JOIN upload_parts p USING(chunk_id)
            JOIN storage_nodes n ON n.node_id=a.node_id AND n.active_incarnation=a.incarnation
            JOIN node_incarnations i ON i.node_id=a.node_id AND i.incarnation=a.incarnation
            WHERE p.upload_id=:upload AND a.recovery_epoch=:epoch AND i.recovery_epoch=:epoch
            AND n.enabled AND n.health_state='ACTIVE' AND i.state='ACTIVE'
            AND a.state='STORED' AND a.replica_health='VERIFIED'
            AND a.ack_size=p.size_bytes AND a.ack_hash=p.checksum_sha256 GROUP BY a.chunk_id)
            SELECT count(*) AS count,COALESCE(sum(p.size_bytes),0) AS bytes,
            COALESCE(bool_and(p.state='VERIFIED' AND NOT p.claim_held AND p.part_index<:count
            AND p.size_bytes=LEAST(:chunk_size,:size-p.part_index*:chunk_size)
            AND COALESCE(e.nodes,0)>=:rf AND COALESCE(e.domains,0)>=:domains),true) AS valid
            FROM upload_parts p LEFT JOIN eligible e USING(chunk_id) WHERE p.upload_id=:upload""",
                upload=session["upload_id"],
                epoch=self.epoch,
                count=session["part_count"],
                chunk_size=session["chunk_size_bytes"],
                size=file["size_bytes"],
                rf=session["replication_factor"],
                domains=session["min_distinct_domains"],
            )
            .mappings()
            .one()
        )
        if (summary["count"], summary["bytes"], summary["valid"], session["active_claims"]) != (
            session["part_count"],
            file["size_bytes"],
            True,
            0,
        ):
            raise DomainError("PARTS_INCOMPLETE")

    @staticmethod
    def _manifest_matches(db, session, file):
        summary = (
            execute(
                db,
                """SELECT count(*) AS count,COALESCE(sum(m.size_bytes),0) AS bytes,
            COALESCE(bool_and(p.upload_id=:upload AND p.part_index=m.part_index
            AND p.size_bytes=m.size_bytes AND p.checksum_sha256=m.checksum_sha256),true) AS valid
            FROM manifest_parts m JOIN upload_parts p USING(chunk_id)
            WHERE m.version_id=:version""",
                upload=session["upload_id"],
                version=file["version_id"],
            )
            .mappings()
            .one()
        )
        if (summary["count"], summary["bytes"], summary["valid"]) != (
            session["part_count"],
            file["size_bytes"],
            True,
        ):
            raise DomainError("PARTS_INCOMPLETE")

    def complete_upload(self, upload_id):
        with self.transaction() as db:
            session = self._lock_session(db, upload_id)
            if session["state"] in ("FINALIZING", "COMPLETED"):
                return self.session(db, upload_id)
            self._open(session)
            file = (
                execute(db, "SELECT * FROM files WHERE file_id=:file", file=session["file_id"])
                .mappings()
                .one()
            )
            if file["state"] != "UPLOADING":
                raise DomainError("UPLOAD_STATE_CONFLICT")
            self._ready_parts(db, session, file)
            self._open(self._lock_session(db, upload_id))
            # Server-side INSERT SELECT bounds application memory even for large manifests.
            execute(
                db,
                """INSERT INTO manifest_parts (version_id,part_index,chunk_id,
                size_bytes,checksum_sha256) SELECT :version,part_index,chunk_id,size_bytes,
                checksum_sha256 FROM upload_parts WHERE upload_id=:upload ORDER BY part_index""",
                version=file["version_id"],
                upload=upload_id,
            )
            self._manifest_matches(db, session, file)
            execute(
                db,
                "UPDATE upload_sessions SET state='FINALIZING',revision=revision+1 "
                "WHERE upload_id=:upload",
                upload=upload_id,
            )
            execute(
                db,
                "UPDATE files SET state='VERIFYING',revision=revision+1 WHERE file_id=:file",
                file=session["file_id"],
            )
            self.enqueue(
                db,
                "FINALIZE",
                session["file_id"],
                dict(
                    upload_id=str(upload_id),
                    revision=session["revision"] + 1,
                    version_id=str(file["version_id"]),
                ),
            )
            return self.session(db, upload_id)

    def publish_finalize(self, upload_id, revision, actual_size, actual_sha256, lease: JobLease):
        """Publish a Worker's full-file verification, performed outside this transaction."""
        positive(revision)
        positive(actual_size, zero=True)
        checksum(actual_sha256)
        with self.transaction() as db:
            execute(
                db,
                "SELECT resource_key FROM admission_counters WHERE "
                "resource_key='upload_sessions' FOR UPDATE",
            ).one()
            session = self._lock_session(db, upload_id)
            if session["state"] != "FINALIZING" or session["revision"] != revision:
                raise DomainError("UPLOAD_STATE_CONFLICT")
            file = (
                execute(db, "SELECT * FROM files WHERE file_id=:file", file=session["file_id"])
                .mappings()
                .one()
            )
            if file["state"] != "VERIFYING":
                raise DomainError("UPLOAD_STATE_CONFLICT")
            self._ready_parts(db, session, file)
            self._manifest_matches(db, session, file)
            valid = (actual_size, actual_sha256) == (file["size_bytes"], file["expected_sha256"])
            execute(
                db,
                """UPDATE files SET state=:state,actual_sha256=:hash,revision=revision+1,
                available_at=CASE WHEN :valid THEN clock_timestamp() ELSE NULL END
                WHERE file_id=:file""",
                state="AVAILABLE" if valid else "FAILED",
                hash=actual_sha256,
                valid=valid,
                file=session["file_id"],
            )
            execute(
                db,
                """UPDATE upload_sessions SET state=:state,revision=revision+1,
                completed_at=CASE WHEN :valid THEN clock_timestamp() ELSE NULL END,error_code=:error
                WHERE upload_id=:upload""",
                state="COMPLETED" if valid else "FAILED",
                valid=valid,
                error=None if valid else "CHECKSUM_MISMATCH",
                upload=upload_id,
            )
            if session["admission_released_at"] is None:
                execute(
                    db,
                    "UPDATE upload_sessions SET admission_released_at=clock_timestamp() "
                    "WHERE upload_id=:upload",
                    upload=upload_id,
                )
                execute(
                    db,
                    "UPDATE admission_counters SET held_count=held_count-1,revision=revision+1 "
                    "WHERE resource_key='upload_sessions'",
                )
            if not valid:
                self.enqueue(db, "CLEANUP", session["file_id"])
            self._finish_domain_job(
                db,
                lease,
                "FINALIZE",
                session["file_id"],
                payload=dict(
                    upload_id=str(upload_id), revision=revision, version_id=str(file["version_id"])
                ),
            )
            return self.session(db, upload_id)

    @staticmethod
    def enqueue(db, kind, target, payload=None):
        return execute(
            db,
            """INSERT INTO jobs (job_id,kind,dedup_key,target_id,payload,
            state,run_after,attempts,fencing_token) VALUES (:id,:kind,:key,:target,
            CAST(:payload AS jsonb),'READY',clock_timestamp(),0,0)
            ON CONFLICT (kind,dedup_key) WHERE state IN ('READY','RUNNING','RETRY')
            DO UPDATE SET dedup_key=excluded.dedup_key RETURNING job_id""",
            id=uuid4(),
            kind=kind,
            key=str(target),
            target=target,
            payload=json.dumps(payload or {}),
        ).scalar_one()

    def uncertain(self, upload_id, index, token, revision):
        with self.transaction() as db:
            self._lock_session(db, upload_id)
            part = (
                execute(
                    db,
                    "SELECT * FROM upload_parts WHERE upload_id=:upload AND "
                    "part_index=:index FOR UPDATE",
                    upload=upload_id,
                    index=index,
                )
                .mappings()
                .one()
            )
            if part["claim_token"] != token or part["revision"] != revision:
                raise DomainError("STALE_CLAIM")
            self._uncertain(db, part)

    def _uncertain(self, db, part):
        if part["state"] == "VERIFIED":
            return
        # The same transition releases the slot once, and persists the intent.
        execute(
            db,
            "UPDATE upload_parts SET state='RECONCILING',claim_held=false WHERE chunk_id=:chunk",
            chunk=part["chunk_id"],
        )
        if part["claim_held"]:
            execute(
                db,
                "UPDATE upload_sessions SET active_claims=active_claims-1 WHERE upload_id=:upload",
                upload=part["upload_id"],
            )
        execute(
            db,
            "UPDATE allocations SET state='UNKNOWN' WHERE chunk_id=:chunk AND "
            "state IN ('RESERVED','WRITING')",
            chunk=part["chunk_id"],
        )
        execute(
            db,
            "UPDATE write_attempts SET state='UNKNOWN' WHERE allocation_id IN "
            "(SELECT allocation_id FROM allocations WHERE chunk_id=:chunk) "
            "AND state IN ('INTENDED','IN_FLIGHT')",
            chunk=part["chunk_id"],
        )
        self.enqueue(db, "RECONCILE", part["chunk_id"])

    def sweep_claims(self, limit=8):
        positive(limit, 8)
        with self.transaction() as db:
            # Candidate scan takes no part locks before file/session locks.
            candidates = (
                execute(
                    db,
                    """SELECT upload_id,part_index FROM upload_parts
                WHERE claim_held AND claim_expires_at<=clock_timestamp()
                ORDER BY claim_expires_at,chunk_id LIMIT :limit""",
                    limit=limit,
                )
                .mappings()
                .all()
            )
        changed = 0
        for candidate in candidates:
            # Do not retain one session's locks while acquiring another's.
            # Claim and session expiry use different fairness/candidate orders.
            with self.transaction() as db:
                self._lock_session(db, candidate["upload_id"])
                part = (
                    execute(
                        db,
                        """SELECT *,claim_expires_at<=clock_timestamp() AS expired
                    FROM upload_parts WHERE upload_id=:upload AND part_index=:index FOR UPDATE""",
                        upload=candidate["upload_id"],
                        index=candidate["part_index"],
                    )
                    .mappings()
                    .one()
                )
                if part["claim_held"] and part["expired"]:
                    self._uncertain(db, part)
                    changed += 1
        return changed

    def abort(self, upload_id):
        with self.transaction() as db:
            execute(
                db,
                "SELECT resource_key FROM admission_counters WHERE "
                "resource_key='upload_sessions' FOR UPDATE",
            ).one()
            session = self._lock_session(db, upload_id)
            if session["state"] == "COMPLETED":
                raise DomainError("UPLOAD_STATE_CONFLICT")
            if session["state"] in ("ABORTED", "EXPIRED", "FAILED"):
                return session["state"]
            execute(
                db,
                "UPDATE files SET state='FAILED',revision=revision+1 WHERE file_id=:file",
                file=session["file_id"],
            )
            execute(
                db,
                "UPDATE upload_sessions SET state='ABORTED',revision=revision+1 "
                "WHERE upload_id=:upload",
                upload=upload_id,
            )
            if session["admission_released_at"] is None:
                execute(
                    db,
                    "UPDATE upload_sessions SET "
                    "admission_released_at=clock_timestamp() WHERE upload_id=:upload",
                    upload=upload_id,
                )
                execute(
                    db,
                    "UPDATE admission_counters SET "
                    "held_count=held_count-1,revision=revision+1 WHERE "
                    "resource_key='upload_sessions'",
                )
            self.enqueue(db, "CLEANUP", session["file_id"])
            return "ABORTED"

    def expire_sessions(self, limit=8):
        positive(limit, 8)
        with self.transaction() as db:
            candidates = (
                execute(
                    db,
                    """SELECT upload_id FROM upload_sessions
                WHERE state='OPEN' AND expires_at<=clock_timestamp()
                ORDER BY expires_at,upload_id LIMIT :limit""",
                    limit=limit,
                )
                .scalars()
                .all()
            )
        changed = 0
        for upload in candidates:
            with self.transaction() as db:
                execute(
                    db,
                    "SELECT resource_key FROM admission_counters "
                    "WHERE resource_key='upload_sessions' FOR UPDATE",
                ).one()
                session = self._lock_session(db, upload)
                if session["state"] != "OPEN" or session["live"]:
                    continue
                execute(
                    db,
                    "UPDATE files SET state='FAILED',revision=revision+1 WHERE file_id=:file",
                    file=session["file_id"],
                )
                execute(
                    db,
                    "UPDATE upload_sessions SET state='EXPIRED',revision=revision+1 "
                    "WHERE upload_id=:upload",
                    upload=upload,
                )
                if session["admission_released_at"] is None:
                    execute(
                        db,
                        "UPDATE upload_sessions SET admission_released_at=clock_timestamp() "
                        "WHERE upload_id=:upload",
                        upload=upload,
                    )
                    execute(
                        db,
                        "UPDATE admission_counters SET held_count=held_count-1,"
                        "revision=revision+1 WHERE resource_key='upload_sessions'",
                    )
                self.enqueue(db, "CLEANUP", session["file_id"])
                changed += 1
        return changed

    def authorize_cleanup(self, upload_id, lease: JobLease, *, after_index=-1, limit=8):
        positive(limit, 8)
        if type(after_index) is not int or not -1 <= after_index <= 2**53 - 1:
            raise ValueError("Invalid cleanup cursor")
        with self.transaction() as db:
            session = self._lock_session(db, upload_id)
            self._cleanup_session(db, session)
            if self._has_live_reads(db, session["file_id"]):
                raise DomainError("READ_LEASE_ACTIVE")
            # Terminal session fences confirmation. A late physical write still
            # requires a durable Storage Delete fence; no capacity is freed here.
            rows = (
                execute(
                    db,
                    "SELECT * FROM upload_parts p WHERE upload_id=:upload "
                    "AND part_index>:after AND EXISTS (SELECT 1 FROM allocations a "
                    "WHERE a.chunk_id=p.chunk_id AND a.state!='RELEASED') "
                    "ORDER BY part_index LIMIT :limit FOR UPDATE",
                    upload=upload_id,
                    after=after_index,
                    limit=limit + 1,
                )
                .mappings()
                .all()
            )
            destinations = []
            for part in rows[:limit]:
                if part["claim_held"]:
                    execute(
                        db,
                        "UPDATE upload_parts SET claim_held=false WHERE chunk_id=:chunk",
                        chunk=part["chunk_id"],
                    )
                    execute(
                        db,
                        "UPDATE upload_sessions SET active_claims=active_claims-1 WHERE "
                        "upload_id=:upload",
                        upload=upload_id,
                    )
                destinations.extend(
                    dict(row)
                    for row in execute(
                        db,
                        """UPDATE allocations
                    SET state='DELETE_PENDING' WHERE chunk_id=:chunk AND state!='RELEASED'
                    RETURNING *""",
                        chunk=part["chunk_id"],
                    ).mappings()
                )
            self.enqueue(db, "CLEANUP", session["file_id"])
            self._validate_cleanup_job(db, lease, session["file_id"])
            return dict(
                allocations=destinations,
                next_after_index=rows[min(limit, len(rows)) - 1]["part_index"]
                if rows
                else after_index,
                has_more=len(rows) > limit,
            )

    def release_allocation(self, evidence: DeleteEvidence, lease: JobLease):
        if (
            evidence.namespace_id,
            evidence.recovery_epoch,
            evidence.state,
            evidence.cleanup_complete,
        ) != (self.namespace_id, self.epoch, "DELETED", True):
            raise DomainError("INVALID_DELETE_ACK")
        with self.transaction() as db:
            row = (
                execute(
                    db,
                    "SELECT a.*,p.upload_id FROM allocations a JOIN upload_parts p "
                    "USING(chunk_id) WHERE allocation_id=:aid",
                    aid=evidence.allocation_id,
                )
                .mappings()
                .one()
            )
            session = self._lock_session(db, row["upload_id"])
            execute(
                db,
                "SELECT chunk_id FROM upload_parts WHERE chunk_id=:chunk FOR UPDATE",
                chunk=row["chunk_id"],
            ).one()
            execute(
                db,
                "SELECT node_id FROM node_budgets WHERE node_id=:node AND "
                "incarnation=:inc FOR UPDATE",
                node=row["node_id"],
                inc=row["incarnation"],
            ).one()
            row = (
                execute(
                    db,
                    "SELECT * FROM allocations WHERE allocation_id=:aid FOR UPDATE",
                    aid=evidence.allocation_id,
                )
                .mappings()
                .one()
            )
            if (row["node_id"], row["incarnation"], row["recovery_epoch"], row["generation"]) != (
                evidence.node_id,
                evidence.incarnation,
                evidence.recovery_epoch,
                evidence.generation,
            ):
                raise DomainError("INVALID_DELETE_ACK")
            if row["state"] == "RELEASED":
                self._validate_cleanup_job(db, lease, session["file_id"])
                return False
            if row["state"] != "DELETE_PENDING":
                raise DomainError("CLEANUP_NOT_AUTHORIZED")
            self._cleanup_session(db, session)
            execute(
                db,
                "UPDATE allocations SET "
                "state='RELEASED',cleanup_complete_at=clock_timestamp() WHERE "
                "allocation_id=:aid",
                aid=evidence.allocation_id,
            )
            execute(
                db,
                "UPDATE node_budgets SET "
                "charged_bytes=charged_bytes-:size,revision=revision+1 WHERE "
                "node_id=:node AND incarnation=:inc",
                size=row["charged_bytes"],
                node=row["node_id"],
                inc=row["incarnation"],
            )
            execute(
                db,
                "UPDATE write_attempts SET "
                "state='CLOSED',finished_at=clock_timestamp() WHERE "
                "allocation_id=:aid",
                aid=evidence.allocation_id,
            )
            self._validate_cleanup_job(db, lease, session["file_id"])
            return True

    @staticmethod
    def _validate_cleanup_job(db, lease, file_id):
        return Repository._validate_job(db, lease, "CLEANUP", file_id)

    @staticmethod
    def _validate_job(db, lease, kind, target):
        job = (
            execute(
                db,
                "SELECT * FROM jobs WHERE job_id=:id FOR UPDATE",
                id=lease.job_id,
            )
            .mappings()
            .one_or_none()
        )
        if job is None:
            raise DomainError("STALE_JOB")
        # LockRows can wait after evaluating a SELECT's target expressions.
        # Read the clock only AFTER owning the row; expiry before a lock wait
        # must not authorize a stale worker's transaction effects.
        live = execute(
            db,
            "SELECT lease_expires_at>clock_timestamp() FROM jobs WHERE job_id=:id",
            id=lease.job_id,
        ).scalar_one()
        if (
            job["kind"],
            job["target_id"],
            job["state"],
            job["lease_owner"],
            job["fencing_token"],
            live,
        ) != (kind, target, "RUNNING", lease.owner, lease.token, True):
            raise DomainError("STALE_JOB")
        return job

    @staticmethod
    def _finish_domain_job(db, lease, kind, target, *, payload=None):
        job = Repository._validate_job(db, lease, kind, target)
        if payload is not None and job["payload"] != payload:
            raise DomainError("STALE_JOB")
        if not execute(
            db,
            "UPDATE jobs SET state='DONE',lease_owner=NULL,lease_expires_at=NULL "
            "WHERE job_id=:job AND lease_expires_at>clock_timestamp()",
            job=lease.job_id,
        ).rowcount:
            raise DomainError("STALE_JOB")

    def claim_jobs(self, owner: UUID, limit=8, *, kinds=None, target=None, exclude=None):
        positive(limit, 8)
        exclude = list(exclude or ())
        if len(exclude) > 24 or any(not isinstance(job_id, UUID) for job_id in exclude):
            raise ValueError("At most 24 resident job UUIDs may be excluded")
        kinds = list(kinds or ("FINALIZE", "RECONCILE", "CLEANUP", "REPAIR", "SCRUB", "HEALTH"))
        with self.transaction() as db:
            return [
                dict(row)
                for row in execute(
                    db,
                    """WITH candidates AS (
                SELECT job_id FROM jobs WHERE
                kind=ANY(:kinds) AND job_id!=ALL(CAST(:exclude AS uuid[]))
                AND (CAST(:target AS uuid) IS NULL OR target_id=:target) AND (
                (state IN ('READY','RETRY') AND run_after<=clock_timestamp()) OR
                (state='RUNNING' AND lease_expires_at<=clock_timestamp()))
                ORDER BY run_after,job_id LIMIT :limit FOR UPDATE SKIP LOCKED)
                UPDATE jobs j SET state='RUNNING',lease_owner=:owner,
                lease_expires_at=clock_timestamp()+interval '60 seconds',
                fencing_token=j.fencing_token+1,attempts=j.attempts+1
                FROM candidates c WHERE j.job_id=c.job_id RETURNING j.*""",
                    owner=owner,
                    limit=limit,
                    kinds=kinds,
                    target=target,
                    exclude=exclude,
                ).mappings()
            ]

    def worker_snapshot(self, job, lease):
        """Bounded identity snapshot; the returned data outlives this transaction."""
        with self.transaction() as db:
            if job["kind"] == "RECONCILE":
                part = dict(
                    execute(
                        db, "SELECT * FROM upload_parts WHERE chunk_id=:id", id=job["target_id"]
                    )
                    .mappings()
                    .one()
                )
                session = self.session(db, part["upload_id"])
                result = dict(session=session, part=part, destinations=self._destinations(db, part))
            else:
                session = dict(
                    execute(
                        db, "SELECT * FROM upload_sessions WHERE file_id=:id", id=job["target_id"]
                    )
                    .mappings()
                    .one()
                )
                result = dict(session=session)
            self._validate_job(db, lease, job["kind"], job["target_id"])
            return result

    def finalize_page(self, upload_id, lease, *, after_index=-1, limit=8):
        positive(limit, 8)
        with self.transaction() as db:
            session = self.session(db, upload_id)
            if session["state"] != "FINALIZING":
                raise DomainError("UPLOAD_STATE_CONFLICT")
            parts = [
                dict(row)
                for row in execute(
                    db,
                    """SELECT m.*
                FROM manifest_parts m WHERE m.version_id=:version AND m.part_index>:after
                ORDER BY m.part_index LIMIT :limit""",
                    version=session["version_id"],
                    after=after_index,
                    limit=limit,
                ).mappings()
            ]
            for part in parts:
                # Only current eligible replicas can supply finalize bytes. Publication
                # rechecks policy and the entire immutable manifest under domain locks.
                part["destinations"] = [
                    dict(
                        row,
                        namespace_id=self.namespace_id,
                        chunk_id=part["chunk_id"],
                        size_bytes=part["size_bytes"],
                        checksum_sha256=part["checksum_sha256"],
                    )
                    for row in execute(
                        db,
                        """
                    SELECT a.*,n.host,n.port FROM allocations a JOIN storage_nodes n
                    ON n.node_id=a.node_id AND n.active_incarnation=a.incarnation
                    JOIN node_incarnations i ON i.node_id=a.node_id AND i.incarnation=a.incarnation
                    WHERE a.chunk_id=:chunk AND a.state='STORED' AND a.replica_health='VERIFIED'
                    AND a.recovery_epoch=:epoch AND i.recovery_epoch=:epoch
                    AND n.enabled AND n.health_state='ACTIVE' AND i.state='ACTIVE'
                    ORDER BY a.allocation_id""",
                        chunk=part["chunk_id"],
                        epoch=self.epoch,
                    ).mappings()
                ]
            self._validate_job(db, lease, "FINALIZE", session["file_id"])
            return parts

    def cleanup_endpoint(self, allocation):
        with self.transaction() as db:
            row = (
                execute(
                    db,
                    "SELECT host,port FROM storage_nodes WHERE node_id=:node",
                    node=allocation["node_id"],
                )
                .mappings()
                .one()
            )
            return dict(allocation, **row, namespace_id=self.namespace_id)

    def finish_cleanup(self, upload_id, lease):
        with self.transaction() as db:
            session = self._lock_session(db, upload_id)
            self._cleanup_session(db, session)
            # Empty manifests also need to wait for readers before DELETED/DONE.
            if self._has_live_reads(db, session["file_id"]):
                raise DomainError("READ_LEASE_ACTIVE")
            if execute(
                db,
                """SELECT 1 FROM allocations a JOIN upload_parts p USING(chunk_id)
                WHERE p.upload_id=:upload AND a.state!='RELEASED' LIMIT 1""",
                upload=upload_id,
            ).first():
                raise DomainError("OUTCOME_PENDING")
            if execute(
                db,
                """SELECT 1 FROM write_attempts w JOIN allocations a USING(allocation_id)
                JOIN upload_parts p USING(chunk_id) WHERE p.upload_id=:upload
                AND w.state!='CLOSED' LIMIT 1""",
                upload=upload_id,
            ).first():
                raise DomainError("OUTCOME_PENDING")
            execute(
                db,
                "UPDATE files SET state='DELETED',revision=revision+1 "
                "WHERE file_id=:file AND state='DELETING'",
                file=session["file_id"],
            )
            self._finish_domain_job(db, lease, "CLEANUP", session["file_id"])

    def fail_worker_job(self, job, lease, *, error, max_attempts=8):
        """Bounded retry; exhausted upload work fails atomically with durable cleanup.

        DEAD cleanup retains all obligations and capacity. No exception text or
        remote body is persisted; error is a fixed code chosen by the Worker.
        """
        positive(max_attempts, 100)
        with self.transaction() as db:
            execute(
                db,
                "SELECT resource_key FROM admission_counters WHERE "
                "resource_key='upload_sessions' FOR UPDATE",
            ).one()
            if job["kind"] == "RECONCILE":
                upload_id = execute(
                    db, "SELECT upload_id FROM upload_parts WHERE chunk_id=:id", id=job["target_id"]
                ).scalar_one()
            else:
                upload_id = execute(
                    db,
                    "SELECT upload_id FROM upload_sessions WHERE file_id=:id",
                    id=job["target_id"],
                ).scalar_one()
            session = self._lock_session(db, upload_id)
            current = self._validate_job(db, lease, job["kind"], job["target_id"])
            # Readers can validly renew for 24h. Waiting for them is not a failed
            # delete attempt and must not exhaust cleanup's retry budget.
            waiting_for_read = job["kind"] == "CLEANUP" and error == "READ_LEASE_ACTIVE"
            exhausted = not waiting_for_read and current["attempts"] >= max_attempts
            if (
                exhausted
                and job["kind"] != "CLEANUP"
                and session["state"] in ("OPEN", "FINALIZING")
            ):
                execute(
                    db,
                    "UPDATE files SET state='FAILED',revision=revision+1 WHERE file_id=:id",
                    id=session["file_id"],
                )
                execute(
                    db,
                    "UPDATE upload_sessions SET state='FAILED',revision=revision+1,"
                    "error_code=:error WHERE upload_id=:id",
                    id=upload_id,
                    error=error,
                )
                if session["admission_released_at"] is None:
                    execute(
                        db,
                        "UPDATE upload_sessions SET admission_released_at=clock_timestamp() "
                        "WHERE upload_id=:id",
                        id=upload_id,
                    )
                    execute(
                        db,
                        "UPDATE admission_counters SET held_count=held_count-1,"
                        "revision=revision+1 WHERE resource_key='upload_sessions'",
                    )
                self.enqueue(db, "CLEANUP", session["file_id"])
            self._validate_job(db, lease, job["kind"], job["target_id"])
            changed = execute(
                db,
                """UPDATE jobs SET state=:state,last_error=:error,lease_owner=NULL,
                lease_expires_at=NULL,run_after=clock_timestamp()+:delay*interval '1 second',
                attempts=attempts-:waiting
                WHERE job_id=:id AND lease_expires_at>clock_timestamp()""",
                id=lease.job_id,
                state="DEAD" if exhausted else "RETRY",
                error=error,
                delay=10
                if waiting_for_read
                else min(300, 5 * 2 ** min(current["attempts"] - 1, 6)),
                waiting=int(waiting_for_read),
            ).rowcount
            if not changed:
                raise DomainError("STALE_JOB")

    def finish_job(self, job_id, owner, token, *, retry=False):
        with self.transaction() as db:
            count = execute(
                db,
                """UPDATE jobs SET state=:state,lease_owner=NULL,
                lease_expires_at=NULL,run_after=clock_timestamp()+interval '5 seconds'
                WHERE job_id=:job AND state='RUNNING' AND lease_owner=:owner
                AND fencing_token=:token AND lease_expires_at>clock_timestamp()""",
                state="RETRY" if retry else "DONE",
                job=job_id,
                owner=owner,
                token=token,
            ).rowcount
            if not count:
                raise DomainError("STALE_JOB")

    def renew_job(self, job_id, owner, token):
        with self.transaction() as db:
            job = (
                execute(db, "SELECT * FROM jobs WHERE job_id=:id FOR UPDATE", id=job_id)
                .mappings()
                .one()
            )
            self._validate_job(db, JobLease(job_id, owner, token), job["kind"], job["target_id"])
            count = execute(
                db,
                """UPDATE jobs SET lease_expires_at=clock_timestamp()+interval '60 seconds'
                WHERE job_id=:job AND state='RUNNING' AND lease_owner=:owner
                AND fencing_token=:token AND lease_expires_at>clock_timestamp()""",
                job=job_id,
                owner=owner,
                token=token,
            ).rowcount
            if not count:
                raise DomainError("STALE_JOB")
