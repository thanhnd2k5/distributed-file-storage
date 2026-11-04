CREATE TABLE cluster_control (
    singleton integer PRIMARY KEY CHECK (singleton=1), namespace_id uuid NOT NULL UNIQUE,
    recovery_epoch uuid NOT NULL, mode text NOT NULL CHECK (mode IN ('ACTIVE','RECOVERING')),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE files (
    file_id uuid PRIMARY KEY, version_id uuid NOT NULL UNIQUE,
    original_name varchar(255) NOT NULL, content_type varchar(255) NOT NULL,
    size_bytes bigint NOT NULL CHECK (size_bytes>=0),
    expected_sha256 varchar(64) NOT NULL CHECK (expected_sha256 ~ '^[0-9a-f]{64}$'),
    actual_sha256 varchar(64) CHECK (actual_sha256 ~ '^[0-9a-f]{64}$'),
    state text NOT NULL CHECK (state IN ('UPLOADING','VERIFYING','AVAILABLE','FAILED','DELETING','DELETED')),
    revision bigint NOT NULL CHECK (revision>0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    available_at timestamptz, deleted_at timestamptz,
    UNIQUE (file_id,version_id),
    CHECK (state!='AVAILABLE' OR (actual_sha256 IS NOT NULL AND actual_sha256=expected_sha256
        AND available_at IS NOT NULL))
);
CREATE INDEX files_catalog ON files (created_at DESC,file_id DESC) WHERE state='AVAILABLE';
CREATE TABLE upload_sessions (
    upload_id uuid PRIMARY KEY, file_id uuid NOT NULL UNIQUE REFERENCES files(file_id) ON DELETE RESTRICT,
    state text NOT NULL CHECK (state IN ('OPEN','FINALIZING','COMPLETED','ABORTED','EXPIRED','FAILED')),
    revision bigint NOT NULL CHECK (revision>0),
    chunk_size_bytes bigint NOT NULL CHECK (chunk_size_bytes>0),
    part_count bigint NOT NULL CHECK (part_count>=0),
    replication_factor integer NOT NULL CHECK (replication_factor>=1),
    min_distinct_domains integer NOT NULL CHECK (min_distinct_domains BETWEEN 1 AND replication_factor),
    max_parallel_parts integer NOT NULL CHECK (max_parallel_parts>0), policy jsonb NOT NULL,
    active_claims integer NOT NULL CHECK (active_claims BETWEEN 0 AND max_parallel_parts),
    expires_at timestamptz NOT NULL, completed_at timestamptz,
    admission_released_at timestamptz, error_code text
);
CREATE INDEX sessions_expiry ON upload_sessions (expires_at,upload_id) WHERE state='OPEN';
CREATE TABLE upload_parts (
    chunk_id uuid PRIMARY KEY, upload_id uuid NOT NULL REFERENCES upload_sessions(upload_id) ON DELETE RESTRICT,
    part_index bigint NOT NULL CHECK (part_index>=0), size_bytes bigint NOT NULL CHECK (size_bytes>0),
    checksum_sha256 varchar(64) NOT NULL CHECK (checksum_sha256 ~ '^[0-9a-f]{64}$'),
    state text NOT NULL CHECK (state IN ('CLAIMED','RECONCILING','VERIFIED')),
    revision bigint NOT NULL CHECK (revision>0), claim_held boolean NOT NULL,
    claim_token uuid, claim_owner uuid, claim_expires_at timestamptz, confirmed_at timestamptz,
    UNIQUE (upload_id,part_index), CHECK (state!='VERIFIED' OR confirmed_at IS NOT NULL),
    CHECK (NOT claim_held OR (claim_token IS NOT NULL AND claim_owner IS NOT NULL
        AND claim_expires_at IS NOT NULL))
);
CREATE INDEX parts_expiry ON upload_parts (claim_expires_at) WHERE state IN ('CLAIMED','RECONCILING');
CREATE TABLE manifest_parts (
    version_id uuid NOT NULL REFERENCES files(version_id) ON DELETE RESTRICT, part_index bigint NOT NULL CHECK (part_index>=0),
    chunk_id uuid NOT NULL UNIQUE REFERENCES upload_parts(chunk_id) ON DELETE RESTRICT,
    size_bytes bigint NOT NULL CHECK (size_bytes>0),
    checksum_sha256 varchar(64) NOT NULL CHECK (checksum_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (version_id,part_index)
);
CREATE TABLE storage_nodes (
    node_id varchar(64) PRIMARY KEY, host varchar(255) NOT NULL,
    port integer NOT NULL CHECK (port BETWEEN 1 AND 65535), failure_domain varchar(255) NOT NULL,
    active_incarnation uuid NOT NULL, enabled boolean NOT NULL,
    health_state text NOT NULL CHECK (health_state IN ('ACTIVE','SUSPECTED','DOWN','QUARANTINED')),
    health_generation bigint NOT NULL CHECK (health_generation>=0),
    health_observed_at timestamptz, available_bytes bigint CHECK (available_bytes>=0)
);
CREATE TABLE node_incarnations (
    node_id varchar(64) NOT NULL REFERENCES storage_nodes(node_id) ON DELETE RESTRICT, incarnation uuid NOT NULL,
    recovery_epoch uuid NOT NULL,
    state text NOT NULL CHECK (state IN ('JOINING','ACTIVE','QUARANTINED','RETIRED')),
    registered_at timestamptz NOT NULL DEFAULT clock_timestamp(), retired_at timestamptz,
    PRIMARY KEY (node_id,incarnation)
);
CREATE UNIQUE INDEX one_active_incarnation ON node_incarnations (node_id) WHERE state='ACTIVE';
ALTER TABLE storage_nodes ADD CONSTRAINT active_incarnation_fk
    FOREIGN KEY (node_id,active_incarnation) REFERENCES node_incarnations(node_id,incarnation) ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED;
CREATE TABLE node_budgets (
    node_id varchar(64) NOT NULL, incarnation uuid NOT NULL,
    allocatable_bytes bigint NOT NULL CHECK (allocatable_bytes>=0),
    charged_bytes bigint NOT NULL CHECK (charged_bytes BETWEEN 0 AND allocatable_bytes),
    revision bigint NOT NULL CHECK (revision>0), PRIMARY KEY (node_id,incarnation),
    FOREIGN KEY (node_id,incarnation) REFERENCES node_incarnations(node_id,incarnation) ON DELETE RESTRICT
);
CREATE TABLE allocations (
    allocation_id uuid PRIMARY KEY, chunk_id uuid NOT NULL REFERENCES upload_parts(chunk_id) ON DELETE RESTRICT,
    node_id varchar(64) NOT NULL, incarnation uuid NOT NULL, recovery_epoch uuid NOT NULL,
    generation bigint NOT NULL CHECK (generation>0), failure_domain_snapshot varchar(255) NOT NULL,
    charged_bytes bigint NOT NULL CHECK (charged_bytes>0),
    state text NOT NULL CHECK (state IN ('RESERVED','WRITING','UNKNOWN','STORED','DELETE_PENDING','RELEASED')),
    replica_health text NOT NULL CHECK (replica_health IN ('UNKNOWN','VERIFIED','MISSING','CORRUPT')),
    observation_generation bigint NOT NULL CHECK (observation_generation>=0), observed_at timestamptz,
    ack_size bigint CHECK (ack_size>=0), ack_hash varchar(64) CHECK (ack_hash ~ '^[0-9a-f]{64}$'),
    acked_at timestamptz, cleanup_complete_at timestamptz,
    FOREIGN KEY (node_id,incarnation) REFERENCES node_incarnations(node_id,incarnation) ON DELETE RESTRICT,
    CHECK (state!='STORED' OR (ack_size IS NOT NULL AND ack_hash IS NOT NULL AND acked_at IS NOT NULL)),
    CHECK (state!='RELEASED' OR cleanup_complete_at IS NOT NULL)
);
CREATE INDEX allocations_part ON allocations (chunk_id,state);
CREATE INDEX allocations_node ON allocations (node_id,incarnation,state);
CREATE TABLE write_attempts (
    attempt_id uuid PRIMARY KEY, allocation_id uuid NOT NULL REFERENCES allocations(allocation_id) ON DELETE RESTRICT,
    generation bigint NOT NULL CHECK (generation>0), owner_token uuid NOT NULL,
    state text NOT NULL CHECK (state IN ('INTENDED','IN_FLIGHT','ACKED','UNKNOWN','CLOSED')),
    lease_expires_at timestamptz NOT NULL, started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz, error_code text, UNIQUE (allocation_id,generation)
);
CREATE INDEX attempts_expiry ON write_attempts (state,lease_expires_at);
CREATE TABLE jobs (
    job_id uuid PRIMARY KEY,
    kind text NOT NULL CHECK (kind IN ('FINALIZE','RECONCILE','CLEANUP','REPAIR','SCRUB','HEALTH')),
    dedup_key text NOT NULL, target_id uuid NOT NULL, payload jsonb NOT NULL,
    state text NOT NULL CHECK (state IN ('READY','RUNNING','RETRY','DONE','DEAD')),
    run_after timestamptz NOT NULL, attempts bigint NOT NULL CHECK (attempts>=0),
    lease_owner uuid, lease_expires_at timestamptz, fencing_token bigint NOT NULL CHECK (fencing_token>=0),
    last_error text, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (state!='RUNNING' OR (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL))
);
CREATE UNIQUE INDEX active_job_dedup ON jobs (kind,dedup_key) WHERE state IN ('READY','RUNNING','RETRY');
CREATE INDEX jobs_ready ON jobs (run_after,job_id) WHERE state IN ('READY','RETRY');
CREATE INDEX jobs_expiry ON jobs (lease_expires_at) WHERE state='RUNNING';
CREATE TABLE idempotency_records (
    operation text NOT NULL CHECK (operation='CREATE_UPLOAD'), key uuid NOT NULL,
    request_sha256 varchar(64) NOT NULL CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
    upload_id uuid NOT NULL UNIQUE REFERENCES upload_sessions(upload_id) ON DELETE RESTRICT,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY (operation,key)
);
CREATE TABLE download_leases (
    read_id uuid PRIMARY KEY, file_id uuid NOT NULL, version_id uuid NOT NULL,
    lease_token uuid NOT NULL UNIQUE, state text NOT NULL CHECK (state IN ('ACTIVE','RELEASED')),
    created_at timestamptz NOT NULL, expires_at timestamptz NOT NULL,
    absolute_expires_at timestamptz NOT NULL, released_at timestamptz,
    FOREIGN KEY (file_id,version_id) REFERENCES files(file_id,version_id) ON DELETE RESTRICT,
    CHECK (created_at<expires_at AND expires_at<=absolute_expires_at)
);
CREATE INDEX download_expiry ON download_leases (file_id,expires_at) WHERE state='ACTIVE';
CREATE TABLE admission_counters (
    resource_key text PRIMARY KEY, limit_count bigint NOT NULL CHECK (limit_count>=0),
    held_count bigint NOT NULL CHECK (held_count BETWEEN 0 AND limit_count),
    revision bigint NOT NULL CHECK (revision>0)
);
