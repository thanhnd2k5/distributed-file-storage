ALTER TABLE v2_schema_version DROP CONSTRAINT v2_schema_version_version_check;
CREATE TABLE node_health_polls (
    node_id varchar(64) PRIMARY KEY REFERENCES storage_nodes(node_id) ON DELETE RESTRICT,
    next_poll_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    ticket_expires_at timestamptz,
    consecutive_failures integer NOT NULL DEFAULT 0 CHECK (consecutive_failures BETWEEN 0 AND 3),
    consecutive_successes integer NOT NULL DEFAULT 0 CHECK (consecutive_successes BETWEEN 0 AND 2),
    writable boolean
);
CREATE INDEX node_health_due ON node_health_polls (next_poll_at,node_id);
INSERT INTO node_health_polls (node_id) SELECT node_id FROM storage_nodes;
