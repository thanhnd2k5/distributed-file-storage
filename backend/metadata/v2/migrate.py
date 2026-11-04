"""Forward-only, checksummed V2 migration; never import V1 Alembic/models.

One database per V2 namespace. Tests may use explicitly owned empty schemas.
No downgrade/reset path: an existing unrelated schema is rejected.
"""

import hashlib
import re
from pathlib import Path
from uuid import UUID

from sqlalchemy import text


def migrate(
    engine, namespace_id: UUID, recovery_epoch: UUID, *, schema="public", session_limit=128
):
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", schema):
        raise ValueError("Invalid schema")
    if type(session_limit) is not int or not 0 < session_limit <= 2**63 - 1:
        raise ValueError("Invalid admission limit")
    source = (Path(__file__).parent / "migrations/0001_foundation.sql").read_text(encoding="utf-8")
    digest = hashlib.sha256(source.encode()).hexdigest()
    with engine.begin() as connection:
        connection.execute(text("SET LOCAL lock_timeout='3s'"))
        connection.execute(text("SET LOCAL statement_timeout='15s'"))
        # Serialize migration only, not normal traffic; lock includes database/schema.
        connection.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(current_database() || :schema,0))"),
            {"schema": schema},
        )
        connection.execute(text(f'SET LOCAL search_path TO "{schema}"'))
        tables = set(
            connection.execute(
                text("SELECT tablename FROM pg_tables WHERE schemaname=:schema"), {"schema": schema}
            ).scalars()
        )
        if tables and "v2_schema_version" not in tables:
            raise ValueError(
                "V2 requires an empty dedicated schema; existing tables were preserved"
            )
        if "v2_schema_version" in tables:
            row = connection.execute(text("SELECT version,checksum FROM v2_schema_version")).one()
            if row != (1, digest):
                raise ValueError("V2 migration version/checksum differs")
            control = connection.execute(
                text("SELECT namespace_id,recovery_epoch FROM cluster_control WHERE singleton=1")
            ).one()
            if control != (namespace_id, recovery_epoch):
                raise ValueError("Namespace/epoch mismatch; migration does not perform recovery")
            return False
        for statement in source.split(";"):
            if statement.strip():
                connection.execute(text(statement))
        connection.execute(
            text("""CREATE TABLE v2_schema_version (
            version integer PRIMARY KEY CHECK(version=1), checksum varchar(64) NOT NULL
        )""")
        )
        connection.execute(
            text("INSERT INTO v2_schema_version VALUES (1,:digest)"), {"digest": digest}
        )
        connection.execute(
            text("""INSERT INTO cluster_control
            (singleton,namespace_id,recovery_epoch,mode) VALUES (1,:namespace,:epoch,'ACTIVE')"""),
            {"namespace": namespace_id, "epoch": recovery_epoch},
        )
        connection.execute(
            text("INSERT INTO admission_counters VALUES ('upload_sessions',:limit,0,1)"),
            {"limit": session_limit},
        )
    return True
