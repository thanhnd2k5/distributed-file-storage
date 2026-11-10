"""Health poll fencing and schema upgrade against the V2 PostgreSQL fixture."""

from threading import Event
from uuid import uuid4

import pytest
from sqlalchemy import text
from test_v2_c2 import cluster as cluster
from test_v2_c2 import confirmed
from test_v2_metadata import claim, create, nodes, parallel, sql
from test_v2_metadata import foundation as foundation

from metadata.v2 import migrate as migration
from metadata.v2.health import HealthObservation
from metadata.v2.repository import DomainError, Repository
from metadata.v2.storage_client import StorageClient


def expire_poll(repo):
    sql(
        repo,
        """UPDATE node_health_polls SET next_poll_at=clock_timestamp()-interval '1 second',
        ticket_expires_at=clock_timestamp()-interval '1 second' WHERE node_id='node-0'""",
    )


def test_health_ticket_is_exclusive_expires_and_fences_late_result(foundation):
    repo = foundation
    nodes(repo)
    sql(repo, "UPDATE storage_nodes SET enabled=false WHERE node_id='node-1'")
    other = Repository(repo.engine, repo.namespace_id, repo.epoch, schema=repo.schema)
    claims = parallel([repo.claim_health_poll, other.claim_health_poll])
    assert claims.count(None) == 1
    old = next(ticket for ticket in claims if ticket)
    expire_poll(repo)
    unreachable = HealthObservation("unreachable")
    assert not repo.apply_health_poll(old, unreachable)
    new = other.claim_health_poll()
    assert new["generation"] > old["generation"]
    assert not repo.apply_health_poll(old, unreachable)
    assert other.apply_health_poll(new, unreachable)
    assert not repo.apply_health_poll(new, unreachable)
    assert (
        sql(repo, "SELECT health_state FROM storage_nodes WHERE node_id='node-0'")[0][
            "health_state"
        ]
        == "SUSPECTED"
    )
    assert (
        sql(repo, "SELECT consecutive_failures FROM node_health_polls WHERE node_id='node-0'")[0][
            "consecutive_failures"
        ]
        == 1
    )
    assert repo.claim_health_poll() is None


def test_reactivation_fences_an_outstanding_poll(foundation):
    repo = foundation
    ids = dict(nodes(repo))
    ticket = repo.claim_health_poll()
    node_id = ticket["node_id"]
    repo.activate_node(
        node_id,
        ids[node_id],
        observed_namespace=repo.namespace_id,
        observed_epoch=repo.epoch,
    )
    assert not repo.apply_health_poll(ticket, HealthObservation("scope_mismatch"))


def test_suspected_node_is_excluded_from_new_placement(foundation):
    repo = foundation
    nodes(repo)
    ticket = repo.claim_health_poll()
    assert repo.apply_health_poll(ticket, HealthObservation("unreachable"))
    with pytest.raises(DomainError, match="INSUFFICIENT_DOMAINS"):
        claim(repo, create(repo))
    assert not sql(repo, "SELECT allocation_id FROM allocations")


@pytest.mark.parametrize("mismatch", [False, True])
def test_real_health_response_keeps_or_quarantines_node(cluster, mismatch):
    repo, _ = cluster
    confirmed(repo, b"abc")
    sql(repo, "UPDATE allocations SET replica_health='CORRUPT' WHERE node_id='node-0'")
    sql(repo, "UPDATE storage_nodes SET enabled=false WHERE node_id='node-1'")
    if mismatch:
        sql(repo, "UPDATE storage_nodes SET failure_domain='other' WHERE node_id='node-0'")
    ticket = repo.claim_health_poll()
    observation = StorageClient().health(ticket, Event())
    assert observation.outcome == ("scope_mismatch" if mismatch else "reachable")
    assert repo.apply_health_poll(ticket, observation)
    assert sql(repo, "SELECT health_state FROM storage_nodes WHERE node_id='node-0'")[0][
        "health_state"
    ] == ("QUARANTINED" if mismatch else "ACTIVE")
    assert sql(
        repo, "SELECT allocatable_bytes,charged_bytes FROM node_budgets WHERE node_id='node-0'"
    ) == [{"allocatable_bytes": 300, "charged_bytes": 3}]
    assert sql(repo, "SELECT replica_health FROM allocations WHERE node_id='node-0'") == [
        {"replica_health": "CORRUPT"}
    ]


def test_version_one_upgrade_backfills_and_preserves_data(foundation, monkeypatch):
    engine = foundation.engine
    repo = Repository(engine, uuid4(), uuid4(), schema="upgrade_" + uuid4().hex)
    args = (engine, repo.namespace_id, repo.epoch)
    with engine.begin() as db:
        db.execute(text(f'CREATE SCHEMA "{repo.schema}"'))
    try:
        with monkeypatch.context() as patch:
            patch.setattr(migration, "MIGRATIONS", migration.MIGRATIONS[:1])
            assert migration.migrate(*args, schema=repo.schema, session_limit=8)
        repo.provision_node(
            "old", uuid4(), host="storage", port=50061, failure_domain="a", budget=30
        )
        original = sql(repo, "SELECT version,checksum FROM v2_schema_version")[0]
        with pytest.raises(ValueError, match="mismatch"):
            migration.migrate(engine, uuid4(), repo.epoch, schema=repo.schema)
        assert sql(repo, "SELECT version,checksum FROM v2_schema_version") == [original]
        assert migration.migrate(*args, schema=repo.schema)
        assert not migration.migrate(*args, schema=repo.schema)
        versions = sql(repo, "SELECT version,checksum FROM v2_schema_version ORDER BY version")
        assert versions[0] == original and versions[1]["version"] == 2
        assert sql(repo, "SELECT node_id FROM node_health_polls") == [{"node_id": "old"}]
        assert sql(repo, "SELECT limit_count FROM admission_counters") == [{"limit_count": 8}]
        assert sql(repo, "SELECT allocatable_bytes FROM node_budgets") == [
            {"allocatable_bytes": 30}
        ]
    finally:
        with engine.begin() as db:
            db.execute(text(f'DROP SCHEMA "{repo.schema}" CASCADE'))
