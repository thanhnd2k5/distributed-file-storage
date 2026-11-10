"""Durable destination reservations on isolated PostgreSQL schemas."""

from uuid import uuid4

import pytest
from test_v2_c1 import lease_for, proofs, replica
from test_v2_d2 import available
from test_v2_f2_discovery import schedule
from test_v2_metadata import foundation as foundation
from test_v2_metadata import parallel, sql

from metadata.v2.repository import DomainError, JobLease


def prepare(repo, *, budget=30, domain="domain-2"):
    session = available(repo)
    incarnation = uuid4()
    repo.provision_node(
        "node-2", incarnation, host="storage-2", port=50061, failure_domain=domain, budget=budget
    )
    repo.activate_node(
        "node-2", incarnation, observed_namespace=repo.namespace_id, observed_epoch=repo.epoch
    )
    sql(repo, "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-0'")
    (deficit,) = repo.repair_scan_page()["deficits"]
    job_id = schedule(repo, deficit)
    job = repo.claim_jobs(uuid4(), kinds=("REPAIR",), target=deficit["chunk_id"])[0]
    assert job["job_id"] == job_id
    lease = JobLease(job_id, job["lease_owner"], job["fencing_token"])
    return session, (deficit["chunk_id"], deficit["version_id"]), lease


def stored(destination):
    (ack,) = proofs(dict(destination, destinations=[destination]))
    return ack


def test_replay_and_reclaim_preserve_one_charge_and_fence_old_ack(foundation):
    repo = foundation
    _, key, lease = prepare(repo)
    first, second = parallel(
        [
            lambda: repo.reserve_repair(*key, lease),
            lambda: replica(repo).reserve_repair(*key, lease),
        ]
    )
    assert first == second
    assert (first["node_id"], first["generation"]) == ("node-2", 1)
    sql(
        repo, "UPDATE jobs SET lease_expires_at=clock_timestamp() WHERE job_id=:id", id=lease.job_id
    )
    new = lease_for(repo, "REPAIR")
    dest = repo.reserve_repair(*key, new)
    assert dest["allocation_id"] == first["allocation_id"]
    assert dest["generation"] == 2
    with pytest.raises(DomainError, match="STALE_JOB"):
        repo.confirm_repair(*key, lease, stored(first))
    with pytest.raises(DomainError, match="INVALID_ALLOCATION_ACK"):
        repo.confirm_repair(*key, new, stored(first))
    repo.confirm_repair(*key, new, stored(dest))
    assert sql(repo, "SELECT charged_bytes FROM node_budgets WHERE node_id='node-2'") == [
        {"charged_bytes": 3}
    ]
    assert sql(repo, "SELECT state FROM jobs WHERE job_id=:id", id=new.job_id) == [
        {"state": "DONE"}
    ]
    assert sql(
        repo,
        "SELECT generation,state FROM write_attempts WHERE allocation_id=:id ORDER BY generation",
        id=dest["allocation_id"],
    ) == [{"generation": 1, "state": "CLOSED"}, {"generation": 2, "state": "ACKED"}]
    assert not repo.repair_scan_page()["deficits"]


@pytest.mark.parametrize(("budget", "domain"), [(2, "domain-2"), (30, "domain-1")])
def test_no_capacity_or_distinct_domain_leaves_no_intent(foundation, budget, domain):
    repo = foundation
    _, key, lease = prepare(repo, budget=budget, domain=domain)
    with pytest.raises(DomainError, match="REPAIR_DESTINATION_UNAVAILABLE"):
        repo.reserve_repair(*key, lease)
    assert not sql(repo, "SELECT * FROM allocations WHERE node_id='node-2'")
    assert sql(repo, "SELECT charged_bytes FROM node_budgets WHERE node_id='node-2'") == [
        {"charged_bytes": 0}
    ]


def test_tombstone_fences_confirmation_and_cleanup_includes_repair(foundation):
    repo = foundation
    session, key, lease = prepare(repo)
    destination = repo.reserve_repair(*key, lease)
    repo.delete_file(session["file_id"])
    with pytest.raises(DomainError, match="REPAIR_NOT_AVAILABLE"):
        repo.confirm_repair(*key, lease, stored(destination))
    cleanup = lease_for(repo, "CLEANUP")
    allocations = repo.authorize_cleanup(session["upload_id"], cleanup)["allocations"]
    assert any(
        row["allocation_id"] == destination["allocation_id"] and row["state"] == "DELETE_PENDING"
        for row in allocations
    )
    assert sql(repo, "SELECT charged_bytes FROM node_budgets WHERE node_id='node-2'") == [
        {"charged_bytes": 3}
    ]
