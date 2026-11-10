"""Repair discovery and deduplication on isolated PostgreSQL schemas."""

from uuid import uuid4

from test_v2_c1 import replica
from test_v2_d2 import available
from test_v2_metadata import foundation as foundation
from test_v2_metadata import parallel, sql


def schedule(repo, deficit):
    return repo.enqueue_repair(deficit["file_id"], deficit["version_id"], deficit["chunk_id"])


def test_scan_advances_past_healthy_part_and_reports_zero_sources(foundation):
    repo = foundation
    available(repo, b"abcdefghi")
    sql(
        repo,
        """UPDATE allocations SET replica_health='CORRUPT' WHERE chunk_id IN
        (SELECT chunk_id FROM manifest_parts WHERE part_index=2)""",
    )
    first = repo.repair_scan_page(limit=2)
    assert first["deficits"] == []
    assert first["next_after"][1] == 1
    last = repo.repair_scan_page(after=first["next_after"], limit=2)
    assert last["next_after"] is None
    (deficit,) = last["deficits"]
    assert (deficit["part_index"], deficit["eligible_nodes"], deficit["eligible_domains"]) == (
        2,
        0,
        0,
    )
    assert schedule(repo, deficit) is None
    assert not sql(repo, "SELECT job_id FROM jobs WHERE kind='REPAIR'")


def test_scan_counts_failure_domains_and_two_connections_enqueue_one_job(foundation):
    repo = foundation
    session = available(repo)
    sql(repo, "UPDATE allocations SET failure_domain_snapshot='same'")
    (deficit,) = repo.repair_scan_page()["deficits"]
    assert (deficit["eligible_nodes"], deficit["eligible_domains"]) == (2, 1)
    before = sql(repo, "SELECT * FROM node_budgets ORDER BY node_id")
    jobs = parallel([lambda: schedule(repo, deficit), lambda: schedule(replica(repo), deficit)])
    assert jobs[0] is not None and jobs[0] == jobs[1]
    rows = sql(repo, "SELECT job_id,target_id,payload FROM jobs WHERE kind='REPAIR'")
    assert rows == [
        dict(
            job_id=jobs[0],
            target_id=deficit["chunk_id"],
            payload=dict(file_id=str(session["file_id"]), version_id=str(deficit["version_id"])),
        )
    ]
    assert sql(repo, "SELECT * FROM node_budgets ORDER BY node_id") == before


def test_enqueue_rechecks_version_health_and_tombstone(foundation):
    repo = foundation
    session = available(repo)
    sql(repo, "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-0'")
    (deficit,) = repo.repair_scan_page()["deficits"]
    assert deficit["eligible_nodes"] == 1
    assert schedule(repo, {**deficit, "version_id": uuid4()}) is None
    sql(repo, "UPDATE storage_nodes SET health_state='ACTIVE' WHERE node_id='node-0'")
    assert schedule(repo, deficit) is None
    sql(repo, "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-0'")
    repo.delete_file(session["file_id"])
    assert schedule(repo, deficit) is None
    assert not repo.repair_scan_page()["deficits"]
    assert not sql(repo, "SELECT job_id FROM jobs WHERE kind='REPAIR'")
