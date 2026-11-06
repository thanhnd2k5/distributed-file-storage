"""E2 durable accounting: real PostgreSQL and POSIX disks across fresh processes.

No HTTP/RPC or scheduling changes. Evidence here concerns the commit/owner
boundary; existing C2/D5 tests cover the adapters and complete transfer flows.
"""

import hashlib
from dataclasses import replace
from uuid import uuid4

import pytest
from test_v2_c1 import claim, lease_for, proofs, upload
from test_v2_metadata import create, evidence, nodes, sql
from test_v2_metadata import foundation as foundation
from v2_e2_process import Call, processes

from metadata.v2.repository import JobLease, Policy

COUNTER = "SELECT * FROM admission_counters WHERE resource_key='upload_sessions' FOR UPDATE"
FILES = "SELECT * FROM files ORDER BY file_id FOR UPDATE"
BUDGET = "SELECT * FROM node_budgets ORDER BY node_id,incarnation FOR UPDATE"


def accounting(repo, *, sessions, claims, charged):
    # A single statement uses one MVCC snapshot for owners AND counters. Never
    # compare independently sampled totals or repair counters from this audit.
    rows = sql(
        repo,
        """SELECT 'sessions' AS resource,resource_key AS identity,
        held_count AS actual,(SELECT count(*) FROM upload_sessions
        WHERE admission_released_at IS NULL) AS expected,limit_count AS cap
        FROM admission_counters WHERE resource_key='upload_sessions'
        UNION ALL
        SELECT 'claims',s.upload_id::text,s.active_claims,
        (SELECT count(*) FROM upload_parts p WHERE p.upload_id=s.upload_id AND p.claim_held),
        s.max_parallel_parts FROM upload_sessions s
        UNION ALL
        SELECT 'bytes',b.node_id || '/' || b.incarnation::text,b.charged_bytes,
        (SELECT coalesce(sum(a.charged_bytes),0) FROM allocations a
        WHERE (a.node_id,a.incarnation)=(b.node_id,b.incarnation) AND a.state!='RELEASED'),
        b.allocatable_bytes FROM node_budgets b""",
    )
    assert rows
    for row in rows:
        assert 0 <= row["actual"] == row["expected"] <= row["cap"], row
    totals = {
        kind: sum(row["actual"] for row in rows if row["resource"] == kind)
        for kind in ("sessions", "claims", "bytes")
    }
    assert totals == dict(sessions=sessions, claims=claims, bytes=charged)
    assert not sql(
        repo,
        "SELECT upload_id FROM upload_sessions WHERE "
        "(state IN ('OPEN','FINALIZING')) != (admission_released_at IS NULL)",
    )
    assert not sql(
        repo,
        "SELECT allocation_id FROM allocations WHERE state='RELEASED' "
        "AND cleanup_complete_at IS NULL",
    )


def new_upload(*, key=None, size=3, fault=None):
    return Call(
        "create_upload",
        (key or uuid4(),),
        dict(original_name="e2.bin", size_bytes=size, expected_sha256="a" * 64),
        fault,
    )


def new_claim(session, index=0, *, fault=None):
    return Call("claim_part", (session["upload_id"], index, 3, "b" * 64, uuid4()), fault=fault)


def confirm_call(session, part, *, fault=None):
    return Call(
        "confirm_part",
        (
            session["upload_id"],
            part["part_index"],
            part["claim_token"],
            part["revision"],
            proofs(part),
        ),
        fault=fault,
    )


def uncertain_call(session, part):
    return Call("uncertain", (session["upload_id"], 0, part["claim_token"], part["revision"]))


def test_independent_create_contenders_share_one_cluster_slot(foundation):
    repo = foundation
    sql(repo, "UPDATE admission_counters SET limit_count=1")
    results = processes(repo, new_upload(), new_upload(), hold=COUNTER)
    assert sum("value" in result for result in results) == 1
    assert {"error": "RESOURCE_LIMIT"} in results
    assert len(sql(repo, "SELECT * FROM files")) == 1
    accounting(repo, sessions=1, claims=0, charged=0)


def test_create_replay_at_full_cap_uses_the_same_owner(foundation):
    repo, key = foundation, uuid4()
    sql(repo, "UPDATE admission_counters SET limit_count=1")
    results = processes(repo, new_upload(key=key), new_upload(key=key), hold=COUNTER)
    first, second = [result["value"] for result in results]
    assert first[0]["upload_id"] == second[0]["upload_id"]
    assert sorted([first[1], second[1]]) == [False, True]
    accounting(repo, sessions=1, claims=0, charged=0)


@pytest.mark.parametrize("fault", ["before_commit", "after_commit"])
def test_create_crash_replay_has_no_orphan_or_double_admission(foundation, fault):
    repo, key = foundation, uuid4()
    processes(repo, new_upload(key=key, fault=fault))
    committed = int(fault == "after_commit")
    accounting(repo, sessions=committed, claims=0, charged=0)
    assert len(sql(repo, "SELECT * FROM files")) == committed
    result = processes(repo, new_upload(key=key))[0]["value"]
    assert result[1] == bool(committed)
    assert len(sql(repo, "SELECT * FROM idempotency_records")) == 1
    accounting(repo, sessions=1, claims=0, charged=0)


@pytest.mark.parametrize("terminal", ["abort", "delete_file", "expire_sessions"])
def test_terminal_race_returns_session_admission_once(foundation, terminal):
    repo = foundation
    nodes(repo)
    session = create(repo)
    claim_part = new_claim(session)
    processes(repo, claim_part)
    if terminal == "expire_sessions":
        sql(repo, "UPDATE upload_sessions SET expires_at=clock_timestamp()-interval '1 second'")
    command = Call(
        terminal,
        ()
        if terminal == "expire_sessions"
        else (session["file_id"] if terminal == "delete_file" else session["upload_id"],),
    )
    results = processes(repo, Call("abort", (session["upload_id"],)), command, hold=COUNTER)
    assert all("value" in result for result in results)
    accounting(repo, sessions=0, claims=1, charged=6)
    assert len(sql(repo, "SELECT * FROM jobs WHERE kind='CLEANUP'")) == 1
    # Another process can use the returned slot, but bytes remain charged.
    sql(repo, "UPDATE admission_counters SET limit_count=1")
    processes(repo, new_upload())
    accounting(repo, sessions=1, claims=1, charged=6)


@pytest.mark.parametrize("outcome", ["publish", "checksum_mismatch", "exhausted"])
def test_finalize_terminal_commit_lost_ack_does_not_release_twice(foundation, outcome):
    repo = foundation
    sql(repo, "UPDATE admission_counters SET limit_count=1")
    nodes(repo)
    session = upload(repo)
    part = claim(repo, session)
    repo.confirm_part(session["upload_id"], 0, part["claim_token"], part["revision"], proofs(part))
    session = repo.complete_upload(session["upload_id"])
    lease = lease_for(repo, "FINALIZE")
    accounting(repo, sessions=1, claims=0, charged=6)
    if outcome == "exhausted":
        job = sql(repo, "SELECT * FROM jobs WHERE job_id=:id", id=lease.job_id)[0]
        command = Call(
            "fail_worker_job", (job, lease), dict(error="STORAGE_UNAVAILABLE", max_attempts=1)
        )
    else:
        digest = hashlib.sha256(b"abc" if outcome == "publish" else b"bad").hexdigest()
        command = Call(
            "publish_finalize", (session["upload_id"], session["revision"], 3, digest, lease)
        )
    processes(repo, replace(command, fault="after_commit"))
    accounting(repo, sessions=0, claims=0, charged=6)
    expected = "COMPLETED" if outcome == "publish" else "FAILED"
    assert repo.get_session(session["upload_id"])["state"] == expected
    result = processes(repo, command)[0]
    assert result == {"error": "STALE_JOB" if outcome == "exhausted" else "UPLOAD_STATE_CONFLICT"}
    assert len(sql(repo, "SELECT * FROM jobs WHERE kind='CLEANUP'")) == int(outcome != "publish")
    processes(repo, new_upload())
    accounting(repo, sessions=1, claims=0, charged=6)


@pytest.mark.parametrize("second_index", [0, 1])
def test_claim_contenders_share_the_session_cap(foundation, second_index):
    repo = foundation
    repo.policy = replace(repo.policy, max_parallel_parts=1)
    nodes(repo)
    session = create(repo, size=6)
    results = processes(repo, new_claim(session), new_claim(session, second_index), hold=FILES)
    assert sum("value" in result for result in results) == 1
    assert {"error": "PART_BUSY" if second_index == 0 else "RESOURCE_LIMIT"} in results
    accounting(repo, sessions=1, claims=1, charged=6)
    assert len(sql(repo, "SELECT * FROM allocations")) == 2
    assert len(sql(repo, "SELECT * FROM write_attempts")) == 2


def test_two_sessions_contend_for_the_same_node_capacity(foundation):
    repo = foundation
    nodes(repo, budget=3)
    first, second = create(repo), create(repo)
    results = processes(repo, new_claim(first), new_claim(second), hold=BUDGET)
    assert sum("value" in result for result in results) == 1
    assert {"error": "INSUFFICIENT_DOMAINS"} in results
    accounting(repo, sessions=2, claims=1, charged=6)
    assert len(sql(repo, "SELECT * FROM upload_parts")) == 1


@pytest.mark.parametrize("fault", ["before_commit", "after_commit"])
def test_claim_crash_and_sweep_cannot_recredit_uncertain_bytes(foundation, fault):
    repo = foundation
    repo.policy = replace(repo.policy, max_parallel_parts=1)
    nodes(repo, budget=3)
    session = create(repo, size=6)
    processes(repo, new_claim(session, fault=fault))
    committed = int(fault == "after_commit")
    accounting(repo, sessions=1, claims=committed, charged=6 * committed)
    assert len(sql(repo, "SELECT * FROM write_attempts")) == 2 * committed
    replay = processes(repo, new_claim(session))[0]
    if committed:
        assert replay == {"error": "PART_BUSY"}
    else:
        assert "value" in replay
    accounting(repo, sessions=1, claims=1, charged=6)
    sql(repo, "UPDATE upload_parts SET claim_expires_at=clock_timestamp()-interval '1 second'")
    results = processes(repo, Call("sweep_claims"), Call("sweep_claims"), hold=FILES)
    assert sorted(result["value"] for result in results) == [0, 1]
    accounting(repo, sessions=1, claims=0, charged=6)
    assert processes(repo, new_claim(session))[0] == {"error": "OUTCOME_PENDING"}
    assert processes(repo, new_claim(session, 1))[0] == {"error": "INSUFFICIENT_DOMAINS"}
    assert len(sql(repo, "SELECT * FROM jobs WHERE kind='RECONCILE'")) == 1
    accounting(repo, sessions=1, claims=0, charged=6)


def test_confirm_lost_commit_ack_replayed_by_two_new_processes(foundation):
    repo = foundation
    nodes(repo)
    session = upload(repo)
    part = claim(repo, session)
    processes(repo, confirm_call(session, part, fault="after_commit"))
    accounting(repo, sessions=1, claims=0, charged=6)
    results = processes(repo, confirm_call(session, part), confirm_call(session, part), hold=FILES)
    assert all(result["value"]["state"] == "VERIFIED" for result in results)
    assert results[0]["value"]["confirmed_at"] == results[1]["value"]["confirmed_at"]
    accounting(repo, sessions=1, claims=0, charged=6)


def test_confirm_and_uncertain_race_return_one_claim_slot(foundation):
    repo = foundation
    nodes(repo)
    session = upload(repo)
    part = claim(repo, session)
    results = processes(
        repo, confirm_call(session, part), uncertain_call(session, part), hold=FILES
    )
    assert "value" in results[1]
    assert "value" in results[0] or results[0] == {"error": "STALE_CLAIM"}
    accounting(repo, sessions=1, claims=0, charged=6)
    assert len(sql(repo, "SELECT * FROM allocations")) == 2


def test_retry_and_new_claim_share_cap_without_recharging_existing_allocations(foundation):
    repo = foundation
    repo.policy = Policy(chunk_size_bytes=3, max_parallel_parts=1)
    nodes(repo)
    session = upload(repo, b"abcdef")
    part = claim(repo, session)
    repo.uncertain(session["upload_id"], 0, part["claim_token"], part["revision"])
    lease = lease_for(repo, "RECONCILE")
    intent = repo.authorize_part_retry(session["upload_id"], 0, part["revision"], uuid4(), lease)
    accounting(repo, sessions=1, claims=0, charged=6)
    retry = Call(
        "retry_part",
        (
            session["upload_id"],
            0,
            intent["claim_token"],
            intent["revision"],
            proofs(intent, "PREPARED"),
            lease,
        ),
    )
    # Both parts must use the same spec for their independent claim attempt.
    fresh = Call("claim_part", (session["upload_id"], 1, 3, "b" * 64, uuid4()))
    results = processes(repo, retry, fresh, hold=FILES)
    assert sum("value" in result for result in results) == 1
    assert {"error": "RESOURCE_LIMIT"} in results
    retry_won = "value" in results[0]
    accounting(repo, sessions=1, claims=1, charged=6 if retry_won else 12)
    assert len(sql(repo, "SELECT * FROM allocations")) == (2 if retry_won else 4)
    assert len(sql(repo, "SELECT * FROM write_attempts")) == (4 if retry_won else 6)


@pytest.mark.parametrize("fault", ["before_commit", "after_commit"])
def test_cleanup_commit_crash_and_competing_ack_release_once(foundation, fault):
    repo = foundation
    nodes(repo)
    session = create(repo)
    part = processes(repo, new_claim(session))[0]["value"]
    repo.abort(session["upload_id"])
    lease = lease_for(repo, "CLEANUP")
    repo.authorize_cleanup(session["upload_id"], lease)
    proof = evidence(repo, part["destinations"][0])
    processes(repo, Call("release_allocation", (proof, lease), fault=fault))
    committed = int(fault == "after_commit")
    accounting(repo, sessions=0, claims=0, charged=6 - 3 * committed)
    call = Call("release_allocation", (proof, lease))
    results = processes(repo, call, call, hold=FILES)
    assert sum(result["value"] for result in results) == 1 - committed
    accounting(repo, sessions=0, claims=0, charged=3)
    assert sql(repo, "SELECT count(*) AS n FROM write_attempts WHERE state='CLOSED'")[0]["n"] == 1


def test_real_storage_lost_store_and_delete_ack_survive_process_restart(foundation, tmp_path):
    repo = foundation
    nodes(repo, budget=3)
    session = upload(repo)
    part = claim(repo, session)
    destinations = part["destinations"]
    for d in destinations:
        root = str(tmp_path / d["node_id"])
        processes(repo, Call("storage_store", (root, d), fault="lost_storage_ack"))
        row = processes(repo, Call("storage_inspect", (root, d)))[0]["value"]
        assert row["state"] == "STORED"
        assert row["actual_hash"] == hashlib.sha256(b"abc").hexdigest()
    # Neither the physical write nor its recovery granted or released anything.
    accounting(repo, sessions=1, claims=1, charged=6)
    processes(repo, uncertain_call(session, part), uncertain_call(session, part), hold=FILES)
    accounting(repo, sessions=1, claims=0, charged=6)
    processes(repo, Call("abort", (session["upload_id"],), fault="after_commit"))
    accounting(repo, sessions=0, claims=0, charged=6)
    lease = lease_for(repo, "CLEANUP")
    repo.authorize_cleanup(session["upload_id"], lease)
    for index, d in enumerate(destinations):
        repo.renew_job(lease.job_id, lease.owner, lease.token)
        root = str(tmp_path / d["node_id"])
        processes(repo, Call("storage_delete", (root, d), fault="lost_storage_ack"))
        row = processes(repo, Call("storage_inspect", (root, d)))[0]["value"]
        assert row["state"] == "DELETED" and row["cleanup_complete"]
        accounting(repo, sessions=0, claims=0, charged=6 - 3 * index)
        # Delete retry from a fresh volume owner returns the actual durable ACK.
        proof = processes(repo, Call("storage_delete", (root, d)))[0]["value"]
        for wrong in (
            replace(proof, state="NOT_FOUND"),
            replace(proof, cleanup_complete=False),
            replace(proof, incarnation=uuid4()),
            replace(proof, generation=2),
        ):
            assert processes(repo, Call("release_allocation", (wrong, lease)))[0] == {
                "error": "INVALID_DELETE_ACK"
            }
        accounting(repo, sessions=0, claims=0, charged=6 - 3 * index)
        call = Call("release_allocation", (proof, lease))
        assert sorted(r["value"] for r in processes(repo, call, call, hold=FILES)) == [False, True]
        accounting(repo, sessions=0, claims=0, charged=3 - 3 * index)
        late = processes(repo, Call("storage_store", (root, d)))[0]
        assert "error" in late  # Restart preserves the tombstone against late writes.
    repo.finish_cleanup(session["upload_id"], lease)
    accounting(repo, sessions=0, claims=0, charged=0)
    # The returned capacity is usable by a different owner/process.
    replacement = processes(repo, new_upload())[0]["value"][0]
    processes(repo, new_claim(replacement))
    accounting(repo, sessions=1, claims=1, charged=6)


def test_job_reclaim_fences_late_cleanup_ack_without_resetting_capacity(foundation):
    repo = foundation
    nodes(repo)
    session = create(repo)
    part = processes(repo, new_claim(session))[0]["value"]
    repo.abort(session["upload_id"])
    calls = [Call("claim_jobs", (uuid4(),), dict(kinds=["CLEANUP"])) for _ in range(2)]
    results = processes(repo, *calls)
    assert sorted(len(result["value"]) for result in results) == [0, 1]
    first = next(result["value"][0] for result in results if result["value"])
    old = JobLease(first["job_id"], first["lease_owner"], first["fencing_token"])
    repo.authorize_cleanup(session["upload_id"], old)
    sql(repo, "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second'")
    calls = [Call("claim_jobs", (uuid4(),), dict(kinds=["CLEANUP"])) for _ in range(2)]
    current = processes(repo, *calls)
    assert sorted(len(result["value"]) for result in current) == [0, 1]
    job = next(result["value"][0] for result in current if result["value"])
    lease = JobLease(job["job_id"], job["lease_owner"], job["fencing_token"])
    assert lease.token == old.token + 1 and lease.owner != old.owner
    proof = evidence(repo, part["destinations"][0])
    assert processes(repo, Call("release_allocation", (proof, old)))[0] == {"error": "STALE_JOB"}
    accounting(repo, sessions=0, claims=0, charged=6)
    assert processes(repo, Call("release_allocation", (proof, lease)))[0]["value"]
    accounting(repo, sessions=0, claims=0, charged=3)
