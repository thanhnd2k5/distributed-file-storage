"""C1 transaction gates on isolated PostgreSQL schemas, independent connections."""

import hashlib
from dataclasses import replace
from threading import Event
from uuid import uuid4

import pytest
from test_v2_metadata import foundation as foundation
from test_v2_metadata import nodes, parallel, sql

from metadata.v2.repository import (
    AllocationEvidence,
    DeleteEvidence,
    DomainError,
    JobLease,
    Repository,
)


def upload(repo, data=b"abc"):
    return repo.create_upload(
        uuid4(),
        original_name="source.bin",
        size_bytes=len(data),
        expected_sha256=hashlib.sha256(data).hexdigest(),
    )[0]


def claim(repo, session, *, index=0, data=b"abc"):
    return repo.claim_part(
        session["upload_id"], index, len(data), hashlib.sha256(data).hexdigest(), uuid4()
    )


def proofs(part, state="STORED"):
    return [
        AllocationEvidence(
            d["namespace_id"],
            d["recovery_epoch"],
            d["node_id"],
            d["incarnation"],
            d["allocation_id"],
            d["generation"],
            part["chunk_id"],
            part["size_bytes"],
            part["checksum_sha256"],
            state,
            part["size_bytes"] if state == "STORED" else None,
            part["checksum_sha256"] if state == "STORED" else None,
        )
        for d in part["destinations"]
    ]


def confirm(repo, session, part):
    return repo.confirm_part(
        session["upload_id"],
        part["part_index"],
        part["claim_token"],
        part["revision"],
        proofs(part),
    )


def lease_for(repo, kind):
    job = next(j for j in repo.claim_jobs(uuid4()) if j["kind"] == kind)
    return JobLease(job["job_id"], job["lease_owner"], job["fencing_token"])


def finalizing(repo, data=b"abc"):
    if data:
        nodes(repo)
    session = upload(repo, data)
    for index in range(session["part_count"]):
        part = claim(repo, session, index=index, data=data[index * 3 : index * 3 + 3])
        confirm(repo, session, part)
    result = repo.complete_upload(session["upload_id"])
    return result, lease_for(repo, "FINALIZE")


def publish(repo, session, lease, *, data=b"abc"):
    return repo.publish_finalize(
        session["upload_id"],
        session["revision"],
        len(data),
        hashlib.sha256(data).hexdigest(),
        lease,
    )


def replica(repo):
    return Repository(
        repo.engine, repo.namespace_id, repo.epoch, schema=repo.schema, policy=repo.policy
    )


def counters(repo):
    return (
        sql(repo, "SELECT held_count FROM admission_counters")[0]["held_count"],
        sql(repo, "SELECT COALESCE(sum(charged_bytes),0) AS n FROM node_budgets")[0]["n"],
    )


def test_renew_confirm_replay_preserves_slots_charge_and_ack_time(foundation):
    repo = foundation
    nodes(repo)
    session = upload(repo)
    part = claim(repo, session)
    sql(repo, "UPDATE upload_parts SET claim_expires_at=clock_timestamp()+interval '5 seconds'")
    expiry = repo.renew_part(session["upload_id"], 0, part["claim_token"], part["revision"])
    assert all(
        row["lease_expires_at"] == expiry for row in sql(repo, "SELECT * FROM write_attempts")
    )
    first, replay = confirm(repo, session, part), confirm(repo, session, part)
    assert first["state"] == "VERIFIED" and replay["confirmed_at"] == first["confirmed_at"]
    assert not first["claim_held"]
    assert repo.get_session(session["upload_id"])["active_claims"] == 0
    assert counters(repo) == (1, 6)
    assert (
        len(
            sql(
                repo, "SELECT * FROM allocations WHERE state='STORED' AND replica_health='VERIFIED'"
            )
        )
        == 2
    )
    before = sql(repo, "SELECT * FROM allocations ORDER BY allocation_id")
    sql(repo, "UPDATE upload_parts SET claim_expires_at=clock_timestamp()-interval '1 second'")
    assert confirm(repo, session, part)["confirmed_at"] == first["confirmed_at"]
    assert claim(repo, session)["state"] == "VERIFIED"
    assert before == sql(repo, "SELECT * FROM allocations ORDER BY allocation_id")
    with pytest.raises(DomainError, match="PART_CONFLICT"):
        claim(repo, session, data=b"xyz")


@pytest.mark.parametrize("change", ["token", "revision", "claim_expiry", "session_expiry", "abort"])
def test_stale_claim_never_renews_or_confirms(foundation, change):
    repo = foundation
    nodes(repo)
    session = upload(repo)
    part = claim(repo, session)
    token, revision = part["claim_token"], part["revision"]
    if change == "token":
        token = uuid4()
    elif change == "revision":
        revision += 1
    elif change == "claim_expiry":
        sql(repo, "UPDATE upload_parts SET claim_expires_at=clock_timestamp()-interval '1 second'")
    elif change == "session_expiry":
        sql(repo, "UPDATE upload_sessions SET expires_at=clock_timestamp()-interval '1 second'")
    else:
        repo.abort(session["upload_id"])
    for action in [
        lambda: repo.renew_part(session["upload_id"], 0, token, revision),
        lambda: repo.confirm_part(session["upload_id"], 0, token, revision, proofs(part)),
    ]:
        with pytest.raises(DomainError):
            action()
    assert counters(repo)[1] == 6
    assert all(row["state"] == "RESERVED" for row in sql(repo, "SELECT * FROM allocations"))


@pytest.mark.parametrize(
    "field",
    [
        "namespace_id",
        "recovery_epoch",
        "node_id",
        "incarnation",
        "allocation_id",
        "generation",
        "chunk_id",
        "size_bytes",
        "checksum_sha256",
        "state",
        "actual_size",
        "actual_hash",
        "duplicate",
        "missing",
    ],
)
def test_confirm_rejects_inexact_ack_without_partial_commit(foundation, field):
    repo = foundation
    nodes(repo)
    session = upload(repo)
    part = claim(repo, session)
    acks = proofs(part)
    if field == "duplicate":
        acks = [acks[0], acks[0]]
    elif field == "missing":
        acks.pop()
    else:
        wrong = {
            "node_id": "wrong-node",
            "generation": 2,
            "size_bytes": 4,
            "checksum_sha256": "0" * 64,
            "state": "PREPARED",
            "actual_size": 2,
            "actual_hash": "0" * 64,
        }.get(field, uuid4())
        acks[1] = replace(acks[1], **{field: wrong})
    with pytest.raises(DomainError, match="INVALID_ALLOCATION_ACK"):
        repo.confirm_part(session["upload_id"], 0, part["claim_token"], part["revision"], acks)
    assert repo.get_session(session["upload_id"])["active_claims"] == 1
    assert all(row["state"] == "RESERVED" for row in sql(repo, "SELECT * FROM allocations"))
    assert all(row["state"] == "INTENDED" for row in sql(repo, "SELECT * FROM write_attempts"))
    assert counters(repo) == (1, 6)


def test_concurrent_confirmation_releases_one_slot(foundation):
    repo, other = foundation, replica(foundation)
    nodes(repo)
    session = upload(repo)
    part = claim(repo, session)
    results = parallel(
        [lambda: confirm(repo, session, part), lambda: confirm(other, session, part)]
    )
    assert all(result["state"] == "VERIFIED" for result in results)
    assert results[0]["confirmed_at"] == results[1]["confirmed_at"]
    assert repo.get_session(session["upload_id"])["active_claims"] == 0
    assert counters(repo) == (1, 6)


def test_confirm_rechecks_lease_after_waiting_for_node_lock(foundation, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    repo = foundation
    nodes(repo)
    session = upload(repo)
    part = claim(repo, session)
    reached = Event()
    original = repo._lock_replica_nodes

    def notify(db, upload_id):
        reached.set()
        original(db, upload_id)

    monkeypatch.setattr(repo, "_lock_replica_nodes", notify)
    with ThreadPoolExecutor(1) as executor:
        with repo.engine.begin() as db:
            from sqlalchemy import text

            db.execute(text(f'SET LOCAL search_path TO "{repo.schema}"'))
            db.execute(text("SELECT * FROM storage_nodes FOR UPDATE"))
            db.execute(
                text(
                    "UPDATE upload_parts SET claim_expires_at=clock_timestamp()+interval '1 second'"
                )
            )
        # Commit the TTL first, then hold only the node lock while confirm owns the part.
        with repo.engine.begin() as db:
            db.execute(text(f'SET LOCAL search_path TO "{repo.schema}"'))
            db.execute(text("SELECT * FROM storage_nodes FOR UPDATE"))
            future = executor.submit(confirm, repo, session, part)
            assert reached.wait(timeout=5)
            db.execute(text("SELECT pg_sleep(1.1)"))
        with pytest.raises(DomainError, match="STALE_CLAIM"):
            future.result(timeout=5)
    assert repo.get_session(session["upload_id"])["active_claims"] == 1
    assert not sql(repo, "SELECT * FROM allocations WHERE state='STORED'")


def test_complete_missing_unknown_and_multichunk_manifest_replay(foundation):
    repo = foundation
    nodes(repo)
    session = upload(repo, b"abcd")
    with pytest.raises(DomainError, match="PARTS_INCOMPLETE"):
        repo.complete_upload(session["upload_id"])
    first = claim(repo, session)
    with pytest.raises(DomainError, match="OUTCOME_PENDING"):
        repo.complete_upload(session["upload_id"])
    confirm(repo, session, first)
    with pytest.raises(DomainError, match="PARTS_INCOMPLETE"):
        repo.complete_upload(session["upload_id"])
    last = claim(repo, session, index=1, data=b"d")
    confirm(repo, session, last)
    results = parallel(
        [
            lambda: repo.complete_upload(session["upload_id"]),
            lambda: replica(repo).complete_upload(session["upload_id"]),
        ]
    )
    assert all(s["state"] == "FINALIZING" for s in results)
    manifest = sql(repo, "SELECT * FROM manifest_parts ORDER BY part_index")
    assert [row["size_bytes"] for row in manifest] == [3, 1]
    assert len(sql(repo, "SELECT * FROM jobs WHERE kind='FINALIZE'")) == 1
    assert repo.complete_upload(session["upload_id"])["revision"] == results[0]["revision"]
    assert manifest == sql(repo, "SELECT * FROM manifest_parts ORDER BY part_index")
    with pytest.raises(DomainError, match="UPLOAD_STATE_CONFLICT"):
        confirm(repo, session, first)
    lease = lease_for(repo, "FINALIZE")
    assert publish(repo, results[0], lease, data=b"abcd")["state"] == "COMPLETED"
    assert repo.complete_upload(session["upload_id"])["state"] == "COMPLETED"
    assert counters(repo) == (0, 8)


@pytest.mark.parametrize("data", [b"", b"abc"])
def test_finalize_publication_and_competing_replay(foundation, data):
    repo = foundation
    session, lease = finalizing(repo, data)
    results = parallel(
        [
            lambda: publish(repo, session, lease, data=data),
            lambda: publish(replica(repo), session, lease, data=data),
        ]
    )
    assert sum(isinstance(result, dict) for result in results) == 1
    assert results.count("UPLOAD_STATE_CONFLICT") == 1
    file = sql(repo, "SELECT * FROM files")[0]
    assert (
        file["state"] == "AVAILABLE" and file["actual_sha256"] == hashlib.sha256(data).hexdigest()
    )
    assert file["available_at"] is not None
    assert counters(repo) == (0, 6 if data else 0)
    assert sql(repo, "SELECT state FROM jobs")[0]["state"] == "DONE"
    with pytest.raises(DomainError, match="UPLOAD_STATE_CONFLICT"):
        repo.abort(session["upload_id"])


@pytest.mark.parametrize("data", [b"xyz", b"ab"])
def test_finalize_mismatch_is_terminal_and_preserves_cleanup_charge(foundation, data):
    repo = foundation
    session, lease = finalizing(repo)
    result = publish(repo, session, lease, data=data)
    assert result["state"] == "FAILED" and result["error_code"] == "CHECKSUM_MISMATCH"
    assert sql(repo, "SELECT state,available_at FROM files") == [
        {"state": "FAILED", "available_at": None}
    ]
    assert counters(repo) == (0, 6)
    assert len(sql(repo, "SELECT * FROM jobs WHERE kind='CLEANUP'")) == 1
    with pytest.raises(DomainError):
        publish(repo, session, lease, data=data)
    assert repo.abort(session["upload_id"]) == "FAILED"
    assert counters(repo) == (0, 6)


@pytest.mark.parametrize("change", ["owner", "token", "expiry", "target", "kind", "payload"])
def test_stale_finalize_job_rolls_back_failure_cleanup_and_admission(foundation, change):
    repo = foundation
    session, lease = finalizing(repo)
    if change == "owner":
        lease = replace(lease, owner=uuid4())
    elif change == "token":
        lease = replace(lease, token=lease.token + 1)
    elif change == "expiry":
        sql(repo, "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second'")
    elif change == "target":
        sql(repo, "UPDATE jobs SET target_id=:id", id=uuid4())
    elif change == "kind":
        sql(repo, "UPDATE jobs SET kind='RECONCILE'")
    else:
        sql(repo, "UPDATE jobs SET payload='{}'::jsonb")
    with pytest.raises(DomainError, match="STALE_JOB"):
        publish(repo, session, lease, data=b"xyz")
    assert repo.get_session(session["upload_id"])["state"] == "FINALIZING"
    assert sql(repo, "SELECT state,actual_sha256 FROM files") == [
        {"state": "VERIFYING", "actual_sha256": None}
    ]
    assert not sql(repo, "SELECT * FROM jobs WHERE kind='CLEANUP'")
    assert counters(repo) == (1, 6)


@pytest.mark.parametrize("change", ["health", "incarnation", "corrupt", "manifest", "revision"])
def test_finalize_rechecks_replica_manifest_and_revision(foundation, change):
    repo = foundation
    session, lease = finalizing(repo)
    if change == "health":
        sql(repo, "UPDATE storage_nodes SET health_state='DOWN'")
    elif change == "incarnation":
        sql(repo, "UPDATE node_incarnations SET state='QUARANTINED'")
    elif change == "corrupt":
        sql(repo, "UPDATE allocations SET replica_health='CORRUPT'")
    elif change == "manifest":
        sql(repo, "UPDATE manifest_parts SET checksum_sha256=:hash", hash="0" * 64)
    else:
        session = dict(session, revision=session["revision"] + 1)
    with pytest.raises(DomainError):
        publish(repo, session, lease)
    assert sql(repo, "SELECT state FROM files")[0]["state"] == "VERIFYING"
    assert counters(repo) == (1, 6)


@pytest.mark.parametrize("operation", ["confirm", "complete", "publish"])
def test_abort_races_never_resurrect_terminal_upload(foundation, operation):
    repo = foundation
    nodes(repo)
    session = upload(repo)
    part = claim(repo, session)
    if operation == "confirm":

        def action():
            return confirm(replica(repo), session, part)
    else:
        confirm(repo, session, part)
        if operation == "complete":

            def action():
                return replica(repo).complete_upload(session["upload_id"])
        else:
            session = repo.complete_upload(session["upload_id"])
            lease = lease_for(repo, "FINALIZE")

            def action():
                return publish(replica(repo), session, lease)

    parallel([action, lambda: repo.abort(session["upload_id"])])
    state = repo.get_session(session["upload_id"])["state"]
    file = sql(repo, "SELECT state FROM files")[0]["state"]
    assert (state, file) in [("COMPLETED", "AVAILABLE"), ("ABORTED", "FAILED")]
    assert counters(repo) == (0, 6)
    if state == "ABORTED":
        with pytest.raises(DomainError):
            confirm(repo, session, part)


def test_retry_requires_committed_fence_intent_and_drain_ack(foundation):
    repo = foundation
    nodes(repo)
    session = upload(repo)
    old = claim(repo, session)
    repo.uncertain(session["upload_id"], 0, old["claim_token"], old["revision"])
    lease = lease_for(repo, "RECONCILE")
    with pytest.raises(DomainError, match="INVALID_ALLOCATION_ACK"):
        repo.retry_part(
            session["upload_id"],
            0,
            old["claim_token"],
            old["revision"],
            proofs(old, "PREPARED"),
            lease,
        )
    intent = repo.authorize_part_retry(session["upload_id"], 0, old["revision"], uuid4(), lease)
    assert {d["allocation_id"] for d in intent["destinations"]} == {
        d["allocation_id"] for d in old["destinations"]
    }
    assert {d["generation"] for d in intent["destinations"]} == {2}
    assert counters(repo) == (1, 6)
    assert repo.get_session(session["upload_id"])["active_claims"] == 0
    with pytest.raises(DomainError, match="INVALID_ALLOCATION_ACK"):
        repo.retry_part(
            session["upload_id"],
            0,
            intent["claim_token"],
            intent["revision"],
            proofs(old, "PREPARED"),
            lease,
        )
    retried = repo.retry_part(
        session["upload_id"],
        0,
        intent["claim_token"],
        intent["revision"],
        proofs(intent, "PREPARED"),
        lease,
    )
    assert retried["state"] == "CLAIMED" and retried["claim_held"]
    assert len(sql(repo, "SELECT * FROM write_attempts WHERE generation=1 AND state='CLOSED'")) == 2
    with pytest.raises(DomainError, match="STALE_CLAIM"):
        confirm(repo, session, old)
    assert confirm(repo, session, retried)["state"] == "VERIFIED"
    assert repo.complete_upload(session["upload_id"])["state"] == "FINALIZING"
    assert counters(repo) == (1, 6)


@pytest.mark.parametrize("stage", ["intent", "retry", "reconcile"])
def test_stale_reconcile_worker_cannot_publish_any_domain_effect(foundation, stage):
    repo = foundation
    nodes(repo)
    session = upload(repo)
    part = claim(repo, session)
    repo.uncertain(session["upload_id"], 0, part["claim_token"], part["revision"])
    lease = lease_for(repo, "RECONCILE")
    if stage == "retry":
        part = repo.authorize_part_retry(session["upload_id"], 0, part["revision"], uuid4(), lease)
    before = sql(repo, "SELECT * FROM allocations ORDER BY allocation_id")
    attempts = sql(repo, "SELECT * FROM write_attempts ORDER BY attempt_id")
    stale = replace(lease, token=lease.token + 1)
    with pytest.raises(DomainError, match="STALE_JOB"):
        if stage == "intent":
            repo.authorize_part_retry(session["upload_id"], 0, part["revision"], uuid4(), stale)
        elif stage == "retry":
            repo.retry_part(
                session["upload_id"],
                0,
                part["claim_token"],
                part["revision"],
                proofs(part, "PREPARED"),
                stale,
            )
        else:
            repo.reconcile_part(session["upload_id"], 0, part["revision"], proofs(part), stale)
    assert before == sql(repo, "SELECT * FROM allocations ORDER BY allocation_id")
    assert attempts == sql(repo, "SELECT * FROM write_attempts ORDER BY attempt_id")
    assert counters(repo) == (1, 6)
    assert repo.get_session(session["upload_id"])["active_claims"] == 0


def test_lost_storage_ack_reconciles_real_bytes_then_finalizes(foundation, tmp_path):
    from contextlib import ExitStack

    from storage.v2.ledger import Ledger, Scope, Spec
    from storage.v2.streaming import StreamingStore

    repo = foundation
    nodes(repo)
    session = upload(repo)
    part = claim(repo, session)
    with ExitStack() as stack:
        ledgers = []
        for d in part["destinations"]:
            scope = Scope(
                str(repo.namespace_id), str(repo.epoch), d["node_id"], str(d["incarnation"])
            )
            ledger = Ledger(tmp_path / d["node_id"], scope, provision=True)
            stack.callback(ledger.close)
            ledgers.append(ledger)
            store = StreamingStore(ledger)
            spec = Spec(str(part["chunk_id"]), 3, part["checksum_sha256"])
            store.prepare(scope, str(d["allocation_id"]), 1, spec)
            store.store(scope, str(d["allocation_id"]), 1, spec, [(0, b"abc")])
        sql(repo, "UPDATE upload_parts SET claim_expires_at=clock_timestamp()-interval '1 second'")
        assert repo.sweep_claims() == 1
        acks = []
        for ledger, proof in zip(ledgers, proofs(part)):
            row = ledger.inspect(ledger.scope, str(proof.allocation_id), 1)
            acks.append(
                replace(
                    proof,
                    state=row["state"],
                    actual_size=row["actual_size"],
                    actual_hash=row["actual_hash"],
                )
            )
            assert ledger.verify_final(row)
        result = repo.reconcile_part(
            session["upload_id"], 0, part["revision"], acks, lease_for(repo, "RECONCILE")
        )
        assert result["state"] == "VERIFIED"
        with pytest.raises(DomainError, match="STALE_CLAIM"):
            confirm(repo, session, part)
        session = repo.complete_upload(session["upload_id"])
        data = (ledgers[0].root / "chunks" / str(acks[0].allocation_id)).read_bytes()
        assert (
            publish(repo, session, lease_for(repo, "FINALIZE"), data=data)["state"] == "COMPLETED"
        )
        assert counters(repo) == (0, 6)


def test_retry_intent_race_and_second_uncertainty_need_new_fence(foundation):
    repo = foundation
    nodes(repo)
    session = upload(repo)
    part = claim(repo, session)
    repo.uncertain(session["upload_id"], 0, part["claim_token"], part["revision"])
    lease = lease_for(repo, "RECONCILE")
    results = parallel(
        [
            lambda: repo.authorize_part_retry(session["upload_id"], 0, 1, uuid4(), lease),
            lambda: replica(repo).authorize_part_retry(session["upload_id"], 0, 1, uuid4(), lease),
        ]
    )
    assert results.count("STALE_CLAIM") == 1
    intent = next(r for r in results if isinstance(r, dict))
    assert len(sql(repo, "SELECT * FROM write_attempts")) == 4
    retried = repo.retry_part(
        session["upload_id"],
        0,
        intent["claim_token"],
        intent["revision"],
        proofs(intent, "PREPARED"),
        lease,
    )
    repo.uncertain(session["upload_id"], 0, retried["claim_token"], retried["revision"])
    lease = lease_for(repo, "RECONCILE")
    with pytest.raises(DomainError, match="INVALID_ALLOCATION_ACK"):
        repo.retry_part(
            session["upload_id"],
            0,
            retried["claim_token"],
            retried["revision"],
            proofs(retried, "PREPARED"),
            lease,
        )
    intent = repo.authorize_part_retry(session["upload_id"], 0, retried["revision"], uuid4(), lease)
    assert {d["generation"] for d in intent["destinations"]} == {3}
    assert counters(repo) == (1, 6)


@pytest.mark.parametrize("blocker", ["claim_expiry", "session_expiry", "slots"])
def test_retry_guard_does_not_revive_lease_or_overrun_slots(foundation, blocker):
    repo = foundation
    nodes(repo)
    session = upload(repo, b"abcdefg")
    part = claim(repo, session)
    repo.uncertain(session["upload_id"], 0, part["claim_token"], part["revision"])
    lease = lease_for(repo, "RECONCILE")
    intent = repo.authorize_part_retry(session["upload_id"], 0, part["revision"], uuid4(), lease)
    if blocker == "claim_expiry":
        sql(repo, "UPDATE upload_parts SET claim_expires_at=clock_timestamp()-interval '1 second'")
    elif blocker == "session_expiry":
        sql(repo, "UPDATE upload_sessions SET expires_at=clock_timestamp()-interval '1 second'")
    else:
        claim(repo, session, index=1, data=b"def")
        claim(repo, session, index=2, data=b"g")
    before = sql(repo, "SELECT * FROM allocations ORDER BY allocation_id")
    with pytest.raises(DomainError):
        repo.retry_part(
            session["upload_id"],
            0,
            intent["claim_token"],
            intent["revision"],
            proofs(intent, "PREPARED"),
            lease,
        )
    assert before == sql(repo, "SELECT * FROM allocations ORDER BY allocation_id")
    assert repo.get_session(session["upload_id"])["active_claims"] == (
        2 if blocker == "slots" else 0
    )


@pytest.mark.parametrize("change", ["namespace", "epoch", "mode"])
def test_c1_recovery_gate_blocks_every_new_entry_point(foundation, change):
    repo = foundation
    nodes(repo)
    session = upload(repo)
    part = claim(repo, session)
    repo.uncertain(session["upload_id"], 0, part["claim_token"], part["revision"])
    lease = lease_for(repo, "RECONCILE")
    if change == "mode":
        sql(repo, "UPDATE cluster_control SET mode='RECOVERING'")
    else:
        column = "namespace_id" if change == "namespace" else "recovery_epoch"
        sql(repo, f"UPDATE cluster_control SET {column}=:id", id=uuid4())
    before = sql(repo, "SELECT * FROM allocations ORDER BY allocation_id")
    for action in [
        lambda: repo.renew_part(session["upload_id"], 0, part["claim_token"], 1),
        lambda: confirm(repo, session, part),
        lambda: repo.authorize_part_retry(session["upload_id"], 0, 1, uuid4(), lease),
        lambda: repo.retry_part(
            session["upload_id"], 0, part["claim_token"], 1, proofs(part), lease
        ),
        lambda: repo.reconcile_part(session["upload_id"], 0, 1, proofs(part), lease),
        lambda: repo.complete_upload(session["upload_id"]),
        lambda: repo.publish_finalize(session["upload_id"], 1, 3, part["checksum_sha256"], lease),
    ]:
        with pytest.raises(DomainError, match="RECOVERY_REQUIRED"):
            action()
    assert before == sql(repo, "SELECT * FROM allocations ORDER BY allocation_id")
    assert counters(repo) == (1, 6)


@pytest.mark.parametrize("change", ["health", "domains", "unknown_allocation"])
def test_confirm_and_complete_enforce_current_replica_policy(foundation, change):
    repo = foundation
    nodes(repo)
    session = upload(repo)
    part = claim(repo, session)
    if change == "unknown_allocation":
        confirm(repo, session, part)
        sql(repo, "UPDATE allocations SET state='UNKNOWN'")
        with pytest.raises(DomainError, match="OUTCOME_PENDING"):
            repo.complete_upload(session["upload_id"])
        assert not sql(repo, "SELECT * FROM manifest_parts")
    else:
        if change == "health":
            sql(repo, "UPDATE storage_nodes SET health_state='DOWN'")
        else:
            sql(repo, "UPDATE allocations SET failure_domain_snapshot='same-domain'")
        with pytest.raises(DomainError, match="STORAGE_UNAVAILABLE"):
            confirm(repo, session, part)
        assert not sql(repo, "SELECT * FROM allocations WHERE state='STORED'")
        assert repo.get_session(session["upload_id"])["active_claims"] == 1
    assert counters(repo) == (1, 6)


def test_finalize_job_expiry_during_publication_rolls_back_success(foundation, monkeypatch):
    from metadata.v2.repository import execute

    repo = foundation
    session, lease = finalizing(repo)
    original = repo._manifest_matches

    def expire_after_validation(db, session, file):
        original(db, session, file)
        execute(db, "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second'")

    monkeypatch.setattr(repo, "_manifest_matches", expire_after_validation)
    with pytest.raises(DomainError, match="STALE_JOB"):
        publish(repo, session, lease)
    assert sql(repo, "SELECT state FROM files")[0]["state"] == "VERIFYING"
    assert repo.get_session(session["upload_id"])["state"] == "FINALIZING"
    assert counters(repo) == (1, 6)


@pytest.mark.parametrize("kind", ["RECONCILE", "CLEANUP"])
def test_retry_and_cleanup_recheck_job_lease_after_waiting_for_job_lock(
    foundation, monkeypatch, kind
):
    from concurrent.futures import ThreadPoolExecutor

    from sqlalchemy import text

    repo = foundation
    nodes(repo)
    session = upload(repo)
    part = claim(repo, session)
    if kind == "RECONCILE":
        repo.uncertain(session["upload_id"], 0, part["claim_token"], part["revision"])
    else:
        repo.abort(session["upload_id"])
    lease = lease_for(repo, kind)
    if kind == "CLEANUP":
        repo.authorize_cleanup(session["upload_id"], lease)
    reached = Event()
    original = repo._validate_job

    def notify(db, lease, kind, target):
        reached.set()
        return original(db, lease, kind, target)

    monkeypatch.setattr(Repository, "_validate_job", staticmethod(notify))
    before = sql(repo, "SELECT * FROM allocations ORDER BY allocation_id")
    sql(repo, "UPDATE jobs SET lease_expires_at=clock_timestamp()+interval '1 second'")
    with ThreadPoolExecutor(1) as executor:
        with repo.engine.begin() as db:
            db.execute(text(f'SET LOCAL search_path TO "{repo.schema}"'))
            db.execute(text("SELECT * FROM jobs FOR UPDATE"))
            if kind == "RECONCILE":
                future = executor.submit(
                    repo.authorize_part_retry,
                    session["upload_id"],
                    0,
                    part["revision"],
                    uuid4(),
                    lease,
                )
            else:
                d = part["destinations"][0]
                proof = DeleteEvidence(
                    repo.namespace_id,
                    repo.epoch,
                    d["node_id"],
                    d["incarnation"],
                    d["allocation_id"],
                    d["generation"],
                    "DELETED",
                    True,
                )
                future = executor.submit(repo.release_allocation, proof, lease)
            assert reached.wait(timeout=5)
            db.execute(text("SELECT pg_sleep(1.1)"))
        with pytest.raises(DomainError, match="STALE_JOB"):
            future.result(timeout=5)
    assert before == sql(repo, "SELECT * FROM allocations ORDER BY allocation_id")
    assert len(sql(repo, "SELECT * FROM write_attempts")) == 2
    assert counters(repo) == (1 if kind == "RECONCILE" else 0, 6)
