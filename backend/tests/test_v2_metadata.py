"""Focused B3 PostgreSQL tests; every fixture owns a unique disposable schema."""

import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from metadata.v2.migrate import migrate
from metadata.v2.repository import DeleteEvidence, DomainError, JobLease, Policy, Repository


@pytest.fixture
def foundation():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.fail("PostgreSQL required: use compose.v2-metadata-tests.yml")
    engine = create_engine(url, pool_size=8, connect_args={"connect_timeout": 5})
    schema = "test_v2_" + uuid4().hex
    namespace, epoch = uuid4(), uuid4()
    with engine.begin() as db:
        db.execute(text(f'CREATE SCHEMA "{schema}"'))
    try:
        assert migrate(engine, namespace, epoch, schema=schema, session_limit=8)
        repo = Repository(
            engine, namespace, epoch, schema=schema, policy=Policy(chunk_size_bytes=3)
        )
        yield repo
    finally:
        with engine.begin() as db:
            db.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        engine.dispose()


def sql(repo, statement, **params):
    with repo.engine.begin() as db:
        db.execute(text(f'SET LOCAL search_path TO "{repo.schema}"'))
        result = db.execute(text(statement), params)
        return result.mappings().all() if result.returns_rows else result.rowcount


def create(repo, *, key=None, size=3, name="sample.bin"):
    return repo.create_upload(
        key or uuid4(), original_name=name, size_bytes=size, expected_sha256="a" * 64
    )[0]


def nodes(repo, budget=30):
    result = []
    for index in range(2):
        node_id, incarnation = f"node-{index}", uuid4()
        repo.provision_node(
            node_id,
            incarnation,
            host=f"storage-{index}",
            port=50061,
            failure_domain=f"domain-{index}",
            budget=budget,
        )
        repo.activate_node(
            node_id, incarnation, observed_namespace=repo.namespace_id, observed_epoch=repo.epoch
        )
        result.append((node_id, incarnation))
    return result


def claim(repo, upload):
    return repo.claim_part(upload["upload_id"], 0, 3, "b" * 64, uuid4())


def parallel(functions):
    barrier = Barrier(len(functions))

    def run(function):
        barrier.wait(timeout=5)
        try:
            return function()
        except DomainError as exc:
            return exc.code

    with ThreadPoolExecutor(len(functions)) as executor:
        futures = [executor.submit(run, function) for function in functions]
        return [future.result(timeout=15) for future in futures]


def test_migration_replay_schema_and_scope(foundation):
    repo = foundation
    assert not migrate(repo.engine, repo.namespace_id, repo.epoch, schema=repo.schema)
    tables = sql(
        repo, "SELECT tablename FROM pg_tables WHERE schemaname=:schema", schema=repo.schema
    )
    assert len(tables) == 16  # fifteen domain tables plus migration version
    assert {row["tablename"] for row in tables} >= {
        "download_leases",
        "manifest_parts",
        "jobs",
        "node_health_polls",
    }
    with pytest.raises(ValueError, match="mismatch"):
        migrate(repo.engine, uuid4(), repo.epoch, schema=repo.schema)
    assert repo.ready()


def test_migration_refuses_existing_tables_without_mutation(foundation):
    repo = foundation
    owned = "unrelated_" + uuid4().hex
    with repo.engine.begin() as db:
        db.execute(text(f'CREATE SCHEMA "{owned}"'))
        db.execute(text(f'CREATE TABLE "{owned}".v1_fixture (value integer)'))
    try:
        with pytest.raises(ValueError, match="dedicated"):
            migrate(repo.engine, repo.namespace_id, repo.epoch, schema=owned)
        with repo.engine.connect() as db:
            assert db.execute(
                text("SELECT tablename FROM pg_tables WHERE schemaname=:schema"), {"schema": owned}
            ).scalars().all() == ["v1_fixture"]
    finally:
        with repo.engine.begin() as db:
            db.execute(text(f'DROP SCHEMA "{owned}" CASCADE'))


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE admission_counters SET held_count=-1",
        "UPDATE admission_counters SET held_count=limit_count+1",
        "UPDATE cluster_control SET mode='INVALID'",
        "UPDATE upload_sessions SET active_claims=max_parallel_parts+1",
        "UPDATE files SET state='AVAILABLE'",
        "UPDATE files SET expected_sha256='invalid'",
        "UPDATE upload_sessions SET file_id='00000000-0000-0000-0000-000000000000'",
    ],
)
def test_constraints_are_enforced_in_postgres(foundation, statement):
    create(foundation)
    with pytest.raises(IntegrityError):
        sql(foundation, statement)
    assert foundation.ready()


def test_create_idempotency_race_admission_and_policy_snapshot(foundation):
    repo = foundation
    key = uuid4()
    results = parallel([lambda: create(repo, key=key), lambda: create(repo, key=key)])
    assert results[0]["upload_id"] == results[1]["upload_id"]
    assert sql(repo, "SELECT count(*) AS n FROM files")[0]["n"] == 1
    assert sql(repo, "SELECT held_count FROM admission_counters")[0]["held_count"] == 1
    sql(repo, "UPDATE admission_counters SET limit_count=held_count")
    repo.policy = Policy(chunk_size_bytes=1, max_file_bytes=1)
    assert create(repo, key=key)["chunk_size_bytes"] == 3
    with pytest.raises(DomainError, match="IDEMPOTENCY_CONFLICT"):
        create(repo, key=key, name="different.bin")


def test_admission_race_never_overruns_and_abort_releases_once(foundation):
    repo = foundation
    sql(repo, "UPDATE admission_counters SET limit_count=1")
    results = parallel([lambda: create(repo), lambda: create(repo)])
    assert results.count("RESOURCE_LIMIT") == 1
    upload = next(value for value in results if isinstance(value, dict))
    assert repo.abort(upload["upload_id"]) == "ABORTED"
    assert repo.abort(upload["upload_id"]) == "ABORTED"
    assert sql(repo, "SELECT held_count FROM admission_counters")[0]["held_count"] == 0
    assert len(sql(repo, "SELECT * FROM jobs WHERE kind='CLEANUP'")) == 1
    assert create(repo)


def test_empty_session_and_part_shape(foundation):
    repo = foundation
    empty = create(repo, size=0)
    assert empty["part_count"] == 0
    with pytest.raises(ValueError):
        repo.claim_part(empty["upload_id"], 0, 1, "b" * 64, uuid4())
    last = create(repo, size=4)
    assert last["part_count"] == 2
    with pytest.raises(DomainError, match="PART_SIZE_MISMATCH"):
        repo.claim_part(last["upload_id"], 1, 3, "b" * 64, uuid4())
    assert not sql(repo, "SELECT * FROM upload_parts")


def test_claim_same_part_race_has_one_owner_and_durable_attempts(foundation):
    repo = foundation
    nodes(repo)
    upload = create(repo)
    results = parallel([lambda: claim(repo, upload), lambda: claim(repo, upload)])
    assert results.count("PART_BUSY") == 1
    part = next(value for value in results if isinstance(value, dict))
    assert len(part["destinations"]) == 2
    assert len({d["allocation_id"] for d in part["destinations"]}) == 2
    assert sql(repo, "SELECT charged_bytes FROM node_budgets ORDER BY node_id") == [
        {"charged_bytes": 3},
        {"charged_bytes": 3},
    ]
    assert len(sql(repo, "SELECT * FROM write_attempts WHERE state='INTENDED'")) == 2
    assert repo.get_session(upload["upload_id"])["active_claims"] == 1
    with pytest.raises(DomainError, match="PART_CONFLICT"):
        repo.claim_part(upload["upload_id"], 0, 3, "c" * 64, uuid4())


def test_two_sessions_capacity_race_and_reservation_rollback(foundation):
    repo = foundation
    nodes(repo, budget=3)
    a, b = create(repo), create(repo)
    results = parallel([lambda: claim(repo, a), lambda: claim(repo, b)])
    assert sum(isinstance(value, dict) for value in results) == 1
    assert results.count("INSUFFICIENT_DOMAINS") == 1
    assert len(sql(repo, "SELECT * FROM upload_parts")) == 1
    assert len(sql(repo, "SELECT * FROM allocations")) == 2
    assert sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 6


def test_strict_domains_and_joining_node_cannot_place(foundation):
    repo = foundation
    registry = nodes(repo)
    sql(repo, "UPDATE storage_nodes SET failure_domain='same-domain'")
    with pytest.raises(DomainError, match="INSUFFICIENT_DOMAINS"):
        claim(repo, create(repo))
    assert not sql(repo, "SELECT * FROM allocations")
    assert sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 0
    sql(
        repo,
        "UPDATE storage_nodes SET health_state='QUARANTINED' WHERE node_id=:node",
        node=registry[0][0],
    )
    with pytest.raises(DomainError):
        claim(repo, create(repo))


def test_unknown_outcome_sweeper_dedup_and_charge_retention(foundation):
    repo = foundation
    nodes(repo)
    upload = create(repo)
    part = claim(repo, upload)
    sql(repo, "UPDATE upload_parts SET claim_expires_at=clock_timestamp()-interval '1 second'")
    results = parallel([lambda: repo.sweep_claims(), lambda: repo.sweep_claims()])
    assert sum(results) == 1
    repo.uncertain(upload["upload_id"], 0, part["claim_token"], 1)
    assert repo.get_session(upload["upload_id"])["active_claims"] == 0
    assert len(sql(repo, "SELECT * FROM jobs WHERE kind='RECONCILE'")) == 1
    assert len(sql(repo, "SELECT * FROM allocations WHERE state='UNKNOWN'")) == 2
    assert len(sql(repo, "SELECT * FROM write_attempts WHERE state='UNKNOWN'")) == 2
    assert sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 6
    with pytest.raises(DomainError, match="OUTCOME_PENDING"):
        claim(repo, upload)


def evidence(repo, destination):
    return DeleteEvidence(
        repo.namespace_id,
        repo.epoch,
        destination["node_id"],
        destination["incarnation"],
        destination["allocation_id"],
        1,
        "DELETED",
        True,
    )


def cleanup_lease(repo):
    job = next(job for job in repo.claim_jobs(uuid4()) if job["kind"] == "CLEANUP")
    return JobLease(job["job_id"], job["lease_owner"], job["fencing_token"])


def test_cleanup_requires_durable_exact_ack_and_releases_once(foundation):
    repo = foundation
    nodes(repo)
    upload = create(repo)
    part = claim(repo, upload)
    proof = evidence(repo, part["destinations"][0])
    repo.abort(upload["upload_id"])
    lease = cleanup_lease(repo)
    with pytest.raises(DomainError, match="CLEANUP_NOT_AUTHORIZED"):
        repo.release_allocation(proof, lease)
    repo.authorize_cleanup(upload["upload_id"], lease)
    for wrong in [
        replace(proof, state="NOT_FOUND"),
        replace(proof, cleanup_complete=False),
        replace(proof, incarnation=uuid4()),
        replace(proof, generation=2),
        replace(proof, recovery_epoch=uuid4()),
    ]:
        with pytest.raises(DomainError, match="INVALID_DELETE_ACK"):
            repo.release_allocation(wrong, lease)
    assert sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 6
    results = parallel(
        [
            lambda: repo.release_allocation(proof, lease),
            lambda: repo.release_allocation(proof, lease),
        ]
    )
    assert sorted(results) == [False, True]
    assert sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 3
    assert len(sql(repo, "SELECT * FROM allocations WHERE state='DELETE_PENDING'")) == 1
    repo.release_allocation(evidence(repo, part["destinations"][1]), lease)
    assert sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 0
    assert all(
        row["charged_bytes"] == 3 for row in sql(repo, "SELECT charged_bytes FROM allocations")
    )
    assert repo.get_session(upload["upload_id"])["active_claims"] == 0


def test_new_incarnation_quarantines_without_recrediting_old_budget(foundation):
    repo = foundation
    registry = nodes(repo)
    claim(repo, create(repo))
    node_id, old_inc = registry[0]
    new_inc = uuid4()
    assert not repo.provision_node(
        node_id, new_inc, host="storage-0", port=50061, failure_domain="domain-0", budget=30
    )
    assert (
        sql(repo, "SELECT health_state FROM storage_nodes WHERE node_id=:node", node=node_id)[0][
            "health_state"
        ]
        == "QUARANTINED"
    )
    assert (
        sql(
            repo,
            "SELECT charged_bytes FROM node_budgets WHERE node_id=:node AND incarnation=:inc",
            node=node_id,
            inc=old_inc,
        )[0]["charged_bytes"]
        == 3
    )
    with pytest.raises(DomainError, match="INSUFFICIENT_DOMAINS"):
        claim(repo, create(repo))
    assert not repo.set_budget(node_id, old_inc, 2)
    assert (
        sql(
            repo,
            "SELECT allocatable_bytes FROM node_budgets WHERE node_id=:node AND incarnation=:inc",
            node=node_id,
            inc=old_inc,
        )[0]["allocatable_bytes"]
        == 30
    )


def test_job_claim_race_expiry_fencing_renew_and_retry(foundation):
    repo = foundation
    repo.abort(create(repo)["upload_id"])
    a, b = uuid4(), uuid4()
    results = parallel([lambda: repo.claim_jobs(a), lambda: repo.claim_jobs(b)])
    assert sorted(map(len, results)) == [0, 1]
    first = next(rows[0] for rows in results if rows)
    repo.renew_job(first["job_id"], first["lease_owner"], first["fencing_token"])
    sql(repo, "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second'")
    second = repo.claim_jobs(uuid4())[0]
    assert second["fencing_token"] == first["fencing_token"] + 1
    with pytest.raises(DomainError, match="STALE_JOB"):
        repo.finish_job(first["job_id"], first["lease_owner"], first["fencing_token"])
    repo.finish_job(second["job_id"], second["lease_owner"], second["fencing_token"], retry=True)
    assert not repo.claim_jobs(uuid4())
    sql(repo, "UPDATE jobs SET run_after=clock_timestamp()-interval '1 second'")
    third = repo.claim_jobs(uuid4())[0]
    repo.finish_job(third["job_id"], third["lease_owner"], third["fencing_token"])
    assert sql(repo, "SELECT state FROM jobs")[0]["state"] == "DONE"


@pytest.mark.parametrize("change", ["mode", "epoch", "namespace"])
def test_recovery_gate_blocks_all_repository_entry_points(foundation, change):
    repo = foundation
    upload = create(repo)
    if change == "mode":
        sql(repo, "UPDATE cluster_control SET mode='RECOVERING'")
    elif change == "epoch":
        sql(repo, "UPDATE cluster_control SET recovery_epoch=:id", id=uuid4())
    else:
        sql(repo, "UPDATE cluster_control SET namespace_id=:id", id=uuid4())
    for action in [
        repo.ready,
        lambda: create(repo),
        lambda: repo.get_session(upload["upload_id"]),
        lambda: repo.abort(upload["upload_id"]),
        lambda: repo.claim_jobs(uuid4()),
        lambda: repo.sweep_claims(),
    ]:
        with pytest.raises(DomainError, match="RECOVERY_REQUIRED"):
            action()
    assert sql(repo, "SELECT state FROM upload_sessions")[0]["state"] == "OPEN"


def test_expiry_race_keeps_charge_and_releases_admission_once(foundation):
    repo = foundation
    nodes(repo)
    upload = create(repo)
    claim(repo, upload)
    other = create(repo)
    sql(
        repo,
        "UPDATE upload_sessions SET expires_at=clock_timestamp()-interval '1 second' "
        "WHERE upload_id=:upload",
        upload=upload["upload_id"],
    )
    assert sum(parallel([lambda: repo.expire_sessions(), lambda: repo.expire_sessions()])) == 1
    assert repo.get_session(upload["upload_id"])["state"] == "EXPIRED"
    assert repo.get_session(other["upload_id"])["state"] == "OPEN"
    assert sql(repo, "SELECT held_count FROM admission_counters")[0]["held_count"] == 1
    assert sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 6
    assert len(sql(repo, "SELECT * FROM jobs WHERE kind='CLEANUP'")) == 1


def test_storage_unknown_outcomes_keep_charge_until_real_delete_ack(foundation, tmp_path):
    import hashlib

    from storage.v2.ledger import Ledger, Scope, Spec
    from storage.v2.streaming import StreamingStore

    repo = foundation
    nodes(repo)
    upload = create(repo)
    chunk_hash = hashlib.sha256(b"abc").hexdigest()
    part = repo.claim_part(upload["upload_id"], 0, 3, chunk_hash, uuid4())
    ledgers = []
    stores = []
    try:
        for index, destination in enumerate(part["destinations"]):
            scope = Scope(
                str(repo.namespace_id),
                str(repo.epoch),
                destination["node_id"],
                str(destination["incarnation"]),
            )
            ledger = Ledger(tmp_path / destination["node_id"], scope, provision=True)
            ledgers.append(ledger)
            store = StreamingStore(ledger)
            stores.append((store, scope))
            aid = str(destination["allocation_id"])
            spec = Spec(str(part["chunk_id"]), 3, chunk_hash)
            store.prepare(scope, aid, 1, spec)
            if index == 0:
                # Storage committed, but the Transfer never submits confirmation.
                assert store.store(scope, aid, 1, spec, [(0, b"abc")])[0]["state"] == "STORED"
            else:

                def broken():
                    yield 0, b"a"
                    raise ConnectionError("wire dropped before complete")

                with pytest.raises(ConnectionError):
                    store.store(scope, aid, 1, spec, broken())
                assert ledger.lookup(aid)["state"] == "PREPARED"
        repo.uncertain(upload["upload_id"], 0, part["claim_token"], part["revision"])
        assert sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 6
        repo.abort(upload["upload_id"])
        lease = cleanup_lease(repo)
        repo.authorize_cleanup(upload["upload_id"], lease)
        for index, (destination, (store, scope)) in enumerate(zip(part["destinations"], stores)):
            aid = str(destination["allocation_id"])
            deleted = store.delete(scope, aid, 1)
            assert deleted["state"] == "DELETED" and deleted["cleanup_complete"]
            # Lost Delete ACK cannot itself free metadata capacity.
            assert (
                sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"]
                == 6 - 3 * index
            )
            deleted = store.delete(scope, aid, 1)
            proof = evidence(repo, destination)
            assert repo.release_allocation(proof, lease)
            assert not repo.release_allocation(proof, lease)
            assert not (store.ledger.root / "chunks" / aid).exists()
            assert not list((store.ledger.root / "tmp").iterdir())
        assert sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 0
    finally:
        for ledger in ledgers:
            ledger.close()


def test_cleanup_authorization_pages_and_retries_do_not_release_charge(foundation):
    repo = foundation
    repo.policy = Policy(chunk_size_bytes=3, max_parallel_parts=4)
    nodes(repo)
    upload = create(repo, size=9)
    for index in range(3):
        repo.claim_part(upload["upload_id"], index, 3, "b" * 64, uuid4())
    repo.abort(upload["upload_id"])
    lease = cleanup_lease(repo)
    first = repo.authorize_cleanup(upload["upload_id"], lease, limit=1)
    assert len(first["allocations"]) == 2 and first["has_more"]
    assert first["next_after_index"] == 0
    replay = repo.authorize_cleanup(upload["upload_id"], lease, limit=1)
    assert {row["allocation_id"] for row in replay["allocations"]} == {
        row["allocation_id"] for row in first["allocations"]
    }
    second = repo.authorize_cleanup(upload["upload_id"], lease, after_index=0, limit=1)
    third = repo.authorize_cleanup(upload["upload_id"], lease, after_index=1, limit=1)
    assert second["has_more"] and not third["has_more"]
    assert len(sql(repo, "SELECT * FROM allocations WHERE state='DELETE_PENDING'")) == 6
    assert sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 18
    assert repo.get_session(upload["upload_id"])["active_claims"] == 0


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE node_budgets SET charged_bytes=allocatable_bytes+1",
        "UPDATE allocations SET state='STORED'",
        "UPDATE allocations SET state='RELEASED'",
        "UPDATE storage_nodes SET active_incarnation=gen_random_uuid()",
        "INSERT INTO node_incarnations SELECT node_id,gen_random_uuid(),recovery_epoch,"
        "'ACTIVE',registered_at,NULL FROM node_incarnations LIMIT 1",
        "INSERT INTO write_attempts SELECT gen_random_uuid(),allocation_id,generation,owner_token,"
        "state,lease_expires_at,started_at,finished_at,error_code FROM write_attempts LIMIT 1",
    ],
)
def test_allocation_and_registry_constraints(foundation, statement):
    repo = foundation
    nodes(repo)
    claim(repo, create(repo))
    with pytest.raises(IntegrityError):
        sql(repo, statement)
    assert sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 6


def test_migration_checksum_guard_and_concurrent_replay(foundation):
    repo = foundation
    assert parallel(
        [
            lambda: migrate(repo.engine, repo.namespace_id, repo.epoch, schema=repo.schema),
            lambda: migrate(repo.engine, repo.namespace_id, repo.epoch, schema=repo.schema),
        ]
    ) == [False, False]
    sql(repo, "UPDATE v2_schema_version SET checksum=:hash", hash="0" * 64)
    with pytest.raises(ValueError, match="checksum"):
        migrate(repo.engine, repo.namespace_id, repo.epoch, schema=repo.schema)
    assert repo.ready()


def test_transactions_explicitly_use_read_committed(foundation):
    repo = foundation
    configured = Repository(
        repo.engine.execution_options(isolation_level="REPEATABLE READ"),
        repo.namespace_id,
        repo.epoch,
        schema=repo.schema,
    )
    with configured.transaction() as db:
        assert db.execute(text("SHOW transaction_isolation")).scalar_one() == "read committed"


def test_stale_cleanup_job_cannot_publish_capacity_release(foundation):
    repo = foundation
    nodes(repo)
    upload = create(repo)
    part = claim(repo, upload)
    repo.abort(upload["upload_id"])
    old = cleanup_lease(repo)
    repo.authorize_cleanup(upload["upload_id"], old)
    sql(repo, "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second'")
    current = cleanup_lease(repo)
    assert current.token == old.token + 1
    proof = evidence(repo, part["destinations"][0])
    with pytest.raises(DomainError, match="STALE_JOB"):
        repo.release_allocation(proof, old)
    assert sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 6
    assert not sql(repo, "SELECT * FROM allocations WHERE state='RELEASED'")
    assert not sql(repo, "SELECT * FROM write_attempts WHERE state='CLOSED'")
    assert repo.release_allocation(proof, current)
    with pytest.raises(DomainError, match="STALE_JOB"):
        repo.authorize_cleanup(upload["upload_id"], old)


def test_sweepers_with_opposite_candidate_orders_do_not_deadlock(foundation):
    repo = foundation
    nodes(repo)
    a, b = create(repo), create(repo)
    claim(repo, a)
    claim(repo, b)
    sql(
        repo,
        """UPDATE upload_sessions SET expires_at=clock_timestamp()-
        CASE WHEN upload_id=:a THEN interval '1 second' ELSE interval '2 seconds' END""",
        a=a["upload_id"],
    )
    sql(
        repo,
        """UPDATE upload_parts SET claim_expires_at=clock_timestamp()-
        CASE WHEN upload_id=:a THEN interval '2 seconds' ELSE interval '1 second' END""",
        a=a["upload_id"],
    )
    barrier = Barrier(2)

    def worker(kind):
        instance = Repository(
            repo.engine, repo.namespace_id, repo.epoch, schema=repo.schema, policy=repo.policy
        )
        lock_session = instance._lock_session
        first = True

        def lock(db, upload_id):
            nonlocal first
            result = lock_session(db, upload_id)
            if first:
                first = False
                # Force the interleaving that previously held A and B in
                # opposite batch transactions and produced SQLSTATE 40P01.
                assert upload_id == (a if kind == "claim" else b)["upload_id"]
                barrier.wait(timeout=5)
            return result

        instance._lock_session = lock
        return instance.sweep_claims() if kind == "claim" else instance.expire_sessions()

    with ThreadPoolExecutor(2) as executor:
        futures = [executor.submit(worker, kind) for kind in ("claim", "expiry")]
        assert [future.result(timeout=12) for future in futures] == [2, 2]
    assert all(
        row["state"] == "EXPIRED" and row["active_claims"] == 0
        for row in sql(repo, "SELECT state,active_claims FROM upload_sessions")
    )
    assert sql(repo, "SELECT held_count FROM admission_counters")[0]["held_count"] == 0
    assert sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 12
    assert len(sql(repo, "SELECT * FROM jobs WHERE kind='RECONCILE'")) == 2
    assert len(sql(repo, "SELECT * FROM jobs WHERE kind='CLEANUP'")) == 2


def test_sweeper_batch_limits_preserve_unselected_owners(foundation):
    repo = foundation
    nodes(repo)
    for _ in range(2):
        claim(repo, create(repo))
    sql(repo, "UPDATE upload_parts SET claim_expires_at=clock_timestamp()-interval '1 second'")
    sql(repo, "UPDATE upload_sessions SET expires_at=clock_timestamp()-interval '1 second'")
    assert repo.sweep_claims(limit=1) == 1
    assert sum(row["claim_held"] for row in sql(repo, "SELECT claim_held FROM upload_parts")) == 1
    assert repo.expire_sessions(limit=1) == 1
    assert sql(repo, "SELECT held_count FROM admission_counters")[0]["held_count"] == 1
    assert repo.sweep_claims(limit=1) == 1
    assert repo.expire_sessions(limit=1) == 1
    assert repo.sweep_claims(limit=1) == repo.expire_sessions(limit=1) == 0
    assert sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 12
