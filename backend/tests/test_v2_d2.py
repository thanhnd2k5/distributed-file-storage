"""D2 read/delete transactions on isolated PostgreSQL schemas and real connections."""

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event
from uuid import uuid4

import pytest
from test_v2_c1 import claim, confirm, counters, finalizing, lease_for, publish, replica, upload
from test_v2_c2 import cluster as cluster
from test_v2_c2 import confirmed, due, run_job
from test_v2_metadata import foundation as foundation
from test_v2_metadata import nodes, parallel, sql

from metadata.v2.repository import DeleteEvidence, DomainError, execute
from metadata.v2.worker import Worker, WorkerConfig


def available(repo, data=b"abc"):
    session, job = finalizing(repo, data)
    publish(repo, session, job, data=data)
    return session


def expire_read(repo):
    sql(
        repo,
        "UPDATE download_leases SET created_at=clock_timestamp()-interval '2 seconds',"
        "expires_at=clock_timestamp()-interval '1 second'",
    )


def delete_proof(repo, allocation):
    return DeleteEvidence(
        repo.namespace_id,
        repo.epoch,
        allocation["node_id"],
        allocation["incarnation"],
        allocation["allocation_id"],
        allocation["generation"],
        "DELETED",
        True,
    )


def wait_for_block(repo, blocker):
    """Prove the other connection is waiting on our lock, rather than sleep and guess."""
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if sql(
            repo,
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
            "WHERE :blocker=ANY(pg_blocking_pids(pid))) AS blocked",
            blocker=blocker,
        )[0]["blocked"]:
            return
        time.sleep(0.01)
    pytest.fail("competing transaction did not wait for the expected row lock")


@pytest.mark.parametrize("data", [b"", b"abcdefghijklm"])
def test_acquire_pins_version_and_pages_immutable_manifest(foundation, data):
    repo = foundation
    session = available(repo, data)
    before = sql(repo, "SELECT * FROM manifest_parts ORDER BY part_index")
    metadata = repo.resolve_download(session["file_id"])
    assert not sql(repo, "SELECT * FROM download_leases")  # HEAD/plan has no lease.
    read = repo.acquire_read(session["file_id"], version_id=metadata["version_id"])
    assert read["version_id"] == session["version_id"]
    assert read["file"]["actual_sha256"] == session["expected_sha256"]
    assert read["file"]["size_bytes"] == len(data)
    assert read["created_at"] == read["db_now"]
    assert read["expires_at"] - read["created_at"] == timedelta(seconds=30)
    assert read["absolute_expires_at"] - read["created_at"] == timedelta(hours=24)
    items, cursor = [], -1
    while True:
        page = replica(repo).read_manifest(
            read["read_id"], read["lease_token"], after_index=cursor, limit=2
        )
        assert len(page["items"]) <= 2 and page["version_id"] == session["version_id"]
        assert page["expires_at"] == read["expires_at"]  # Paging does not renew.
        items.extend(page["items"])
        if not page["has_more"]:
            assert page["next_after_index"] is None
            break
        assert page["next_after_index"] > cursor
        cursor = page["next_after_index"]
    assert [
        {k: p[k] for k in ("version_id", "part_index", "chunk_id", "size_bytes", "checksum_sha256")}
        for p in items
    ] == before
    for part in items:
        assert len(part["destinations"]) == 2
        for d in part["destinations"]:
            assert d["namespace_id"] == repo.namespace_id and d["recovery_epoch"] == repo.epoch
            assert d["chunk_id"] == part["chunk_id"] and d["generation"] == 1
            assert (
                d["size_bytes"] == part["size_bytes"]
                and d["checksum_sha256"] == part["checksum_sha256"]
            )
            assert "host" in d and "port" in d
    assert before == sql(repo, "SELECT * FROM manifest_parts ORDER BY part_index")
    second = replica(repo).acquire_read(session["file_id"])
    assert second["read_id"] != read["read_id"] and second["lease_token"] != read["lease_token"]
    assert counters(repo) == (0, len(data) * 2)


@pytest.mark.parametrize(
    "state", ["UPLOADING", "VERIFYING", "FAILED", "DELETING", "DELETED", "absent"]
)
def test_resolve_and_acquire_recheck_file_state_without_creating_lease(foundation, state):
    repo = foundation
    session = upload(repo)
    file_id = session["file_id"]
    if state == "absent":
        file_id = uuid4()
    else:
        sql(repo, "UPDATE files SET state=:state", state=state)
    code = "FILE_NOT_FOUND" if state in ("DELETING", "DELETED", "absent") else "FILE_NOT_READY"
    for action in (repo.resolve_download, repo.acquire_read):
        with pytest.raises(DomainError, match=code):
            action(file_id)
    assert not sql(repo, "SELECT * FROM download_leases")


def test_acquire_checks_resolved_version_and_delete_between_plan_and_acquire(foundation):
    repo = foundation
    session = available(repo, b"")
    observed = repo.resolve_download(session["file_id"])
    with pytest.raises(DomainError, match="PRECONDITION_FAILED"):
        repo.acquire_read(session["file_id"], version_id=uuid4())
    replica(repo).delete_file(session["file_id"])
    with pytest.raises(DomainError, match="FILE_NOT_FOUND"):
        repo.acquire_read(session["file_id"], version_id=observed["version_id"])
    assert not sql(repo, "SELECT * FROM download_leases")


@pytest.mark.parametrize("action", ["renew_read", "read_manifest", "release_read"])
def test_read_id_and_secret_token_are_both_required(foundation, action):
    repo = foundation
    session = available(repo, b"")
    read = repo.acquire_read(session["file_id"])
    other = repo.acquire_read(session["file_id"])
    method = getattr(replica(repo), action)
    before = sql(repo, "SELECT * FROM download_leases ORDER BY read_id")
    for token in (uuid4(), other["lease_token"]):
        with pytest.raises(DomainError, match="STALE_READ_LEASE"):
            method(read["read_id"], token)
    with pytest.raises(DomainError, match="READ_LEASE_NOT_FOUND"):
        method(uuid4(), read["lease_token"])
    assert before == sql(repo, "SELECT * FROM download_leases ORDER BY read_id")


@pytest.mark.parametrize("terminal", ["expired", "absolute", "released"])
def test_renew_and_manifest_never_revive_terminal_lease(foundation, terminal):
    repo = foundation
    session = available(repo)
    read = repo.acquire_read(session["file_id"])
    if terminal == "released":
        assert repo.release_read(read["read_id"], read["lease_token"])
    else:
        expire_read(repo)
        if terminal == "absolute":
            sql(repo, "UPDATE download_leases SET absolute_expires_at=expires_at")
    replica(repo).delete_file(session["file_id"])
    before = sql(repo, "SELECT * FROM download_leases")
    for method in (repo.renew_read, repo.read_manifest):
        with pytest.raises(DomainError, match="STALE_READ_LEASE"):
            method(read["read_id"], read["lease_token"])
    assert before == sql(repo, "SELECT * FROM download_leases")
    assert repo.release_read(read["read_id"], read["lease_token"]) == (terminal != "released")
    first = sql(repo, "SELECT * FROM download_leases")
    assert not replica(repo).release_read(read["read_id"], read["lease_token"])
    assert first == sql(repo, "SELECT * FROM download_leases")
    assert repo.authorize_cleanup(session["upload_id"], lease_for(repo, "CLEANUP"))["allocations"]


def test_renew_after_tombstone_clamps_absolute_limit_and_preserves_manifest(foundation):
    repo = foundation
    session = available(repo)
    read = repo.acquire_read(session["file_id"])
    page = repo.read_manifest(read["read_id"], read["lease_token"])
    sql(
        repo,
        "UPDATE download_leases SET expires_at=clock_timestamp()+interval '2 seconds',"
        "absolute_expires_at=clock_timestamp()+interval '5 seconds'",
    )
    replica(repo).delete_file(session["file_id"])
    renewed = replica(repo).renew_read(read["read_id"], read["lease_token"])
    assert renewed["expires_at"] == renewed["absolute_expires_at"]
    assert renewed["created_at"] == read["created_at"]
    assert repo.read_manifest(read["read_id"], read["lease_token"])["items"] == page["items"]
    cleanup = lease_for(repo, "CLEANUP")
    with pytest.raises(DomainError, match="READ_LEASE_ACTIVE"):
        repo.authorize_cleanup(session["upload_id"], cleanup)
    with pytest.raises(DomainError, match="READ_LEASE_ACTIVE"):
        repo.finish_cleanup(session["upload_id"], cleanup)
    assert counters(repo) == (0, 6)


def test_renew_extends_only_ttl_and_manifest_pages_survive_delete(foundation):
    repo = foundation
    session = available(repo, b"abcdefghij")
    read = repo.acquire_read(session["file_id"])
    first = repo.read_manifest(read["read_id"], read["lease_token"], limit=1)
    sql(repo, "UPDATE download_leases SET expires_at=clock_timestamp()+interval '5 seconds'")
    replica(repo).delete_file(session["file_id"])
    before_file = sql(repo, "SELECT * FROM files")
    renewed = repo.renew_read(read["read_id"], read["lease_token"])
    assert 29 < (renewed["expires_at"] - renewed["db_now"]).total_seconds() <= 30
    assert renewed["absolute_expires_at"] == read["absolute_expires_at"]
    rest = repo.read_manifest(
        read["read_id"], read["lease_token"], after_index=first["next_after_index"], limit=1000
    )
    assert [p["part_index"] for p in first["items"] + rest["items"]] == [0, 1, 2, 3]
    assert [p["size_bytes"] for p in rest["items"]] == [3, 3, 1]
    assert not rest["has_more"]
    assert before_file == sql(repo, "SELECT * FROM files")


@pytest.mark.parametrize(
    "field,value",
    [
        ("after_index", -2),
        ("after_index", True),
        ("after_index", 1.5),
        ("after_index", 2**53),
        ("limit", 0),
        ("limit", True),
        ("limit", 1.5),
        ("limit", 1001),
    ],
)
def test_manifest_paging_rejects_invalid_bounds(foundation, field, value):
    repo = foundation
    session = available(repo, b"")
    read = repo.acquire_read(session["file_id"])
    with pytest.raises(ValueError):
        repo.read_manifest(read["read_id"], read["lease_token"], **{field: value})


@pytest.mark.parametrize(
    "change",
    ["health", "enabled", "incarnation", "epoch", "missing", "corrupt", "size", "hash", "pending"],
)
def test_manifest_keeps_spec_but_excludes_ineligible_replica(foundation, change):
    repo = foundation
    session = available(repo)
    read = repo.acquire_read(session["file_id"])
    before = sql(repo, "SELECT * FROM manifest_parts")
    changes = {
        "health": "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-0'",
        "enabled": "UPDATE storage_nodes SET enabled=false WHERE node_id='node-0'",
        "incarnation": "UPDATE node_incarnations SET state='RETIRED' WHERE node_id='node-0'",
        "epoch": "UPDATE allocations SET recovery_epoch=:epoch WHERE node_id='node-0'",
        "missing": "UPDATE allocations SET replica_health='MISSING' WHERE node_id='node-0'",
        "corrupt": "UPDATE allocations SET replica_health='CORRUPT' WHERE node_id='node-0'",
        "size": "UPDATE allocations SET ack_size=1 WHERE node_id='node-0'",
        "hash": "UPDATE allocations SET ack_hash=repeat('0',64) WHERE node_id='node-0'",
        "pending": "UPDATE allocations SET state='DELETE_PENDING' WHERE node_id='node-0'",
    }
    sql(repo, changes[change], epoch=uuid4())
    part = repo.read_manifest(read["read_id"], read["lease_token"])["items"][0]
    assert (
        part["chunk_id"] == before[0]["chunk_id"]
        and part["checksum_sha256"] == before[0]["checksum_sha256"]
    )
    assert [d["node_id"] for d in part["destinations"]] == ["node-1"]
    sql(repo, "UPDATE storage_nodes SET health_state='DOWN'")
    assert not repo.read_manifest(read["read_id"], read["lease_token"])["items"][0]["destinations"]
    assert sql(repo, "SELECT * FROM manifest_parts") == before and counters(repo) == (0, 6)


@pytest.mark.parametrize("first", ["acquire", "delete"])
def test_acquire_delete_race_serializes_on_file_lock(foundation, monkeypatch, first):
    repo = foundation
    session = available(repo, b"")
    other = replica(repo)
    locked, proceed, holder = Event(), Event(), []
    original = repo._file

    def pause(db, file_id, *, lock=False):
        result = original(db, file_id, lock=lock)
        if lock and not locked.is_set():
            holder.append(execute(db, "SELECT pg_backend_pid()").scalar_one())
            locked.set()
            assert proceed.wait(4)
        return result

    monkeypatch.setattr(repo, "_file", pause)
    actions = (
        [repo.acquire_read, other.delete_file]
        if first == "acquire"
        else [repo.delete_file, other.acquire_read]
    )
    with ThreadPoolExecutor(2) as pool:
        a = pool.submit(actions[0], session["file_id"])
        assert locked.wait(3)
        b = pool.submit(actions[1], session["file_id"])
        try:
            wait_for_block(repo, holder[0])
        finally:
            proceed.set()
        result = a.result(5)
        if first == "acquire":
            assert result["state"] == "ACTIVE" and b.result(5)["state"] == "DELETING"
            assert other.renew_read(result["read_id"], result["lease_token"])["state"] == "ACTIVE"
        else:
            assert result["state"] == "DELETING"
            with pytest.raises(DomainError, match="FILE_NOT_FOUND"):
                b.result(5)
            assert not sql(repo, "SELECT * FROM download_leases")


@pytest.mark.parametrize("locked_table", ["files", "download_leases"])
@pytest.mark.parametrize("action", ["renew_read", "read_manifest"])
def test_expiry_is_checked_after_file_and_lease_lock_wait(foundation, locked_table, action):
    repo = foundation
    session = available(repo, b"")
    read = repo.acquire_read(session["file_id"])
    sql(repo, "UPDATE download_leases SET expires_at=clock_timestamp()+interval '1 second'")
    with ThreadPoolExecutor(1) as pool:
        with repo.engine.begin() as db:
            execute(db, f'SET LOCAL search_path TO "{repo.schema}"')
            execute(db, f"SELECT * FROM {locked_table} FOR UPDATE").all()
            holder = execute(db, "SELECT pg_backend_pid()").scalar_one()
            future = pool.submit(
                getattr(replica(repo), action), read["read_id"], read["lease_token"]
            )
            wait_for_block(repo, holder)
            execute(db, "SELECT pg_sleep(1.1)")
        with pytest.raises(DomainError, match="STALE_READ_LEASE"):
            future.result(5)
    assert (
        sql(repo, "SELECT * FROM download_leases")[0]["expires_at"]
        < sql(repo, "SELECT clock_timestamp() AS now")[0]["now"]
    )


def test_acquire_ttl_starts_after_lock_wait(foundation):
    repo = foundation
    session = available(repo, b"")
    with ThreadPoolExecutor(1) as pool:
        with repo.engine.begin() as db:
            execute(db, f'SET LOCAL search_path TO "{repo.schema}"')
            execute(db, "SELECT * FROM files FOR UPDATE").all()
            holder = execute(db, "SELECT pg_backend_pid()").scalar_one()
            future = pool.submit(replica(repo).acquire_read, session["file_id"])
            wait_for_block(repo, holder)
            execute(db, "SELECT pg_sleep(0.1)")
            unlocked_after = execute(db, "SELECT clock_timestamp()").scalar_one()
        read = future.result(5)
    assert read["created_at"] >= unlocked_after
    assert read["expires_at"] - read["created_at"] == timedelta(seconds=30)


@pytest.mark.parametrize("expired", [False, True])
def test_renew_cleanup_race_cannot_authorize_delete_then_revive_reader(foundation, expired):
    repo = foundation
    session = available(repo)
    read = repo.acquire_read(session["file_id"])
    repo.delete_file(session["file_id"])
    cleanup = lease_for(repo, "CLEANUP")
    if expired:
        expire_read(repo)
    results = parallel(
        [
            lambda: repo.renew_read(read["read_id"], read["lease_token"]),
            lambda: replica(repo).authorize_cleanup(session["upload_id"], cleanup),
        ]
    )
    if expired:
        assert results[0] == "STALE_READ_LEASE" and len(results[1]["allocations"]) == 2
        assert {a["state"] for a in sql(repo, "SELECT * FROM allocations")} == {"DELETE_PENDING"}
    else:
        assert results[0]["state"] == "ACTIVE" and results[1] == "READ_LEASE_ACTIVE"
        assert {a["state"] for a in sql(repo, "SELECT * FROM allocations")} == {"STORED"}
    assert counters(repo) == (0, 6)


def test_cleanup_uses_clock_after_waiting_for_file_lock(foundation):
    repo = foundation
    session = available(repo)
    read = repo.acquire_read(session["file_id"])
    repo.delete_file(session["file_id"])
    cleanup = lease_for(repo, "CLEANUP")
    sql(repo, "UPDATE download_leases SET expires_at=clock_timestamp()+interval '1 second'")
    with ThreadPoolExecutor(1) as pool:
        with repo.engine.begin() as db:
            execute(db, f'SET LOCAL search_path TO "{repo.schema}"')
            execute(db, "SELECT * FROM files FOR UPDATE").all()
            holder = execute(db, "SELECT pg_backend_pid()").scalar_one()
            future = pool.submit(replica(repo).authorize_cleanup, session["upload_id"], cleanup)
            wait_for_block(repo, holder)
            execute(db, "SELECT pg_sleep(1.1)")
        assert len(future.result(5)["allocations"]) == 2
    with pytest.raises(DomainError, match="STALE_READ_LEASE"):
        repo.renew_read(read["read_id"], read["lease_token"])


def test_release_renew_race_cannot_resurrect_reader(foundation):
    repo = foundation
    session = available(repo, b"")
    read = repo.acquire_read(session["file_id"])
    repo.delete_file(session["file_id"])
    results = parallel(
        [
            lambda: repo.renew_read(read["read_id"], read["lease_token"]),
            lambda: replica(repo).release_read(read["read_id"], read["lease_token"]),
        ]
    )
    assert results[1] is True
    assert results[0] == "STALE_READ_LEASE" or results[0]["state"] == "ACTIVE"
    assert sql(repo, "SELECT state FROM download_leases")[0]["state"] == "RELEASED"
    repo.finish_cleanup(session["upload_id"], lease_for(repo, "CLEANUP"))
    assert repo.delete_file(session["file_id"])["state"] == "DELETED"


@pytest.mark.parametrize("phase", ["OPEN", "FINALIZING", "COMPLETED", "FAILED"])
def test_delete_is_idempotent_and_fences_upload_without_releasing_bytes(foundation, phase):
    repo = foundation
    if phase in ("FINALIZING", "COMPLETED", "FAILED"):
        session, job = finalizing(repo)
        if phase != "FINALIZING":
            publish(repo, session, job, data=b"abc" if phase == "COMPLETED" else b"xyz")
    else:
        nodes(repo)
        session = upload(repo)
        part = claim(repo, session)
    before = sql(repo, "SELECT * FROM allocations ORDER BY allocation_id")
    results = parallel(
        [
            lambda: repo.delete_file(session["file_id"]),
            lambda: replica(repo).delete_file(session["file_id"]),
        ]
    )
    assert (
        results[0]
        == results[1]
        == {"file_id": session["file_id"], "state": "DELETING", "cleanup_pending": True}
    )
    assert counters(repo) == (0, 6)
    assert before == sql(repo, "SELECT * FROM allocations ORDER BY allocation_id")
    assert len(sql(repo, "SELECT * FROM jobs WHERE kind='CLEANUP'")) == 1
    after = sql(repo, "SELECT * FROM files")
    assert after[0]["deleted_at"] is not None
    assert repo.delete_file(session["file_id"])["cleanup_pending"]
    assert after == sql(repo, "SELECT * FROM files")
    if phase == "OPEN":
        with pytest.raises(DomainError):
            confirm(repo, session, part)
    if phase == "FINALIZING":
        with pytest.raises(DomainError, match="UPLOAD_STATE_CONFLICT"):
            publish(repo, session, job)
    cleanup = lease_for(repo, "CLEANUP")
    page = repo.authorize_cleanup(session["upload_id"], cleanup)
    with pytest.raises(DomainError, match="OUTCOME_PENDING"):
        repo.finish_cleanup(session["upload_id"], cleanup)
    for allocation in page["allocations"]:
        assert repo.release_allocation(delete_proof(repo, allocation), cleanup)
        assert not repo.release_allocation(delete_proof(repo, allocation), cleanup)
    repo.finish_cleanup(session["upload_id"], cleanup)
    assert repo.delete_file(session["file_id"]) == {
        "file_id": session["file_id"],
        "state": "DELETED",
        "cleanup_pending": False,
    }
    assert counters(repo) == (0, 0) and repo.get_session(session["upload_id"])["active_claims"] == 0
    assert all(a["state"] == "CLOSED" for a in sql(repo, "SELECT * FROM write_attempts"))


@pytest.mark.parametrize("operation", ["confirm", "publish"])
def test_delete_races_upload_commit_without_resurrecting_file(foundation, operation):
    repo = foundation
    if operation == "publish":
        session, job = finalizing(repo)
    else:
        nodes(repo)
        session = upload(repo)
        part = claim(repo, session)

    def mutate():
        if operation == "publish":
            return publish(repo, session, job)
        return confirm(repo, session, part)

    results = parallel([mutate, lambda: replica(repo).delete_file(session["file_id"])])
    assert isinstance(results[0], dict) or results[0] == "UPLOAD_STATE_CONFLICT"
    assert results[1]["state"] == "DELETING"
    assert sql(repo, "SELECT state FROM files")[0]["state"] == "DELETING"
    assert counters(repo) == (0, 6)
    with pytest.raises(DomainError):
        mutate()


def test_delete_and_cleanup_publication_rollback_on_stale_job_or_enqueue_failure(
    foundation, monkeypatch
):
    repo = foundation
    session = available(repo, b"")
    before = sql(repo, "SELECT * FROM files")
    with monkeypatch.context() as patch:

        def fail(*args, **kwargs):
            raise DomainError("ENQUEUE_FAILED")

        patch.setattr(repo, "enqueue", fail)
        with pytest.raises(DomainError, match="ENQUEUE_FAILED"):
            repo.delete_file(session["file_id"])
    assert before == sql(repo, "SELECT * FROM files")
    assert repo.get_session(session["upload_id"])["state"] == "COMPLETED"
    repo.delete_file(session["file_id"])
    cleanup = lease_for(repo, "CLEANUP")
    sql(
        repo,
        "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' "
        "WHERE kind='CLEANUP'",
    )
    with pytest.raises(DomainError, match="STALE_JOB"):
        repo.finish_cleanup(session["upload_id"], cleanup)
    assert sql(repo, "SELECT state FROM files")[0]["state"] == "DELETING"
    current = lease_for(repo, "CLEANUP")
    repo.finish_cleanup(session["upload_id"], current)
    assert repo.delete_file(session["file_id"])["state"] == "DELETED"
    with pytest.raises(DomainError, match="FILE_NOT_FOUND"):
        repo.delete_file(uuid4())


@pytest.mark.parametrize("change", ["namespace", "epoch", "mode"])
def test_recovery_gate_blocks_every_read_and_delete_entrypoint(foundation, change):
    repo = foundation
    session = available(repo, b"")
    read = repo.acquire_read(session["file_id"])
    if change == "mode":
        sql(repo, "UPDATE cluster_control SET mode='RECOVERING'")
    else:
        column = "namespace_id" if change == "namespace" else "recovery_epoch"
        sql(repo, f"UPDATE cluster_control SET {column}=:value", value=uuid4())
    before = sql(repo, "SELECT * FROM download_leases")
    for action in [
        lambda: repo.resolve_download(session["file_id"]),
        lambda: repo.acquire_read(session["file_id"]),
        lambda: repo.renew_read(read["read_id"], read["lease_token"]),
        lambda: repo.release_read(read["read_id"], read["lease_token"]),
        lambda: repo.read_manifest(read["read_id"], read["lease_token"]),
        lambda: repo.delete_file(session["file_id"]),
    ]:
        with pytest.raises(DomainError, match="RECOVERY_REQUIRED"):
            action()
    assert before == sql(repo, "SELECT * FROM download_leases")
    assert sql(repo, "SELECT state FROM files")[0]["state"] == "AVAILABLE"


@pytest.mark.parametrize("data", [b"", b"abcdefg"])
def test_worker_waits_for_all_readers_without_spending_retry_budget_then_deletes_bytes(
    cluster, data
):
    repo, resources = cluster
    session = confirmed(repo, data)
    worker = Worker(repo, config=WorkerConfig(max_attempts=1))
    try:
        repo.complete_upload(session["upload_id"])
        assert run_job(worker, "FINALIZE")["state"] == "COMPLETED"
        reads = [repo.acquire_read(session["file_id"]) for _ in range(2)]
        repo.delete_file(session["file_id"])
        for _ in range(3):
            run_job(worker, "CLEANUP")
            job = sql(repo, "SELECT *,clock_timestamp() AS now FROM jobs WHERE kind='CLEANUP'")[0]
            assert job["state"] == "RETRY" and job["attempts"] == 0
            assert job["last_error"] == "READ_LEASE_ACTIVE"
            assert 8 < (job["run_after"] - job["now"]).total_seconds() <= 10
            assert counters(repo) == (0, len(data) * 2)
            for ledger, _, _ in resources:
                assert all(row["state"] == "STORED" for row in ledger.inventory(ledger.scope)[0])
            due(repo)
        repo.release_read(reads[0]["read_id"], reads[0]["lease_token"])
        run_job(worker, "CLEANUP")
        assert sql(repo, "SELECT state FROM jobs WHERE kind='CLEANUP'")[0]["state"] == "RETRY"
        # Expiry is sufficient for GC; the second reader never releases.
        expire_read(repo)
        due(repo)
        run_job(worker, "CLEANUP")
        assert counters(repo) == (0, 0)
        assert repo.delete_file(session["file_id"])["state"] == "DELETED"
        assert sql(repo, "SELECT state FROM jobs WHERE kind='CLEANUP'")[0]["state"] == "DONE"
        for ledger, _, _ in resources:
            assert all(row["cleanup_complete"] for row in ledger.inventory(ledger.scope)[0])
            assert not list((ledger.root / "chunks").iterdir())
    finally:
        worker.close()
