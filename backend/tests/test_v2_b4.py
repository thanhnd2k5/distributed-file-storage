"""B4 cross-boundary gates: PostgreSQL, real gRPC/ledger, offline CLI and ENOSPC.

Reuse B3's isolated schema fixture; never mutate the public schema or V1 volumes.
These gates exercise recovery hooks, not a cluster backup/restore procedure.
"""

import errno
import hashlib
import os
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from threading import Event
from uuid import UUID, uuid4

import grpc
import pytest
import v2_storage_pb2 as pb
import v2_storage_pb2_grpc as rpc
from test_v2_metadata import cleanup_lease, create, nodes, sql
from test_v2_metadata import foundation as foundation

from metadata.v2.repository import DeleteEvidence, DomainError, Repository
from storage.v2.ledger import Ledger, PreconditionError, Scope
from storage.v2.server import create_server


@contextmanager
def serving(root, scope, *, provision=False):
    ledger = Ledger(root, scope, provision=provision, chunk_limit=3)
    server, port, executor = create_server(ledger)
    try:
        server.start()
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            grpc.channel_ready_future(channel).result(timeout=5)
            yield ledger, rpc.StorageServiceStub(channel)
    finally:
        server.stop(0).wait()
        executor.shutdown(wait=True)
        ledger.close()


def wire_scope(scope):
    return pb.NodeScope(
        namespace_id=scope.namespace_id,
        recovery_epoch=scope.recovery_epoch,
        node_id=scope.node_id,
        node_incarnation=scope.node_incarnation,
    )


def key_for(scope, allocation):
    return pb.AllocationKey(scope=wire_scope(scope), allocation_id=str(allocation))


def delete_proof(record):
    scope = record.key.scope
    return DeleteEvidence(
        UUID(scope.namespace_id),
        UUID(scope.recovery_epoch),
        scope.node_id,
        UUID(scope.node_incarnation),
        UUID(record.key.allocation_id),
        record.generation,
        pb.AllocationState.Name(record.state),
        record.cleanup_complete,
    )


def claimed_storage(repo):
    nodes(repo)
    upload = create(repo)
    part = repo.claim_part(upload["upload_id"], 0, 3, hashlib.sha256(b"abc").hexdigest(), uuid4())
    destination = part["destinations"][0]
    scope = Scope(
        str(repo.namespace_id),
        str(repo.epoch),
        destination["node_id"],
        str(destination["incarnation"]),
    )
    key = key_for(scope, destination["allocation_id"])
    chunk = pb.ChunkSpec(
        chunk_id=str(part["chunk_id"]),
        size_bytes=3,
        checksum_sha256=hashlib.sha256(b"abc").hexdigest(),
    )
    return upload, scope, key, chunk


def store_frames(key, chunk):
    yield pb.StoreChunkFrame(header=pb.StoreChunkHeader(key=key, generation=1, chunk=chunk))
    yield pb.StoreChunkFrame(block=pb.DataBlock(offset=0, data=b"abc"))


def stored(stub, key, chunk):
    stub.PrepareAllocation(
        pb.PrepareAllocationRequest(key=key, generation=1, chunk=chunk), timeout=5
    )
    record = stub.StoreChunk(store_frames(key, chunk), timeout=5).allocation
    assert record.state == pb.STORED
    assert record.actual_size_bytes == 3
    assert record.actual_checksum_sha256 == chunk.checksum_sha256
    return record


def charged(repo):
    return sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"]


def test_b4_replacement_volume_cannot_ack_old_incarnation(foundation, tmp_path):
    repo = foundation
    upload, scope, key, chunk = claimed_storage(repo)
    old_root = tmp_path / "lost-ledger"
    with serving(old_root, scope, provision=True) as (_, stub):
        stored(stub, key, chunk)
    (old_root / "ledger.sqlite3").unlink()
    with pytest.raises(PreconditionError, match="Missing ledger"):
        Ledger(old_root, scope, provision=True)
    assert (old_root / "chunks" / key.allocation_id).read_bytes() == b"abc"

    replacement = replace(scope, node_incarnation=str(uuid4()))
    incarnation = UUID(replacement.node_incarnation)
    with serving(tmp_path / "replacement", replacement, provision=True) as (ledger, stub):
        repo.provision_node(
            scope.node_id,
            incarnation,
            host="replacement",
            port=50061,
            failure_domain="replacement-domain",
            budget=30,
        )
        assert (
            sql(
                repo, "SELECT state FROM node_incarnations WHERE incarnation=:inc", inc=incarnation
            )[0]["state"]
            == "JOINING"
        )
        with pytest.raises(DomainError, match="INSUFFICIENT_DOMAINS"):
            repo.claim_part(create(repo)["upload_id"], 0, 3, chunk.checksum_sha256, uuid4())
        with pytest.raises(grpc.RpcError) as failure:
            stub.DeleteAllocation(pb.AllocationRequest(key=key, generation=1), timeout=5)
        assert failure.value.code() == grpc.StatusCode.FAILED_PRECONDITION
        assert ledger.inventory(replacement)[0] == []

        health = stub.HealthCheck(pb.HealthCheckRequest(scope=wire_scope(replacement)), timeout=5)
        assert health.scope.node_incarnation == replacement.node_incarnation
        repo.activate_node(
            scope.node_id,
            incarnation,
            observed_namespace=UUID(health.scope.namespace_id),
            observed_epoch=UUID(health.scope.recovery_epoch),
        )
        assert (
            sql(
                repo,
                "SELECT state FROM node_incarnations WHERE incarnation=:inc",
                inc=UUID(scope.node_incarnation),
            )[0]["state"]
            == "RETIRED"
        )
        assert charged(repo) == 6
        repo.abort(upload["upload_id"])
        lease = cleanup_lease(repo)
        repo.authorize_cleanup(upload["upload_id"], lease)
        # A valid durable ACK from the replacement disk cannot free the old disk's charge.
        record = stub.DeleteAllocation(
            pb.AllocationRequest(key=key_for(replacement, key.allocation_id), generation=1),
            timeout=5,
        )
        with pytest.raises(DomainError, match="INVALID_DELETE_ACK"):
            repo.release_allocation(delete_proof(record), lease)
        assert charged(repo) == 6
        assert (
            sql(
                repo,
                "SELECT charged_bytes FROM node_budgets WHERE incarnation=:inc",
                inc=incarnation,
            )[0]["charged_bytes"]
            == 0
        )
        assert (old_root / "chunks" / key.allocation_id).read_bytes() == b"abc"
        assert ledger.lookup(key.allocation_id)["cleanup_complete"]


@pytest.mark.parametrize("failure_point", ["tombstone", "cleanup_flush"])
def test_b4_disk_full_retains_charge_until_durable_wire_ack(foundation, monkeypatch, failure_point):
    import storage.v2.ledger as ledger_module

    configured = os.environ.get("V2_DISK_FULL_DIR")
    assert configured, "B4 requires compose.v2-tests.yml bounded tmpfs"
    repo = foundation
    upload, scope, key, chunk = claimed_storage(repo)
    request = pb.AllocationRequest(key=key, generation=1)
    with tempfile.TemporaryDirectory(dir=configured) as directory:
        root = Path(directory)
        with serving(root, scope, provision=True) as (ledger, stub):
            stored(stub, key, chunk)
            repo.abort(upload["upload_id"])
            lease = cleanup_lease(repo)
            repo.authorize_cleanup(upload["upload_id"], lease)
            other_key = key_for(scope, uuid4())
            stub.PrepareAllocation(
                pb.PrepareAllocationRequest(key=other_key, generation=1, chunk=chunk), timeout=5
            )
            filler = root / "test-owned-filler"
            try:
                with filler.open("wb", buffering=0) as handle:
                    while True:
                        try:
                            handle.write(b"x" * 65536)
                        except OSError as exc:
                            assert exc.errno == errno.ENOSPC
                            break
                assert not ledger.writable(1)
                with pytest.raises(grpc.RpcError) as failure:
                    stub.StoreChunk(store_frames(other_key, chunk), timeout=5)
                assert failure.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
                assert ledger.lookup(other_key.allocation_id)["state"] == "PREPARED"

                original_flush = ledger_module.fsync_directory

                def exhausted(path):
                    # Before SQLite fence, or after unlink but before durable cleanup.
                    target = root if failure_point == "tombstone" else root / "chunks"
                    if path == target:
                        raise OSError(errno.ENOSPC, "B4 injected flush failure")
                    return original_flush(path)

                with monkeypatch.context() as patch:
                    patch.setattr(ledger_module, "fsync_directory", exhausted)
                    with pytest.raises(grpc.RpcError) as failure:
                        stub.DeleteAllocation(request, timeout=5)
                    assert failure.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
                row = ledger.lookup(key.allocation_id)
                assert not row["cleanup_complete"]
                assert row["state"] == ("STORED" if failure_point == "tombstone" else "DELETED")
                assert charged(repo) == 6
                assert (
                    sql(
                        repo,
                        "SELECT state FROM allocations WHERE allocation_id=:id",
                        id=UUID(key.allocation_id),
                    )[0]["state"]
                    == "DELETE_PENDING"
                )
                assert (
                    sql(repo, "SELECT state FROM jobs WHERE job_id=:id", id=lease.job_id)[0][
                        "state"
                    ]
                    == "RUNNING"
                )
                # Retry on the still-full filesystem uses reserved physical ledger space.
                ack = stub.DeleteAllocation(request, timeout=5)
                assert ack.state == pb.DELETED and ack.cleanup_complete
                assert charged(repo) == 6  # Simulate lost ACK at the coordinator.
                replay = stub.DeleteAllocation(request, timeout=5)
                assert repo.release_allocation(delete_proof(replay), lease)
                assert not repo.release_allocation(delete_proof(replay), lease)
                assert charged(repo) == 3
                assert not (root / "chunks" / key.allocation_id).exists()
                assert not list((root / "tmp").iterdir())
            finally:
                filler.unlink(missing_ok=True)
        with serving(root, scope) as (ledger, stub):
            assert ledger.writable(1)
            assert stub.InspectAllocation(request, timeout=5).cleanup_complete
            assert charged(repo) == 3


def test_b4_offline_epoch_cli_fences_old_stream_and_preserves_unknown_bytes(
    foundation, tmp_path, monkeypatch
):
    repo = foundation
    upload, scope, key, chunk = claimed_storage(repo)
    root = tmp_path / "epoch-volume"
    new_epoch = uuid4()
    entered, drain = Event(), Event()
    cleanup_upload = create(repo, size=0)
    repo.abort(cleanup_upload["upload_id"])
    lease = cleanup_lease(repo)

    def maintenance(action, allocation=None, *, success=True):
        env = dict(
            os.environ,
            V2_NAMESPACE_ID=scope.namespace_id,
            V2_RECOVERY_EPOCH=scope.recovery_epoch,
            NODE_ID=scope.node_id,
            V2_NODE_INCARNATION=scope.node_incarnation,
            V2_DATA_DIR=str(root),
        )
        command = [
            sys.executable,
            "scripts/v2_storage_maintenance.py",
            action,
            "--epoch",
            str(new_epoch),
        ]
        if allocation:
            command.extend(["--allocation", allocation])
        result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=10)
        assert (result.returncode == 0) == success, result.stderr
        return result

    orphan = key_for(scope, uuid4())
    tombstone = key_for(scope, uuid4())
    with serving(root, scope, provision=True) as (ledger, stub):
        stored(stub, orphan, chunk)
        stub.DeleteAllocation(pb.AllocationRequest(key=tombstone, generation=1), timeout=5)
        stub.PrepareAllocation(
            pb.PrepareAllocationRequest(key=key, generation=1, chunk=chunk), timeout=5
        )

        original_write = ledger.write_block

        def observe_write(handle, block):
            original_write(handle, block)
            entered.set()

        monkeypatch.setattr(ledger, "write_block", observe_write)

        def slow_frames():
            yield next(store_frames(key, chunk))
            yield pb.StoreChunkFrame(block=pb.DataBlock(offset=0, data=b"a"))
            assert drain.wait(timeout=10)
            yield pb.StoreChunkFrame(block=pb.DataBlock(offset=1, data=b"bc"))

        pending = stub.StoreChunk.future(slow_frames(), timeout=10)
        try:
            assert entered.wait(timeout=5)
            assert ledger.lookup(key.allocation_id)["state"] == "WRITING"
            sql(repo, "UPDATE cluster_control SET mode='RECOVERING'")
            before = sql(repo, "SELECT * FROM node_budgets ORDER BY node_id")
            for action in [
                repo.ready,
                lambda: repo.abort(upload["upload_id"]),
                lambda: repo.claim_jobs(uuid4()),
                lambda: repo.expire_sessions(),
                lambda: repo.authorize_cleanup(cleanup_upload["upload_id"], lease),
                lambda: repo.renew_job(lease.job_id, lease.owner, lease.token),
                lambda: repo.finish_job(lease.job_id, lease.owner, lease.token),
            ]:
                with pytest.raises(DomainError, match="RECOVERY_REQUIRED"):
                    action()
            failed = maintenance("begin", success=False)
            assert "BlockingIOError" in failed.stderr  # Serving process still owns flock.
            drain.set()
            assert pending.result(timeout=5).allocation.state == pb.STORED
            assert charged(repo) == 6
            assert before == sql(repo, "SELECT * FROM node_budgets ORDER BY node_id")
        finally:
            drain.set()
            pending.cancel()

    maintenance("begin")
    with pytest.raises(PreconditionError, match="Recovery gate closed"):
        Ledger(root, scope)
    maintenance("adopt", key.allocation_id)
    maintenance("adopt", tombstone.allocation_id, success=False)
    maintenance("close", tombstone.allocation_id)
    maintenance("activate")
    with pytest.raises(PreconditionError, match="mismatch"):
        Ledger(root, scope)

    new_scope = replace(scope, recovery_epoch=str(new_epoch))
    with serving(root, new_scope) as (ledger, stub):
        with pytest.raises(grpc.RpcError) as failure:
            stub.StoreChunk(store_frames(key, chunk), timeout=5)
        assert failure.value.code() == grpc.StatusCode.FAILED_PRECONDITION
        adopted = key_for(new_scope, key.allocation_id)
        assert (
            stub.InspectAllocation(pb.AllocationRequest(key=adopted, generation=1), timeout=5).state
            == pb.STORED
        )
        for method in [stub.InspectAllocation, stub.DeleteAllocation]:
            with pytest.raises(grpc.RpcError) as failure:
                method(
                    pb.AllocationRequest(
                        key=key_for(new_scope, orphan.allocation_id), generation=1
                    ),
                    timeout=5,
                )
            assert failure.value.code() == grpc.StatusCode.FAILED_PRECONDITION
        with pytest.raises(grpc.RpcError) as failure:
            stub.PrepareAllocation(
                pb.PrepareAllocationRequest(
                    key=key_for(new_scope, tombstone.allocation_id), generation=99, chunk=chunk
                ),
                timeout=5,
            )
        assert failure.value.code() == grpc.StatusCode.FAILED_PRECONDITION
        inventory = stub.ListAllocations(
            pb.ListAllocationsRequest(scope=wire_scope(new_scope), limit=100), timeout=5
        )
        unknown = next(
            row for row in inventory.items if row.key.allocation_id == orphan.allocation_id
        )
        assert unknown.key.scope.recovery_epoch == scope.recovery_epoch
        assert (root / "chunks" / orphan.allocation_id).read_bytes() == b"abc"
        assert (root / "chunks" / key.allocation_id).read_bytes() == b"abc"
        assert not list((root / "tmp").iterdir())
        assert charged(repo) == 6
        assert (
            sql(
                repo,
                "SELECT state FROM upload_sessions WHERE upload_id=:id",
                id=upload["upload_id"],
            )[0]["state"]
            == "OPEN"
        )
        # Installing a Storage epoch alone never opens the restored DB gate.
        new_repo = Repository(repo.engine, repo.namespace_id, new_epoch, schema=repo.schema)
        with pytest.raises(DomainError, match="RECOVERY_REQUIRED"):
            new_repo.ready()
        sql(repo, "UPDATE cluster_control SET mode='ACTIVE'")
        with pytest.raises(DomainError, match="RECOVERY_REQUIRED"):
            new_repo.ready()  # DB still carries the old epoch; do not auto-promote it.
