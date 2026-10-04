from contextlib import contextmanager
from threading import Event

import grpc
import pytest
from health_control import pause_background
from sqlalchemy.exc import SQLAlchemyError
from test_metadata_delete import api as production_api
from test_metadata_delete import cluster as production_cluster
from test_metadata_delete import snapshot, upload

from metadata.models import ChunkReplica, File, StorageNode
from metadata.operations import DataOperation, data_operation
from metadata.repair import RepairChunk, repair_chunk
from metadata.storage_client import StorageRpcError

api = production_api
cluster = production_cluster


def stop_health(api):
    pause_background(api[1].state.health_worker)


def first_chunk(api, file_id):
    file, chunks, _ = snapshot(api, file_id)
    chunk = min(chunks, key=lambda c: c.chunk_index)
    return RepairChunk(
        file.id,
        chunk.id,
        chunk.chunk_index,
        chunk.size_bytes,
        chunk.checksum_sha256,
        file.replication_factor,
    )


def run_one(api, file_id):
    with data_operation(api[1].state) as operation:
        return repair_chunk(operation, api[4], first_chunk(api, file_id), Event(), set(), {})


def path(api, replica):
    return api[3][int(replica.node_id[-1]) - 1].service.store.chunk_path(str(replica.chunk_id))


def force_destination(api, file_id):
    stop_health(api)
    copies = snapshot(api, file_id)[2]
    bad = copies[0]
    with api[2].begin() as session:
        # Keep only known nodes as writable destinations, forcing recovery at the old node.
        for node in session.query(StorageNode):
            if node.node_id not in {r.node_id for r in copies}:
                node.enabled = False
    return bad, copies[1]


def test_repair_probes_healthy_cached_rf_without_creating_replica(api, monkeypatch):
    file_id = upload(api)
    original = DataOperation.get_chunk
    calls = []

    def probe(operation, *args, **kwargs):
        calls.append(args[0])
        return original(operation, *args, **kwargs)

    monkeypatch.setattr(DataOperation, "get_chunk", probe)
    result, count = run_one(api, file_id)
    assert result.outcome == "HEALTHY" and count == 0 and result.live_replica_count == 2
    assert len(calls) == 2 and len(snapshot(api, file_id)[2]) == 2


@pytest.mark.parametrize("mode", ["missing", "corrupted", "pending"])
def test_repair_missing_corrupt_or_ambiguous_replica_from_verified_source(api, monkeypatch, mode):
    data = b"verified source payload"
    file_id = upload(api, data)
    bad, good = force_destination(api, file_id)
    if mode == "missing":
        path(api, bad).unlink()
    elif mode == "corrupted":
        path(api, bad).write_bytes(b"!" * len(data))
    with api[2].begin() as session:
        session.get(ChunkReplica, (bad.chunk_id, bad.node_id)).status = "PENDING"
    original = DataOperation.store_chunk
    calls = []

    def store(operation, node, chunk_id, payload, checksum, **kwargs):
        with api[2]() as session:
            replica = session.get(ChunkReplica, (bad.chunk_id, node))
            assert replica.status == "PENDING" and not replica.cleanup_pending
            assert session.get(ChunkReplica, (good.chunk_id, good.node_id)).status == "VERIFIED"
        assert not operation._in_transaction and payload == data
        calls.append(node)
        return original(operation, node, chunk_id, payload, checksum, **kwargs)

    monkeypatch.setattr(DataOperation, "store_chunk", store)
    result, count = run_one(api, file_id)
    assert result.outcome == ("HEALTHY" if mode == "pending" else "REPAIRED")
    assert count == (0 if mode == "pending" else 1) and len(calls) == count
    assert path(api, bad).read_bytes() == data and path(api, good).read_bytes() == data
    assert result.live_replica_count == 2
    assert api[0].get(f"/api/v1/files/{file_id}/download").content == data


@pytest.mark.parametrize("mode", ["all_missing", "all_corrupt", "all_timeout"])
def test_repair_without_source_never_deletes_or_stores(api, monkeypatch, mode):
    file_id = upload(api)
    stop_health(api)
    for replica in snapshot(api, file_id)[2]:
        if mode == "all_missing":
            path(api, replica).unlink()
        elif mode == "all_corrupt":
            path(api, replica).write_bytes(b"bad")
    if mode == "all_timeout":

        def timeout(operation, node, *args, **kwargs):
            raise StorageRpcError("GetChunk", node, grpc.StatusCode.DEADLINE_EXCEEDED, 2)

        monkeypatch.setattr(DataOperation, "get_chunk", timeout)

    def forbidden(*args, **kwargs):
        pytest.fail("No verified source: destructive RPC is forbidden")

    monkeypatch.setattr(DataOperation, "store_chunk", forbidden)
    monkeypatch.setattr(DataOperation, "delete_chunk", forbidden)
    result, count = run_one(api, file_id)
    assert result.outcome == "UNAVAILABLE" and count == 0 and result.live_replica_count == 0
    if mode == "all_timeout":
        assert all(
            r.status == "VERIFIED" and r.last_error == "DEADLINE_EXCEEDED"
            for r in snapshot(api, file_id)[2]
        )


def test_repair_down_node_copies_to_third_domain_and_retains_old_mapping(api):
    data = b"new replica bytes"
    file_id = upload(api, data)
    stop_health(api)
    bad = snapshot(api, file_id)[2][0]
    with api[2].begin() as session:
        session.get(StorageNode, bad.node_id).status = "DOWN"
    result, count = run_one(api, file_id)
    assert result.outcome == "REPAIRED" and count == 1 and not result.domain_degraded
    copies = snapshot(api, file_id)[2]
    assert len(copies) == 3 and all(path(api, r).read_bytes() == data for r in copies)
    with api[2].begin() as session:
        session.get(StorageNode, bad.node_id).status = "ACTIVE"
    result, count = run_one(api, file_id)
    assert result.outcome == "HEALTHY" and count == 0 and result.live_replica_count == 3


def test_repair_one_live_node_no_destination_and_suspected_source_is_not_live(api):
    file_id = upload(api)
    stop_health(api)
    copies = snapshot(api, file_id)[2]
    with api[2].begin() as session:
        for node in session.query(StorageNode):
            node.status = "SUSPECTED" if node.node_id == copies[0].node_id else "DOWN"
    result, count = run_one(api, file_id)
    assert result.outcome == "NO_DESTINATION" and count == 0 and result.live_replica_count == 0


def test_repair_disabled_or_unconfigured_endpoint_has_no_rpc(api, monkeypatch):
    file_id = upload(api)
    stop_health(api)
    copies = snapshot(api, file_id)[2]
    excluded = copies[0].node_id
    with api[2].begin() as session:
        session.get(StorageNode, excluded).port += 1
    original = DataOperation.get_chunk
    calls = []

    def probe(operation, node, *args, **kwargs):
        assert node != excluded
        calls.append(node)
        return original(operation, node, *args, **kwargs)

    monkeypatch.setattr(DataOperation, "get_chunk", probe)
    result, count = run_one(api, file_id)
    assert result.outcome == "REPAIRED" and count == 1 and len(calls) == 1
    assert len(snapshot(api, file_id)[2]) == 3


def test_repair_uses_file_rf_and_same_domain_degradation_without_extra_copy(api):
    file_id = upload(api)
    stop_health(api)
    api[4].replication_factor = 3
    with api[2].begin() as session:
        copies = snapshot(api, file_id)[2]
        for replica in copies:
            node = session.get(StorageNode, replica.node_id)
            node.failure_domain = "same-domain"
        # Mirror the configured topology, so endpoint identity matching remains valid.
        for config in api[4].storage_nodes_json:
            if config.node_id in {r.node_id for r in copies}:
                config.failure_domain = "same-domain"
    result, count = run_one(api, file_id)
    assert result.outcome == "HEALTHY" and count == 0 and result.live_replica_count == 2
    assert result.domain_degraded and len(snapshot(api, file_id)[2]) == 2


@pytest.mark.parametrize("fault", ["delete", "store", "ack_loss"])
def test_repair_failed_attempt_is_durable_and_reconciled_next_pass(api, monkeypatch, fault):
    file_id = upload(api)
    bad, _ = force_destination(api, file_id)
    path(api, bad).write_bytes(b"wrong")
    original = DataOperation.store_chunk if fault != "delete" else DataOperation.delete_chunk

    def fail(operation, node, *args, **kwargs):
        if fault == "ack_loss":
            original(operation, node, *args, **kwargs)
        raise StorageRpcError(
            "DeleteChunk" if fault == "delete" else "StoreChunk",
            node,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            2,
        )

    method = "delete_chunk" if fault == "delete" else "store_chunk"
    monkeypatch.setattr(DataOperation, method, fail)
    result, count = run_one(api, file_id)
    assert result.outcome == "ERROR" and count == 0
    attempt = next(r for r in snapshot(api, file_id)[2] if r.node_id == bad.node_id)
    assert attempt.status == "PENDING" and not attempt.cleanup_pending
    monkeypatch.setattr(DataOperation, method, original)
    result, count = run_one(api, file_id)
    assert result.outcome == ("HEALTHY" if fault == "ack_loss" else "REPAIRED")
    assert count == (0 if fault == "ack_loss" else 1) and result.live_replica_count == 2


@pytest.mark.parametrize("after_store", [False, True])
def test_repair_db_failure_preserves_attempt_and_never_stores_before_pending_commit(
    api, monkeypatch, after_store
):
    file_id = upload(api)
    bad, _ = force_destination(api, file_id)
    path(api, bad).unlink()
    original_transaction = DataOperation.transaction
    original_store = DataOperation.store_chunk
    calls = []

    @contextmanager
    def transaction(operation):
        with original_transaction(operation) as session:
            yield session
            dirty_pending = any(
                isinstance(row, ChunkReplica) and row.status == "PENDING" for row in session.dirty
            )
            if (not after_store and dirty_pending) or (after_store and calls):
                raise SQLAlchemyError("injected database interruption")

    def store(operation, *args, **kwargs):
        ack = original_store(operation, *args, **kwargs)
        calls.append(ack)
        return ack

    monkeypatch.setattr(DataOperation, "transaction", transaction)
    monkeypatch.setattr(DataOperation, "store_chunk", store)
    with pytest.raises(SQLAlchemyError):
        run_one(api, file_id)
    assert bool(calls) == after_store
    with api[2]() as session:
        replica = session.get(ChunkReplica, (bad.chunk_id, bad.node_id))
        assert replica.status == ("PENDING" if after_store else "MISSING")
        assert session.get(File, file_id).status == "AVAILABLE"
    assert path(api, bad).exists() == after_store and not api[1].state.operation_lock.locked()
