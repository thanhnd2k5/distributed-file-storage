"""F3 node rejoin and inventory reconciliation gates on real PostgreSQL and gRPC."""

import hashlib
from threading import Event
from uuid import uuid4

import pytest
import v2_storage_pb2 as pb
import v2_storage_pb2_grpc as rpc
from test_v2_c1 import claim, confirm, upload
from test_v2_e2 import accounting
from test_v2_f2_integration import (
    f2_cluster as f2_cluster,
)
from test_v2_f2_integration import (
    make_available,
    read_verified_bytes,
    store_blocks,
    until,
)
from test_v2_metadata import foundation as foundation
from test_v2_metadata import sql

from metadata.v2.repository import InventoryItem, InventoryPage, Policy
from metadata.v2.storage_client import StorageClient, key_for
from metadata.v2.worker import Worker, WorkerConfig
from storage.v2.ledger import Ledger, Scope
from storage.v2.server import create_server


def test_same_volume_rejoin_pages_inventory_and_wakes_dead_repair(f2_cluster):
    repo, harness = f2_cluster
    repo.policy = Policy(chunk_size_bytes=3, replication_factor=2, min_distinct_domains=2)
    n0 = harness.add_node("node-0", domain="domain-0", budget=100, chunk_limit=10)
    harness.add_node("node-1", domain="domain-1", budget=100, chunk_limit=10)

    session = make_available(repo, b"abcdefghi")
    assert session["part_count"] == 3

    # Simulate node-0 going DOWN and a DEAD repair job waiting for node recovery.
    n0.stop()
    for _ in range(3):
        sql(
            repo,
            "UPDATE node_health_polls SET next_poll_at=clock_timestamp() WHERE node_id='node-0'",
        )
        ticket = repo.claim_health_poll(node_id="node-0")
        obs = StorageClient().health(ticket, Event())
        assert obs.outcome == "unreachable"
        assert repo.apply_health_poll(ticket, obs)
    assert (
        sql(repo, "SELECT health_state FROM storage_nodes WHERE node_id='node-0'")[0][
            "health_state"
        ]
        == "DOWN"
    )

    chunk_0 = sql(
        repo,
        "SELECT chunk_id FROM manifest_parts WHERE version_id=:v AND part_index=0",
        v=session["version_id"],
    )[0]["chunk_id"]
    dead_job_id = repo.enqueue_repair(session["file_id"], session["version_id"], chunk_0)
    assert dead_job_id is not None
    sql(repo, "UPDATE jobs SET state='DEAD',attempts=8 WHERE job_id=:id", id=dead_job_id)

    # Restart node-0 on the same volume and incarnation.
    n0.restart(repo)

    # A raw apply_health_poll on a DOWN node MUST NOT promote it to ACTIVE without inventory rejoin.
    ticket = repo.claim_health_poll(node_id="node-0")
    obs = StorageClient().health(ticket, Event())
    assert obs.outcome == "reachable"
    assert repo.apply_health_poll(ticket, obs)
    assert (
        sql(repo, "SELECT health_state FROM storage_nodes WHERE node_id='node-0'")[0][
            "health_state"
        ]
        == "DOWN"
    )

    # Worker rejoin with page_size=1 pages across all 3 allocations and promotes node-0 to ACTIVE.
    worker = Worker(repo, config=WorkerConfig(page_size=1))
    try:
        outcome = worker.poll_health(node_id="node-0")
        assert outcome == "ACTIVE"
    finally:
        worker.close()

    node_row = sql(
        repo,
        "SELECT health_state,health_generation,available_bytes "
        "FROM storage_nodes WHERE node_id='node-0'",
    )[0]
    assert node_row["health_state"] == "ACTIVE"
    assert node_row["available_bytes"] is not None

    alloc_rows = sql(
        repo,
        "SELECT state,replica_health,observation_generation "
        "FROM allocations WHERE node_id='node-0'",
    )
    assert len(alloc_rows) == 3
    assert all(
        r["state"] == "STORED"
        and r["replica_health"] == "VERIFIED"
        and r["observation_generation"] == node_row["health_generation"]
        for r in alloc_rows
    )
    assert sql(repo, "SELECT state,attempts FROM jobs WHERE job_id=:id", id=dead_job_id)[0] == {
        "state": "RETRY",
        "attempts": 0,
    }
    assert (
        read_verified_bytes(repo, session["file_id"], expected_nodes={"node-0", "node-1"})
        == b"abcdefghi"
    )
    accounting(repo, sessions=0, claims=0, charged=18)


def test_rejoin_detects_missing_and_corrupt_retains_charge_and_repairs(f2_cluster):
    repo, harness = f2_cluster
    repo.policy = Policy(chunk_size_bytes=3, replication_factor=2, min_distinct_domains=2)
    n0 = harness.add_node("node-0", domain="domain-0", budget=100, chunk_limit=10)
    harness.add_node("node-1", domain="domain-1", budget=100, chunk_limit=10)

    session = make_available(repo, b"abcdef")
    # Also create an uncommitted session with an UNKNOWN allocation on node-0 to verify
    # rejoin never promotes UNKNOWN to STORED.
    uncommitted = upload(repo, b"xyz")
    u_part = claim(repo, uncommitted, index=0, data=b"xyz")
    store_blocks(u_part, b"xyz")
    sql(
        repo,
        "UPDATE allocations SET state='UNKNOWN',replica_health='UNKNOWN' WHERE chunk_id=:c",
        c=u_part["chunk_id"],
    )

    parts = sql(
        repo,
        """SELECT m.part_index,m.chunk_id,a.allocation_id
        FROM manifest_parts m JOIN allocations a USING (chunk_id)
        WHERE m.version_id=:v AND a.node_id='node-0' ORDER BY m.part_index""",
        v=session["version_id"],
    )
    part0_aid = str(parts[0]["allocation_id"])
    part1_aid = str(parts[1]["allocation_id"])

    # Stop node-0:
    # - Remove part 0 from node-0's SQLite ledger & disk (simulating a missing replica).
    # - Corrupt part 1's chunk file on disk so Ledger.recover() marks it CORRUPT on restart.
    with n0.ledger._mutex, n0.ledger._db:
        n0.ledger._db.execute("DELETE FROM allocations WHERE allocation_id=?", (part0_aid,))
    (n0.root / "chunks" / part0_aid).unlink()
    (n0.root / "chunks" / part1_aid).write_bytes(b"bad")

    sql(repo, "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-0'")
    n0.restart(repo)

    # Add active node-2 in domain-2 so F2 repair can heal both deficits after rejoin.
    harness.add_node("node-2", domain="domain-2", budget=100, chunk_limit=10)

    before_budget = sql(repo, "SELECT charged_bytes FROM node_budgets WHERE node_id='node-0'")[0][
        "charged_bytes"
    ]
    assert before_budget == 9  # 6 bytes for session + 3 bytes for uncommitted

    worker = Worker(repo, config=WorkerConfig(page_size=1, repair_scan_seconds=60.0))
    try:
        assert worker.poll_health(node_id="node-0") == "ACTIVE"

        # Check metadata states immediately after rejoin before executing repair jobs:
        p0_row = sql(
            repo,
            "SELECT state,replica_health FROM allocations WHERE allocation_id=:aid",
            aid=parts[0]["allocation_id"],
        )[0]
        p1_row = sql(
            repo,
            "SELECT state,replica_health FROM allocations WHERE allocation_id=:aid",
            aid=parts[1]["allocation_id"],
        )[0]
        u_row = sql(
            repo,
            "SELECT state,replica_health FROM allocations WHERE chunk_id=:c AND node_id='node-0'",
            c=u_part["chunk_id"],
        )[0]
        assert p0_row == {"state": "STORED", "replica_health": "MISSING"}
        assert p1_row == {"state": "STORED", "replica_health": "CORRUPT"}
        assert u_row == {"state": "UNKNOWN", "replica_health": "UNKNOWN"}

        # Charged bytes on node-0 MUST be retained for both MISSING and CORRUPT allocations.
        assert (
            sql(repo, "SELECT charged_bytes FROM node_budgets WHERE node_id='node-0'")[0][
                "charged_bytes"
            ]
            == before_budget
        )

        # Both parts of the AVAILABLE file must have REPAIR jobs enqueued and converge to node-2.
        until(
            lambda: (
                worker.tick() is None
                and not sql(
                    repo,
                    "SELECT job_id FROM jobs WHERE kind='REPAIR' AND state!='DONE'",
                )
            )
        )
    finally:
        worker.close()

    assert (
        read_verified_bytes(repo, session["file_id"], expected_nodes={"node-1", "node-2"})
        == b"abcdef"
    )
    assert (
        sql(repo, "SELECT charged_bytes FROM node_budgets WHERE node_id='node-2'")[0][
            "charged_bytes"
        ]
        == 6
    )
    accounting(repo, sessions=1, claims=1, charged=24)


@pytest.mark.parametrize("mode", ["unknown_allocation", "resurrected_released", "old_epoch"])
def test_rejoin_quarantines_orphan_or_resurrected_allocation_without_deleting_or_publishing(
    f2_cluster, mode
):
    repo, harness = f2_cluster
    repo.policy = Policy(chunk_size_bytes=3, replication_factor=2, min_distinct_domains=2)
    n0 = harness.add_node("node-0", domain="domain-0", budget=100, chunk_limit=10)
    harness.add_node("node-1", domain="domain-1", budget=100, chunk_limit=10)
    harness.add_node("node-2", domain="domain-2", budget=100, chunk_limit=10)

    session = make_available(repo, b"abc")
    sql(repo, "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-0'")

    orphan_aid = str(uuid4())
    orphan_chunk_id = str(uuid4())
    orphan_hash = hashlib.sha256(b"orp").hexdigest()
    orphan_file = n0.root / "chunks" / orphan_aid

    if mode in ("unknown_allocation", "old_epoch"):
        orphan_file.write_bytes(b"orp")
        epoch = str(uuid4()) if mode == "old_epoch" else str(repo.epoch)
        with n0.ledger._mutex, n0.ledger._db:
            n0.ledger._db.execute(
                """INSERT INTO allocations (
                    allocation_id,epoch,generation,chunk_id,size_bytes,
                    checksum_sha256,state,actual_size,actual_hash,cleanup_complete
                ) VALUES (?,?,?,?,?,?,'STORED',3,?,0)""",
                (
                    orphan_aid,
                    epoch,
                    1,
                    orphan_chunk_id,
                    3,
                    orphan_hash,
                    orphan_hash,
                ),
            )
    else:
        # Mode: resurrected_released -> PostgreSQL says RELEASED, but Storage still has STORED.
        aid = sql(
            repo,
            "SELECT allocation_id FROM allocations WHERE node_id='node-0'",
        )[0]["allocation_id"]
        orphan_file = n0.root / "chunks" / str(aid)
        sql(
            repo,
            """UPDATE allocations SET state='RELEASED',cleanup_complete_at=clock_timestamp()
            WHERE allocation_id=:aid""",
            aid=aid,
        )
        sql(
            repo,
            "UPDATE node_budgets SET charged_bytes=0 WHERE node_id='node-0'",
        )

    worker = Worker(repo, config=WorkerConfig(page_size=1, repair_scan_seconds=60.0))
    try:
        assert worker.poll_health(node_id="node-0") == "QUARANTINED"
        assert (
            sql(repo, "SELECT health_state FROM storage_nodes WHERE node_id='node-0'")[0][
                "health_state"
            ]
            == "QUARANTINED"
        )
        # Orphan file on disk MUST NOT be auto-deleted, and MUST NOT be added to PostgreSQL.
        assert orphan_file.exists()
        assert not sql(
            repo,
            "SELECT allocation_id FROM allocations WHERE allocation_id=:aid",
            aid=orphan_aid,
        )
        # Repair job is automatically enqueued and heals the file onto node-2.
        until(
            lambda: (
                worker.tick() is None
                and not sql(
                    repo,
                    "SELECT job_id FROM jobs WHERE kind='REPAIR' AND state!='DONE'",
                )
            )
        )
    finally:
        worker.close()

    assert (
        read_verified_bytes(repo, session["file_id"], expected_nodes={"node-1", "node-2"}) == b"abc"
    )
    accounting(
        repo,
        sessions=0,
        claims=0,
        charged=6 if mode == "resurrected_released" else 9,
    )


def test_replacement_volume_new_incarnation_quarantines_until_explicit_activation_and_repairs(
    f2_cluster, tmp_path
):
    repo, harness = f2_cluster
    repo.policy = Policy(chunk_size_bytes=3, replication_factor=2, min_distinct_domains=2)
    n0 = harness.add_node("node-0", domain="domain-0", budget=100, chunk_limit=10)
    harness.add_node("node-1", domain="domain-1", budget=100, chunk_limit=10)
    harness.add_node("node-2", domain="domain-2", budget=100, chunk_limit=10)

    session = make_available(repo, b"abc")
    old_inc = n0.incarnation
    sql(repo, "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-0'")

    # Replace node-0's disk with a fresh volume and new incarnation before administrative approval.
    n0.stop()
    new_inc = uuid4()
    new_root = tmp_path / "node-0-replacement"
    n0.root = new_root
    n0.incarnation = new_inc
    n0.ledger = Ledger(
        new_root,
        Scope(str(repo.namespace_id), str(repo.epoch), "node-0", str(new_inc)),
        provision=True,
        chunk_limit=10,
    )
    n0.server, n0.port, n0.executor = create_server(n0.ledger, failure_domain="domain-0")
    n0.server.start()
    sql(repo, "UPDATE storage_nodes SET port=:port WHERE node_id='node-0'", port=n0.port)

    worker = Worker(repo, config=WorkerConfig(page_size=1, repair_scan_seconds=60.0))
    try:
        # Unapproved new incarnation fails scope check and transitions node-0 to QUARANTINED.
        assert worker.poll_health(node_id="node-0") == "scope_mismatch"
        assert (
            sql(repo, "SELECT health_state FROM storage_nodes WHERE node_id='node-0'")[0][
                "health_state"
            ]
            == "QUARANTINED"
        )

        # Admin provisions and activates replacement incarnation; old incarnation keeps charge.
        repo.provision_node(
            "node-0",
            new_inc,
            host="127.0.0.1",
            port=n0.port,
            failure_domain="domain-0",
            budget=100,
        )
        repo.activate_node(
            "node-0",
            new_inc,
            observed_namespace=repo.namespace_id,
            observed_epoch=repo.epoch,
        )
        assert (
            sql(
                repo,
                "SELECT state FROM node_incarnations WHERE node_id='node-0' AND incarnation=:inc",
                inc=old_inc,
            )[0]["state"]
            == "RETIRED"
        )
        assert (
            sql(
                repo,
                "SELECT charged_bytes FROM node_budgets "
                "WHERE node_id='node-0' AND incarnation=:inc",
                inc=old_inc,
            )[0]["charged_bytes"]
            == 3
        )
        assert (
            sql(
                repo,
                "SELECT charged_bytes FROM node_budgets "
                "WHERE node_id='node-0' AND incarnation=:inc",
                inc=new_inc,
            )[0]["charged_bytes"]
            == 0
        )

        # Old incarnation replica is ineligible; Worker scans and repairs onto node-2.
        worker.next_repair_scan = 0.0
        until(
            lambda: (
                worker.tick() is None
                and len(repo.repair_scan_page()["deficits"]) == 0
                and not sql(
                    repo,
                    "SELECT job_id FROM jobs WHERE kind='REPAIR' AND state!='DONE'",
                )
            )
        )
    finally:
        worker.close()

    assert (
        read_verified_bytes(repo, session["file_id"], expected_nodes={"node-1", "node-2"}) == b"abc"
    )
    accounting(repo, sessions=0, claims=0, charged=9)


def test_tombstone_retains_charge_until_delete_ack_and_late_inventory_is_fenced(f2_cluster):
    repo, harness = f2_cluster
    repo.policy = Policy(chunk_size_bytes=3, replication_factor=2, min_distinct_domains=2)
    n0 = harness.add_node("node-0", domain="domain-0", budget=100, chunk_limit=10)
    harness.add_node("node-1", domain="domain-1", budget=100, chunk_limit=10)

    session = make_available(repo, b"abc")

    # Create a pre-Prepare DELETED tombstone (without ChunkSpec) on node-0 to verify wire parsing.
    tombstone_target = {
        "namespace_id": repo.namespace_id,
        "recovery_epoch": repo.epoch,
        "node_id": "node-0",
        "incarnation": n0.incarnation,
        "allocation_id": uuid4(),
        "generation": 1,
        "host": "127.0.0.1",
        "port": n0.port,
    }
    with StorageClient.channel(tombstone_target) as channel:
        stub = rpc.StorageServiceStub(channel)
        rec = stub.DeleteAllocation(
            pb.AllocationRequest(key=key_for(tombstone_target), generation=1), timeout=5
        )
        assert rec.state == pb.DELETED and not rec.HasField("chunk")

    # Node-0 goes DOWN, and file is deleted while node-0 is DOWN.
    sql(repo, "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-0'")
    repo.delete_file(session["file_id"])

    # Claim an old rejoin ticket and let a newer ticket supersede it to test late inventory fencing.
    stale_ticket = repo.claim_health_poll(node_id="node-0")
    sql(
        repo,
        """UPDATE node_health_polls SET next_poll_at=clock_timestamp()-interval '1 second',
        ticket_expires_at=clock_timestamp()-interval '1 second' WHERE node_id='node-0'""",
    )
    active_ticket = repo.claim_health_poll(node_id="node-0")
    assert active_ticket["generation"] > stale_ticket["generation"]

    fake_corrupt_page = InventoryPage(
        items=(
            InventoryItem(
                namespace_id=repo.namespace_id,
                recovery_epoch=repo.epoch,
                node_id="node-0",
                incarnation=n0.incarnation,
                allocation_id=uuid4(),
                generation=1,
                state="CORRUPT",
                cleanup_complete=False,
                inventory_sequence=1,
                chunk_id=uuid4(),
                size_bytes=3,
                checksum_sha256=hashlib.sha256(b"abc").hexdigest(),
            ),
        ),
        upper_sequence=1,
        next_after_sequence=1,
        has_more=False,
    )
    # Stale ticket MUST be fenced and return STALE without quarantining or mutating node-0.
    assert repo.reconcile_rejoin_page(stale_ticket, fake_corrupt_page) == "STALE"
    assert (
        sql(repo, "SELECT health_state FROM storage_nodes WHERE node_id='node-0'")[0][
            "health_state"
        ]
        == "DOWN"
    )

    # Expire active_ticket so Worker can run full rejoin on node-0.
    sql(
        repo,
        """UPDATE node_health_polls SET next_poll_at=clock_timestamp()-interval '1 second',
        ticket_expires_at=clock_timestamp()-interval '1 second' WHERE node_id='node-0'""",
    )

    worker = Worker(repo, config=WorkerConfig(page_size=1))
    try:
        assert worker.poll_health(node_id="node-0") == "ACTIVE"
        # Rejoin MUST NOT release charged_bytes before CLEANUP executes DeleteAllocation.
        assert (
            sql(repo, "SELECT charged_bytes FROM node_budgets WHERE node_id='node-0'")[0][
                "charged_bytes"
            ]
            == 3
        )
        # Now let Worker execute the CLEANUP job to delete allocations and release budget.
        until(
            lambda: (
                worker.tick() is None
                and sql(repo, "SELECT state FROM files WHERE file_id=:id", id=session["file_id"])[
                    0
                ]["state"]
                == "DELETED"
            )
        )
    finally:
        worker.close()

    assert (
        sql(repo, "SELECT charged_bytes FROM node_budgets WHERE node_id='node-0'")[0][
            "charged_bytes"
        ]
        == 0
    )
    assert (
        sql(repo, "SELECT charged_bytes FROM node_budgets WHERE node_id='node-1'")[0][
            "charged_bytes"
        ]
        == 0
    )
    accounting(repo, sessions=0, claims=0, charged=0)


def test_unowned_disk_file_quarantines_node_on_restart_without_deleting_file(f2_cluster):
    repo, harness = f2_cluster
    repo.policy = Policy(chunk_size_bytes=3, replication_factor=2, min_distinct_domains=2)
    n0 = harness.add_node("node-0", domain="domain-0", budget=100, chunk_limit=10)
    harness.add_node("node-1", domain="domain-1", budget=100, chunk_limit=10)
    harness.add_node("node-2", domain="domain-2", budget=100, chunk_limit=10)

    session = make_available(repo, b"abc")
    sql(repo, "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-0'")

    n0.stop()
    (n0.root / "chunks" / "unowned-orphan.chunk").write_bytes(b"rogue-data")
    n0.restart(repo)

    quarantined_files = list((n0.root / "quarantine").iterdir())
    assert len(quarantined_files) == 1

    worker = Worker(repo, config=WorkerConfig(page_size=1, repair_scan_seconds=60.0))
    try:
        # Inventory still runs under recovery_required, then the node is quarantined.
        assert worker.poll_health(node_id="node-0") == "QUARANTINED"
        assert (
            sql(repo, "SELECT health_state FROM storage_nodes WHERE node_id='node-0'")[0][
                "health_state"
            ]
            == "QUARANTINED"
        )
        assert sql(
            repo,
            "SELECT replica_health FROM allocations WHERE node_id='node-0'",
        ) == [{"replica_health": "VERIFIED"}]
        assert quarantined_files[0].exists()
        until(
            lambda: (
                worker.tick() is None
                and len(repo.repair_scan_page()["deficits"]) == 0
                and not sql(
                    repo,
                    "SELECT job_id FROM jobs WHERE kind='REPAIR' AND state!='DONE'",
                )
            )
        )
    finally:
        worker.close()

    assert (
        read_verified_bytes(repo, session["file_id"], expected_nodes={"node-1", "node-2"}) == b"abc"
    )
    accounting(repo, sessions=0, claims=0, charged=9)


def test_inflight_delete_incomplete_tombstone_does_not_quarantine_rejoin(f2_cluster):
    repo, harness = f2_cluster
    repo.policy = Policy(chunk_size_bytes=3, replication_factor=2, min_distinct_domains=2)
    n0 = harness.add_node("node-0", domain="domain-0", budget=100, chunk_limit=10)
    harness.add_node("node-1", domain="domain-1", budget=100, chunk_limit=10)

    session = make_available(repo, b"abc")
    aid = sql(repo, "SELECT allocation_id FROM allocations WHERE node_id='node-0'")[0][
        "allocation_id"
    ]
    chunk_id = sql(repo, "SELECT chunk_id FROM allocations WHERE allocation_id=:aid", aid=aid)[0][
        "chunk_id"
    ]
    sql(repo, "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-0'")
    repo.delete_file(session["file_id"])
    sql(repo, "UPDATE allocations SET state='DELETE_PENDING' WHERE allocation_id=:aid", aid=aid)

    ticket = repo.claim_health_poll(node_id="node-0")
    page = InventoryPage(
        items=(
            InventoryItem(
                namespace_id=repo.namespace_id,
                recovery_epoch=repo.epoch,
                node_id="node-0",
                incarnation=n0.incarnation,
                allocation_id=aid,
                generation=1,
                state="DELETED",
                cleanup_complete=False,
                inventory_sequence=1,
                chunk_id=chunk_id,
                size_bytes=3,
                checksum_sha256=hashlib.sha256(b"abc").hexdigest(),
            ),
        ),
        upper_sequence=1,
        next_after_sequence=1,
        has_more=False,
    )
    assert repo.reconcile_rejoin_page(ticket, page) == "ACTIVE"
    assert (
        sql(repo, "SELECT health_state FROM storage_nodes WHERE node_id='node-0'")[0][
            "health_state"
        ]
        == "ACTIVE"
    )
    assert (
        sql(repo, "SELECT charged_bytes FROM node_budgets WHERE node_id='node-0'")[0][
            "charged_bytes"
        ]
        == 3
    )
    accounting(repo, sessions=0, claims=0, charged=6)


def test_rejoin_stored_actual_mismatch_marks_corrupt_without_quarantining_node(f2_cluster):
    repo, harness = f2_cluster
    repo.policy = Policy(chunk_size_bytes=3, replication_factor=2, min_distinct_domains=2)
    n0 = harness.add_node("node-0", domain="domain-0", budget=100, chunk_limit=10)
    harness.add_node("node-1", domain="domain-1", budget=100, chunk_limit=10)
    harness.add_node("node-2", domain="domain-2", budget=100, chunk_limit=10)

    session = make_available(repo, b"abc")
    row = sql(
        repo,
        """SELECT a.allocation_id,a.chunk_id,a.generation,p.checksum_sha256
        FROM allocations a JOIN upload_parts p USING (chunk_id)
        WHERE a.node_id='node-0'""",
    )[0]
    before_charge = sql(repo, "SELECT charged_bytes FROM node_budgets WHERE node_id='node-0'")[0][
        "charged_bytes"
    ]
    sql(repo, "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-0'")

    ticket = repo.claim_health_poll(node_id="node-0")
    page = InventoryPage(
        items=(
            InventoryItem(
                namespace_id=repo.namespace_id,
                recovery_epoch=repo.epoch,
                node_id="node-0",
                incarnation=n0.incarnation,
                allocation_id=row["allocation_id"],
                generation=row["generation"],
                state="STORED",
                cleanup_complete=False,
                inventory_sequence=1,
                chunk_id=row["chunk_id"],
                size_bytes=3,
                checksum_sha256=row["checksum_sha256"],
                actual_size=3,
                actual_hash=hashlib.sha256(b"bad").hexdigest(),
            ),
        ),
        upper_sequence=1,
        next_after_sequence=1,
        has_more=False,
    )
    assert repo.reconcile_rejoin_page(ticket, page) == "ACTIVE"
    assert (
        sql(repo, "SELECT health_state FROM storage_nodes WHERE node_id='node-0'")[0][
            "health_state"
        ]
        == "ACTIVE"
    )
    assert sql(
        repo,
        "SELECT state,replica_health FROM allocations WHERE allocation_id=:aid",
        aid=row["allocation_id"],
    )[0] == {"state": "STORED", "replica_health": "CORRUPT"}
    assert (
        sql(repo, "SELECT charged_bytes FROM node_budgets WHERE node_id='node-0'")[0][
            "charged_bytes"
        ]
        == before_charge
    )

    worker = Worker(repo, config=WorkerConfig(page_size=1, repair_scan_seconds=60.0))
    try:
        until(
            lambda: (
                worker.tick() is None
                and not sql(
                    repo,
                    "SELECT job_id FROM jobs WHERE kind='REPAIR' AND state!='DONE'",
                )
            )
        )
    finally:
        worker.close()

    assert (
        read_verified_bytes(repo, session["file_id"], expected_nodes={"node-1", "node-2"}) == b"abc"
    )
    accounting(repo, sessions=0, claims=0, charged=9)


def test_confirm_ack_keeps_observation_generation_at_health_fence(f2_cluster):
    repo, harness = f2_cluster
    repo.policy = Policy(chunk_size_bytes=3, replication_factor=2, min_distinct_domains=2)
    harness.add_node("node-0", domain="domain-0", budget=100, chunk_limit=10)
    harness.add_node("node-1", domain="domain-1", budget=100, chunk_limit=10)

    session = upload(repo, b"abc")
    part = claim(repo, session, index=0, data=b"abc")
    store_blocks(part, b"abc")
    sql(repo, "UPDATE storage_nodes SET health_generation=40")
    confirm(repo, session, part)

    rows = sql(
        repo,
        """SELECT a.observation_generation,n.health_generation
        FROM allocations a JOIN storage_nodes n USING (node_id)
        WHERE a.chunk_id=:chunk""",
        chunk=part["chunk_id"],
    )
    assert rows and all(r["observation_generation"] >= r["health_generation"] == 40 for r in rows)


def test_rejoin_page_watermark_survives_ack_rewrite_of_seen_allocation(f2_cluster):
    repo, harness = f2_cluster
    repo.policy = Policy(chunk_size_bytes=3, replication_factor=2, min_distinct_domains=2)
    harness.add_node("node-0", domain="domain-0", budget=100, chunk_limit=10)
    harness.add_node("node-1", domain="domain-1", budget=100, chunk_limit=10)

    make_available(repo, b"abcdef")
    sql(repo, "UPDATE storage_nodes SET health_state='DOWN' WHERE node_id='node-0'")
    ticket = repo.claim_health_poll(node_id="node-0")
    client = StorageClient()
    page1 = client.list_allocations(
        ticket, after_sequence=0, upper_sequence=None, limit=1, stopping=Event()
    )
    assert page1 is not None and page1.has_more and len(page1.items) == 1
    assert (
        repo.reconcile_rejoin_page(
            ticket, page1, after_sequence=0, available_bytes=1, writable=True
        )
        == "CONTINUE"
    )
    seen = page1.items[0].allocation_id
    # Production acks use GREATEST(generation, health_generation); keep the fence.
    sql(
        repo,
        """UPDATE allocations SET observation_generation=GREATEST(
            generation,
            (SELECT health_generation FROM storage_nodes WHERE node_id=allocations.node_id)
        ) WHERE allocation_id=:aid""",
        aid=seen,
    )
    page2 = client.list_allocations(
        ticket,
        after_sequence=page1.next_after_sequence,
        upper_sequence=page1.upper_sequence,
        limit=1,
        stopping=Event(),
    )
    assert page2 is not None and not page2.has_more
    assert (
        repo.reconcile_rejoin_page(
            ticket,
            page2,
            after_sequence=page1.next_after_sequence,
            upper_sequence=page1.upper_sequence,
            available_bytes=1,
            writable=True,
        )
        == "ACTIVE"
    )
    assert (
        sql(
            repo,
            "SELECT replica_health FROM allocations WHERE allocation_id=:aid",
            aid=seen,
        )[0]["replica_health"]
        == "VERIFIED"
    )
    assert (
        sql(repo, "SELECT health_state FROM storage_nodes WHERE node_id='node-0'")[0][
            "health_state"
        ]
        == "ACTIVE"
    )
