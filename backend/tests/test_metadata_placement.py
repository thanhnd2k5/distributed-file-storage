from dataclasses import FrozenInstanceError, replace
from itertools import permutations

import pytest

from metadata.placement import PlacementNode, select_destination

SIZE = 100


def node(node_id, domain="dev_host", **changes):
    return PlacementNode(
        **(
            dict(
                node_id=node_id,
                failure_domain=domain,
                enabled=True,
                status="ACTIVE",
                available_bytes=1000,
                used_bytes=0,
            )
            | changes
        )
    )


def choose_replicas(nodes, rf, allocation=None):
    """Simulate successful placement acks; actual Store/ack is owned by P2/P3."""
    allocation = allocation if allocation is not None else {}
    verified = []
    for _ in range(rf):
        chosen = select_destination(
            nodes, SIZE, rf, verified_replicas=verified, allocated_bytes=allocation
        )
        if chosen is None:
            break
        verified.append(chosen)
        allocation[chosen.node_id] = allocation.get(chosen.node_id, 0) + SIZE
    return verified


@pytest.mark.parametrize("rf", [1, 2, 3])
def test_rf_selects_distinct_nodes_and_stops_when_satisfied(rf):
    nodes = [node(f"node-{i}") for i in (1, 2, 3)]
    selected = choose_replicas(nodes, rf)
    assert [item.node_id for item in selected] == [f"node-{i}" for i in range(1, rf + 1)]
    assert select_destination(nodes, SIZE, rf, verified_replicas=selected) is None


@pytest.mark.parametrize("order", list(permutations((1, 2, 3))))
def test_ties_break_by_node_id_independent_of_snapshot_order(order):
    nodes = [node(f"node-{i}") for i in order]
    assert select_destination(nodes, SIZE, 2).node_id == "node-1"


def test_different_domain_has_priority_over_allocation_and_used_bytes():
    first = node("node-1", "A")
    same_domain = node("node-2", "A")
    other_domain = node("node-3", "B", used_bytes=999)
    chosen = select_destination(
        [first, same_domain, other_domain],
        SIZE,
        2,
        verified_replicas=[first],
        allocated_bytes={"node-3": 1000000},
    )
    assert chosen == other_domain


def test_a_b_b_topology_keeps_a_replica_on_each_domain():
    nodes = [
        node("node-1", "A", used_bytes=900),
        node("node-2", "B"),
        node("node-3", "B", used_bytes=1),
    ]
    replicas = choose_replicas(nodes, 2)
    assert [item.node_id for item in replicas] == ["node-2", "node-1"]
    assert {item.failure_domain for item in replicas} == {"A", "B"}


def test_allocation_before_used_bytes_and_used_bytes_before_id():
    nodes = [
        node("node-1", used_bytes=0),
        node("node-2", used_bytes=50),
        node("node-3", used_bytes=20),
    ]
    assert select_destination(nodes, SIZE, 2).node_id == "node-1"
    assert select_destination(nodes, SIZE, 2, allocated_bytes={"node-1": SIZE}).node_id == "node-3"
    assert (
        select_destination(nodes, SIZE, 2, allocated_bytes={"node-1": SIZE, "node-3": SIZE}).node_id
        == "node-2"
    )


def test_allocation_persists_across_chunks_but_new_operation_starts_fresh():
    nodes = [node(f"node-{i}") for i in (1, 2, 3)]
    allocation = {}
    placements = [choose_replicas(nodes, 2, allocation) for _ in range(3)]
    assert [[item.node_id for item in replicas] for replicas in placements] == [
        ["node-1", "node-2"],
        ["node-3", "node-1"],
        ["node-2", "node-3"],
    ]
    assert allocation == {"node-1": 2 * SIZE, "node-2": 2 * SIZE, "node-3": 2 * SIZE}
    assert [item.node_id for item in choose_replicas(nodes, 2)] == ["node-1", "node-2"]


@pytest.mark.parametrize(
    "changes",
    [
        {"enabled": False},
        {"status": "SUSPECTED"},
        {"status": "DOWN"},
        {"available_bytes": None},
        {"available_bytes": SIZE - 1},
        {"available_bytes": 0},
        {"used_bytes": None},
        {"used_bytes": -1},
    ],
)
def test_ineligible_snapshots_are_never_selected(changes):
    ineligible = node("node-1", "B", **changes)
    eligible = node("node-2", "A", used_bytes=1000)
    assert select_destination([ineligible, eligible], SIZE, 2) == eligible
    assert select_destination([ineligible], SIZE, 2) is None


def test_free_space_equal_to_chunk_size_is_eligible_and_is_not_a_reservation():
    available = node("node-1", available_bytes=SIZE)
    allocation = {"node-1": 10000}
    assert select_destination([available], SIZE, 2, allocated_bytes=allocation) == available
    assert available.available_bytes == SIZE and allocation == {"node-1": 10000}


def test_failed_node_is_excluded_even_if_snapshot_remains_active():
    nodes = [node("node-1", "A"), node("node-2", "B"), node("node-3", "B")]
    verified = [nodes[0]]
    failures = {"node-2"}
    assert (
        select_destination(nodes, SIZE, 2, verified_replicas=verified, failed_node_ids=failures)
        == nodes[2]
    )
    failures.add("node-3")
    assert (
        select_destination(nodes, SIZE, 2, verified_replicas=verified, failed_node_ids=failures)
        is None
    )
    assert nodes[1].status == nodes[2].status == "ACTIVE"


def test_one_domain_fallback_warns_and_does_not_reuse_a_replica_node(caplog):
    nodes = [node("node-1"), node("node-2")]
    chosen = select_destination(nodes, SIZE, 2, verified_replicas=[nodes[0]])
    assert chosen == nodes[1]
    assert "shares failure domain dev_host" in caplog.text


def test_unusable_other_domain_does_not_prevent_same_domain_fallback(caplog):
    first = node("node-1", "A")
    second = node("node-2", "A")
    offline = node("node-3", "B", status="DOWN")
    assert (
        select_destination([first, second, offline], SIZE, 2, verified_replicas=[first]) == second
    )
    assert "shares failure domain A" in caplog.text


def test_domain_preference_preserves_existing_domains_not_in_candidate_snapshots(caplog):
    existing = node("source", "A")
    destinations = [node("node-1", "A"), node("node-2", "B")]
    assert (
        select_destination(destinations, SIZE, 2, verified_replicas=[existing]) == destinations[1]
    )
    assert not caplog.records


def test_rf_count_uses_distinct_replica_nodes_and_does_not_rebalance_when_met(caplog):
    first, second, other = node("node-1", "A"), node("node-2", "A"), node("node-3", "B")
    assert (
        select_destination([first, second, other], SIZE, 2, verified_replicas=[first, first])
        == other
    )
    assert (
        select_destination([first, second, other], SIZE, 2, verified_replicas=[first, second])
        is None
    )
    assert not caplog.records


def test_insufficient_nodes_returns_no_destination_instead_of_reusing_one():
    single = node("node-1")
    selected = choose_replicas([single], 2)
    assert selected == [single]
    assert select_destination([], SIZE, 2) is None
    assert len(choose_replicas([single, node("node-2")], 3)) == 2


def test_selection_does_not_account_unacknowledged_attempts_or_mutate_snapshots():
    nodes = [node("node-1"), node("node-2")]
    before = [replace(item) for item in nodes]
    allocation, failures, verified = {}, set(), []
    for _ in range(2):
        assert (
            select_destination(
                nodes,
                SIZE,
                2,
                allocated_bytes=allocation,
                failed_node_ids=failures,
                verified_replicas=verified,
            )
            == nodes[0]
        )
    assert allocation == {} and failures == set() and verified == [] and nodes == before
    with pytest.raises(FrozenInstanceError):
        nodes[0].status = "DOWN"


@pytest.mark.parametrize("size, rf", [(0, 2), (-1, 2), (SIZE, 0), (SIZE, -1)])
def test_invalid_placement_limits_fail_clearly(size, rf):
    with pytest.raises(ValueError):
        select_destination([node("node-1")], size, rf)
