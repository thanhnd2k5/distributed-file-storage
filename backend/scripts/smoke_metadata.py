"""Read-only REST smoke by default; explicit fixture commands use a UUID schema only."""

import argparse
import json
import re
import urllib.error
import urllib.request
from datetime import UTC, datetime
from time import monotonic, sleep

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from metadata.config import MetadataSettings
from metadata.db import Base
from scripts.metadata_fixture import digest, seed, snapshot

BASE = "http://127.0.0.1:8000/api/v1"


class SmokeCheckError(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise SmokeCheckError(message)


def get(path):
    with urllib.request.urlopen(BASE + path, timeout=3) as response:
        require(response.status == 200, f"{path}: expected HTTP 200")
        return json.load(response)


def configured_nodes(nodes, settings):
    configs = sorted(settings.storage_nodes_json, key=lambda node: node.node_id)
    enabled = [node for node in nodes if node["enabled"]]
    require(
        [node["node_id"] for node in enabled] == [config.node_id for config in configs],
        "Enabled node IDs do not match the configured registry",
    )
    return enabled, configs


def observe(settings, down_node=None):
    require(get("/health/live") == {"status": "LIVE"}, "Metadata is not live")
    require(get("/health/ready") == {"status": "READY"}, "Metadata is not ready")
    nodes = get("/nodes")["items"]
    enabled, configs = configured_nodes(nodes, settings)
    for node, config in zip(enabled, configs, strict=True):
        for field in ("node_id", "host", "port", "failure_domain"):
            require(node[field] == getattr(config, field), f"{config.node_id}: changed {field}")
        require(node["last_success_at"] is not None, f"{config.node_id}: missing health success")
        for field in ("capacity_bytes", "available_bytes", "used_bytes"):
            require(
                type(node[field]) is int and node[field] >= 0, f"{config.node_id}: invalid {field}"
            )
        if node["node_id"] != down_node:
            require(node["status"] == "ACTIVE", f"{config.node_id}: expected ACTIVE")
            require(node["last_error"] is None, f"{config.node_id}: health error not cleared")
    cluster = get("/cluster")
    domains = {config.failure_domain for config in configs}
    active_domains = {node["failure_domain"] for node in enabled if node["status"] == "ACTIVE"}
    require(
        cluster["configured_failure_domains"] == len(domains), "Configured domain count changed"
    )
    expected = {"active": 0, "suspected": 0, "down": 0, "disabled": len(nodes) - len(enabled)}
    for node in enabled:
        expected[node["status"].lower()] += 1
    if down_node is None:
        require(cluster["nodes"] == expected, "Cluster node counts do not match the baseline")
        require(
            cluster["active_failure_domains"] == len(active_domains),
            "Active domain count does not match the baseline",
        )
    else:
        # Separate GETs can straddle a health commit; compare only stable invariants.
        counts = cluster["nodes"]
        require(
            sum(counts.values()) == len(nodes) and counts["disabled"] == expected["disabled"],
            "Cluster total/disabled node counts changed",
        )
        require(counts["active"] >= len(configs) - 1, "Another configured node is not ACTIVE")
        other_domains = {node["failure_domain"] for node in enabled if node["node_id"] != down_node}
        require(
            len(other_domains) <= cluster["active_failure_domains"] <= len(domains),
            "Active domain count changed beyond the stopped node",
        )
    require(cluster["chunk_size_bytes"] == settings.chunk_size_bytes, "Chunk size changed")
    require(cluster["default_replication_factor"] == settings.replication_factor, "RF changed")
    require(
        cluster["max_file_size_bytes"] == settings.max_file_size_bytes, "File size limit changed"
    )
    return nodes, cluster


def wait_baseline(settings, after=None):
    deadline = monotonic() + 40
    while monotonic() < deadline:
        # Connection/503 is expected only while recovering a service we just restarted.
        try:
            get("/health/ready")
            nodes = get("/nodes")["items"]
        except (urllib.error.URLError, TimeoutError):
            sleep(0.5)
            continue
        enabled, _ = configured_nodes(nodes, settings)
        if all(node["status"] == "ACTIVE" for node in enabled) and (
            after is None
            or all(datetime.fromisoformat(node["last_success_at"]) > after for node in enabled)
        ):
            nodes, cluster = observe(settings)
            print(json.dumps({"result": "baseline passed", "nodes": nodes, "cluster": cluster}))
            return
        sleep(0.5)
    raise SmokeCheckError("Metadata did not recover ready/ACTIVE with fresh snapshots within 40s")


def wait_down(settings, node_id):
    require(
        node_id in {config.node_id for config in settings.storage_nodes_json},
        f"{node_id} is not configured",
    )
    deadline = (
        monotonic()
        + settings.node_down_after_seconds
        + 2 * (settings.health_interval_seconds + settings.health_rpc_timeout_seconds)
        + 5
    )
    previous_status = None
    while monotonic() < deadline:
        nodes, _ = observe(settings, down_node=node_id)
        target = next(node for node in nodes if node["node_id"] == node_id)
        if target["status"] != previous_status:
            age = (
                datetime.now(UTC) - datetime.fromisoformat(target["last_success_at"])
            ).total_seconds()
            print(
                f"{node_id}: {target['status']}, last-success age={age:.2f}s, "
                f"error={target['last_error']}",
                flush=True,
            )
            previous_status = target["status"]
        if target["status"] == "DOWN":
            age = (
                datetime.now(UTC) - datetime.fromisoformat(target["last_success_at"])
            ).total_seconds()
            require(
                target["last_error"] in {"UNAVAILABLE", "DEADLINE_EXCEEDED"},
                f"{node_id}: unexpected down reason",
            )
            require(age >= settings.node_down_after_seconds, f"{node_id}: DOWN before threshold")
            print("down passed: other nodes ACTIVE; live/ready remained HTTP 200")
            return
        sleep(0.5)
    raise SmokeCheckError(f"{node_id} did not become DOWN within the bounded observation window")


def fixture_command(settings, args):
    require(re.fullmatch(r"m2_smoke_[0-9a-f]{32}", args.schema or ""), "Unsafe fixture schema name")
    require(
        args.command in {"fixture-seed", "fixture-verify", "fixture-drop"},
        "Unknown fixture command",
    )
    engine = create_engine(settings.database_url, connect_args={"connect_timeout": 5})
    scoped = engine.execution_options(schema_translate_map={None: args.schema})
    sessions = sessionmaker(bind=scoped, expire_on_commit=False)
    try:
        if args.command == "fixture-seed":
            with engine.begin() as connection:
                connection.execute(CreateSchema(args.schema))
            Base.metadata.create_all(scoped)
            seed(sessions, settings.storage_nodes_json)
            print(json.dumps({"schema": args.schema, "digest": digest(snapshot(sessions))}))
        elif args.command == "fixture-verify":
            state = snapshot(sessions)  # No schema/table creation after DB restart.
            require(
                digest(state) == args.digest, "Fixture columns changed across PostgreSQL restart"
            )
            print(
                json.dumps(
                    {
                        "result": "DB fixture persistence passed",
                        "rows": {table: len(rows) for table, rows in state.items()},
                        "digest": args.digest,
                    }
                )
            )
        else:
            with engine.begin() as connection:
                connection.execute(DropSchema(args.schema, cascade=True, if_exists=True))
            print(f"Dropped only fixture schema {args.schema}")
    finally:
        engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        nargs="?",
        default="baseline",
        choices=("baseline", "down", "fixture-seed", "fixture-verify", "fixture-drop"),
    )
    parser.add_argument("--node", default="node-2")
    parser.add_argument("--after", type=datetime.fromisoformat)
    parser.add_argument("--schema")
    parser.add_argument("--digest")
    args = parser.parse_args()
    if args.command.startswith("fixture-") and args.schema is None:
        parser.error("fixture commands require --schema")
    if args.command == "fixture-verify" and args.digest is None:
        parser.error("fixture-verify requires --digest")
    settings = MetadataSettings()
    if args.command == "down":
        wait_down(settings, args.node)
    elif args.command == "baseline":
        wait_baseline(settings, args.after)
    else:
        fixture_command(settings, args)


if __name__ == "__main__":
    main()
