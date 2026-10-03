import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy.schema import DropSchema

from metadata.config import MetadataSettings
from scripts import smoke_metadata as smoke


@pytest.fixture
def api(monkeypatch):
    settings = MetadataSettings(
        _env_file=None,
        database_url="postgresql+psycopg://test:test@127.0.0.1:1/test",
        storage_nodes_json=[
            {
                "node_id": f"node-{i}",
                "host": f"storage-{i}",
                "port": 50050 + i,
                "failure_domain": "test",
            }
            for i in (1, 2)
        ],
    )
    stamp = datetime.now(UTC).isoformat()
    nodes = [
        {
            **config.model_dump(),
            "enabled": True,
            "status": "ACTIVE",
            "last_success_at": stamp,
            "last_error": None,
            "capacity_bytes": 1000,
            "available_bytes": 900,
            "used_bytes": 0,
        }
        for config in settings.storage_nodes_json
    ]
    nodes.append(
        {
            **nodes[0],
            "node_id": "retired",
            "failure_domain": "old-domain",
            "enabled": False,
            "status": "DOWN",
            "last_success_at": None,
            "capacity_bytes": None,
            "available_bytes": None,
            "used_bytes": None,
        }
    )
    cluster = {
        "nodes": {"active": 2, "suspected": 0, "down": 0, "disabled": 1},
        "configured_failure_domains": 1,
        "active_failure_domains": 1,
        "chunk_size_bytes": settings.chunk_size_bytes,
        "default_replication_factor": settings.replication_factor,
        "max_file_size_bytes": settings.max_file_size_bytes,
    }
    responses = {
        "/health/live": {"status": "LIVE"},
        "/health/ready": {"status": "READY"},
        "/nodes": {"items": nodes},
        "/cluster": cluster,
    }
    monkeypatch.setattr(smoke, "get", lambda path: responses[path])
    return settings, nodes, cluster


@pytest.mark.parametrize("status", ["ACTIVE", "SUSPECTED", "DOWN"])
def test_baseline_accepts_disabled_history_without_requiring_metrics(
    api, monkeypatch, capsys, status
):
    settings, nodes, _ = api
    nodes[-1]["status"] = status
    after = datetime.now(UTC) - timedelta(seconds=1)

    def no_wait(seconds):
        raise AssertionError("Disabled history must not make the baseline wait")

    monkeypatch.setattr(smoke, "sleep", no_wait)
    smoke.wait_baseline(settings, after)
    result = json.loads(capsys.readouterr().out)
    assert result["cluster"]["nodes"]["disabled"] == 1
    assert len(result["nodes"]) == 3


@pytest.mark.parametrize("change", ["missing", "disabled", "extra", "endpoint"])
def test_baseline_still_rejects_invalid_enabled_registry(api, change):
    settings, nodes, _ = api
    if change == "missing":
        nodes.pop(0)
    elif change == "disabled":
        nodes[0]["enabled"] = False
    elif change == "extra":
        nodes[-1]["enabled"] = True
    else:
        nodes[0]["port"] = 1
    with pytest.raises(smoke.SmokeCheckError):
        smoke.observe(settings)


def test_baseline_rejects_wrong_disabled_count(api):
    settings, _, cluster = api
    cluster["nodes"]["disabled"] = 0
    with pytest.raises(smoke.SmokeCheckError, match="node counts"):
        smoke.observe(settings)


def test_down_observation_keeps_disabled_count_and_history(api, capsys):
    settings, nodes, cluster = api
    nodes[1].update(
        status="DOWN",
        last_error="UNAVAILABLE",
        last_success_at=(datetime.now(UTC) - timedelta(seconds=11)).isoformat(),
    )
    cluster["nodes"].update(active=1, down=1)
    smoke.wait_down(settings, "node-2")
    assert "down passed" in capsys.readouterr().out


def test_down_allows_cluster_get_to_straddle_transition_across_domains(api):
    settings, nodes, cluster = api
    settings.storage_nodes_json[1].failure_domain = "second"
    nodes[1]["failure_domain"] = "second"
    cluster["configured_failure_domains"] = 2
    # Node GET sees ACTIVE; next GET sees the node's committed SUSPECTED transition.
    cluster["nodes"].update(active=1, suspected=1)
    smoke.observe(settings, down_node="node-2")


@pytest.mark.parametrize(
    "schema", [None, "", "public", "m2_smoke_" + "a" * 31, "m2_smoke_" + "a" * 32 + "_other"]
)
def test_unsafe_schema_is_rejected_before_engine_creation(monkeypatch, schema):
    engine = MagicMock()
    monkeypatch.setattr(smoke, "create_engine", engine)
    with pytest.raises(smoke.SmokeCheckError, match="Unsafe fixture schema"):
        smoke.fixture_command(
            SimpleNamespace(database_url="unused"),
            SimpleNamespace(command="fixture-drop", schema=schema),
        )
    engine.assert_not_called()


def test_valid_drop_uses_only_requested_fixture_schema(monkeypatch):
    engine = MagicMock()
    monkeypatch.setattr(smoke, "create_engine", lambda *args, **kwargs: engine)
    schema = "m2_smoke_" + "a" * 32
    smoke.fixture_command(
        SimpleNamespace(database_url="unused"),
        SimpleNamespace(command="fixture-drop", schema=schema),
    )
    statement = engine.begin.return_value.__enter__.return_value.execute.call_args.args[0]
    assert isinstance(statement, DropSchema) and statement.element == schema
    engine.dispose.assert_called_once()


@pytest.mark.parametrize("scenario", ["schema", "digest", "live"])
def test_optimized_python_keeps_guard_persistence_and_http_checks(scenario):
    harness = """
import sys
sys.path.insert(0, 'generated')
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from scripts import smoke_metadata as smoke
mode = sys.argv[1]
try:
    if mode == 'live':
        with patch.object(smoke, 'get', return_value={'status': 'WRONG'}):
            smoke.observe(None)
    elif mode == 'schema':
        with patch.object(smoke, 'create_engine',
                          side_effect=RuntimeError('DB must not be touched')):
            smoke.fixture_command(SimpleNamespace(database_url='unused'),
                                  SimpleNamespace(command='fixture-drop', schema='public'))
    else:
        with patch.object(smoke, 'create_engine', return_value=MagicMock()), \
             patch.object(smoke, 'snapshot', return_value={'files': [{'id': 'changed'}]}):
            smoke.fixture_command(SimpleNamespace(database_url='unused'),
                SimpleNamespace(command='fixture-verify',
                                schema='m2_smoke_' + 'a'*32, digest='0'*64))
except smoke.SmokeCheckError:
    print('CHECK_REJECTED')
else:
    raise RuntimeError('optimized Python skipped a required smoke check')
"""
    result = subprocess.run(
        [sys.executable, "-O", "-c", harness, scenario], capture_output=True, text=True, timeout=15
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "CHECK_REJECTED"
