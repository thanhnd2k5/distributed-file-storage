"""Interrupted manifests recover against real Metadata, PostgreSQL and Storage."""

import hashlib
import io
import json
import urllib.error
from uuid import UUID, uuid4

import pytest
from test_metadata_delete import PREFIX, snapshot
from test_metadata_delete import api as production_api
from test_metadata_delete import cluster as production_cluster

from metadata.models import Chunk, File
from scripts import smoke_failure as smoke
from scripts.smoke_metadata import SmokeCheckError

api = production_api
cluster = production_cluster


def fixture_record(api, data=b"recovery payload"):
    name = "m4-smoke-" + uuid4().hex + ".bin"
    response = api[0].post(PREFIX, files={"file": (name, data)})
    assert response.status_code == 201, response.text
    return {"file": {"file_id": response.json()["file_id"], "original_name": name}}


@pytest.mark.parametrize(
    "mode",
    ["partial", "already-deleted", "failed-upload", "failed-empty", "failed-unattempted"],
)
def test_finish_recovers_history_and_confirms_physical_cleanup(api, tmp_path, monkeypatch, mode):
    empty = mode in {"failed-empty", "failed-unattempted"}
    expected_chunks = 0 if mode == "failed-empty" else 1
    expected_mappings = 0 if empty else 2
    record = fixture_record(api, b"" if empty else b"recovery payload")
    file_id = UUID(record["file"]["file_id"])
    if mode == "already-deleted":
        assert api[0].delete(f"{PREFIX}/{file_id}").status_code == 200
        assert api[0].get(f"{PREFIX}/{file_id}").status_code == 404
    state = {"version": "M4-P6", "files": {"repair": record}, "evidence": {}}
    if mode.startswith("failed-"):
        with api[2].begin() as session:
            file = session.get(File, file_id)
            file.status = "FAILED"
            if mode == "failed-unattempted":
                file.size_bytes, file.total_chunks, file.checksum_sha256 = 1, 1, None
                session.add(
                    Chunk(
                        id=uuid4(),
                        file_id=file_id,
                        chunk_index=0,
                        size_bytes=1,
                        checksum_sha256=hashlib.sha256(b"x").hexdigest(),
                    )
                )
        state["files"] = {}
        state["upload_failure"] = {
            "name": record["file"]["original_name"],
            "error": {"error": {"details": {"file_id": str(file_id)}}},
        }
    path = tmp_path / "partial.json"
    path.write_text(json.dumps(state), encoding="utf-8")
    monkeypatch.setattr(smoke, "MetadataSettings", lambda: api[4])
    # Use the fixture's unique schema; production reads/writes are exercised unchanged.
    monkeypatch.setattr(smoke, "create_engine", lambda *a, **k: api[2].kw["bind"])

    def mutate(base, endpoint, method, body=None):
        response = api[0].request(method, "/api/v1" + endpoint, json=body)
        if response.status_code >= 400:
            raise urllib.error.HTTPError(
                endpoint, response.status_code, "HTTP failure", {}, io.BytesIO(response.content)
            )
        if method == "DELETE":
            persisted = json.loads(path.read_text(encoding="utf-8"))
            assert len(next(iter(persisted["files"].values()))["chunks"]) == expected_chunks
        return response.status_code, response.json()

    monkeypatch.setattr(smoke, "mutate", mutate)
    monkeypatch.setattr("sys.argv", ["smoke_failure.py", "finish", "--manifest", str(path)])
    smoke.main()
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert len(saved["files"]) == 1
    recovered = next(iter(saved["files"].values()))
    assert len(recovered["chunks"]) == expected_chunks
    assert recovered["terminal"]["delete"]["status"] == "DELETED"
    assert recovered["terminal"]["absent"]
    assert recovered["terminal"]["rpc_checked_mappings"] == expected_mappings
    assert sum(server.service.store.used_bytes for server in api[3]) == 0
    assert snapshot(api, file_id)[0].status == "DELETED"


@pytest.mark.parametrize("mismatch", ["name", "missing-id"])
def test_identity_must_match_durable_metadata_before_delete(api, monkeypatch, mismatch):
    record = fixture_record(api)
    original_id = UUID(record["file"]["file_id"])
    if mismatch == "name":
        record["file"]["original_name"] = "m4-smoke-" + uuid4().hex + ".bin"
    else:
        record["file"]["file_id"] = str(uuid4())
    monkeypatch.setattr(smoke, "create_engine", lambda *a, **k: api[2].kw["bind"])
    monkeypatch.setattr(
        smoke, "mutate", lambda *a: pytest.fail("Unverified identity reached DELETE")
    )
    with pytest.raises(SmokeCheckError, match="Ownership mismatch|missing from durable"):
        smoke.finish("unused", record, api[4])
    assert snapshot(api, original_id)[0].status == "AVAILABLE"
    assert sum(server.service.store.used_bytes for server in api[3]) > 0
