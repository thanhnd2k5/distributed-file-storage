"""Keep live-smoke mutations owned and deletion evidence strict under python -O."""

import json
from types import SimpleNamespace
from uuid import uuid4

import grpc
import pytest

from metadata.storage_client import StorageRpcError
from scripts import smoke_failure as smoke
from scripts.smoke_metadata import SmokeCheckError


@pytest.mark.parametrize("name", ["user-document.bin", "m4-smoke-not-a-uuid.bin"])
def test_unowned_manifest_cannot_reach_delete(monkeypatch, name):
    monkeypatch.setattr(smoke, "mutate", lambda *a: pytest.fail("Unowned DELETE reached HTTP"))
    record = {"file": {"file_id": str(uuid4()), "original_name": name}}
    with pytest.raises(SmokeCheckError, match="manifest-owned"):
        smoke.finish("unused", record, None)


@pytest.mark.parametrize("status", [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED])
def test_absence_evidence_rejects_transport_failure(monkeypatch, status):
    class Client:
        def __init__(self, settings):
            self.closed = False

        def start(self):
            pass

        def close(self):
            self.closed = True

        def get_chunk(self, *args):
            raise StorageRpcError("GetChunk", "node-1", status, 1)

    client = Client(None)
    monkeypatch.setattr(smoke, "StorageClient", lambda settings: client)
    with pytest.raises(SmokeCheckError):
        smoke.actual(
            None,
            [
                {
                    "nodes": ["node-1"],
                    "chunk_id": str(uuid4()),
                    "size_bytes": 1,
                    "checksum_sha256": "unused",
                }
            ],
            absent=True,
        )
    assert client.closed


def test_zero_progress_cursor_does_not_claim_complete(monkeypatch):
    record = {
        "file": {"file_id": str(uuid4()), "original_name": "m4-smoke-" + uuid4().hex + ".bin"}
    }
    monkeypatch.setattr(
        smoke,
        "mutate",
        lambda *a: (
            200,
            {
                "checked_chunks": 0,
                "repaired_replicas": 0,
                "remaining_chunks": 3,
                "next_after": None,
                "results": [],
            },
        ),
    )
    with pytest.raises(SmokeCheckError, match="no progress"):
        smoke.scan("unused", record)


@pytest.mark.parametrize("label", ["repair", "legacy"])
def test_finish_accepts_partial_create_checkpoint(tmp_path, monkeypatch, label):
    record = {
        "file": {"file_id": str(uuid4()), "original_name": "m4-smoke-" + uuid4().hex + ".bin"}
    }
    state = {"version": "M4-P6", "files": {label: record}, "evidence": {}}
    path = tmp_path / "partial.json"
    path.write_text(json.dumps(state), encoding="utf-8")
    monkeypatch.setattr(smoke, "MetadataSettings", lambda: SimpleNamespace(replication_factor=3))
    finished = []

    def finish(base, item, settings, *, checkpoint):
        checkpoint()
        finished.append(item)

    monkeypatch.setattr(smoke, "finish", finish)
    monkeypatch.setattr("sys.argv", ["smoke_failure.py", "finish", "--manifest", str(path)])
    smoke.main()
    assert finished == [record]


@pytest.mark.parametrize("already_recorded", [False, True])
def test_failed_upload_id_is_recovered_once(already_recorded):
    record = {
        "file": {"file_id": str(uuid4()), "original_name": "m4-smoke-" + uuid4().hex + ".bin"}
    }
    state = {
        "files": {"repair": record} if already_recorded else {},
        "upload_failure": {
            "name": record["file"]["original_name"],
            "error": {"error": {"details": {"file_id": record["file"]["file_id"]}}},
        },
    }
    smoke.recover_failed_upload(state)
    smoke.recover_failed_upload(state)
    assert list(state["files"].values()) == [record]
