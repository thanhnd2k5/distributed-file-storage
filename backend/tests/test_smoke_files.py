import hashlib
from email.message import Message

import pytest

from scripts import smoke_files as smoke


@pytest.mark.parametrize("down_node", [None, "node-2"])
def test_one_configured_domain_is_not_degraded_at_rf_two(monkeypatch, down_node):
    data = smoke.payload(12)
    summary = {
        "file_id": "fixture",
        "size_bytes": 12,
        "chunk_size_bytes": 2,
        "total_chunks": 6,
        "replication_factor": 2,
        "checksum_sha256": hashlib.sha256(data).hexdigest(),
    }
    pairs = [("node-1", "node-2"), ("node-1", "node-3"), ("node-2", "node-3")] * 2
    chunks = []
    under = 4 if down_node else 0
    for index, pair in enumerate(pairs):
        live = 1 if down_node in pair else 2
        chunks.append(
            {
                "chunk_id": str(index),
                "chunk_index": index,
                "size_bytes": 2,
                "checksum_sha256": hashlib.sha256(data[2 * index : 2 * index + 2]).hexdigest(),
                "live_replica_count": live,
                "live_failure_domain_count": 1,
                "domain_degraded": False,
                "state": "UNDER_REPLICATED" if live == 1 else "AVAILABLE",
                "replicas": [
                    {
                        "node_id": node,
                        "failure_domain": "dev_host",
                        "status": "VERIFIED",
                        "cleanup_pending": False,
                        "node_status": "DOWN" if node == down_node else "ACTIVE",
                    }
                    for node in pair
                ],
            }
        )
    responses = {
        "/files/fixture": {
            **summary,
            "known_readable": True,
            "unavailable_chunks": 0,
            "under_replicated_chunks": under,
            "domain_degraded_chunks": 0,
            "over_replicated_chunks": 0,
            "cleanup_pending_replicas": 0,
        },
        "/files/fixture/chunks": {"items": chunks},
        "/files?limit=100&offset=0": {"items": [summary], "total": 1},
        "/cluster": {
            "configured_failure_domains": 1,
            "files_available": 1,
            "under_replicated_chunks": under,
        },
    }
    headers = Message()
    headers["Content-Type"] = "application/octet-stream"
    headers["Content-Length"] = "12"
    headers["X-File-Checksum-SHA256"] = summary["checksum_sha256"]
    monkeypatch.setattr(smoke, "get", lambda base, path: responses[path])
    monkeypatch.setattr(smoke, "request", lambda base, path: (200, headers, data))
    result = smoke.verify("unused", {"file": summary, "chunks": smoke.mapping(chunks)}, down_node)
    assert result["domain_degraded_chunks"] == 0 and result["under_replicated_chunks"] == under
