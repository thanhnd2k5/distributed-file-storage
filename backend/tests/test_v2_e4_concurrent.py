"""Sixteen simultaneous byte streams with delete and durable cleanup."""

import hashlib
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
from test_v2_c3 import until
from test_v2_d5 import CHUNK, atomic, download, drained, local_rows, metrics, path, source, uploaded
from test_v2_d5 import d5 as base_fixture
from test_v2_metadata import foundation as foundation
from test_v2_metadata import sql


@pytest.fixture
def workload(foundation, tmp_path, request, monkeypatch):
    monkeypatch.setenv("V2_D5_DOWNLOAD_STREAMS", "8")
    monkeypatch.setenv("V2_D5_DOWNLOAD_BUFFERED_BYTES", str(8 * (CHUNK + 2 * 262144)))
    yield from base_fixture.__wrapped__(foundation, tmp_path, request)


def test_sixteen_streams_delete_and_cleanup(workload):
    repo, roots, control, origin, services, report = workload
    data = source(2 * CHUNK + 17)
    session = uploaded(workload, data)
    chunk = str(sql(repo, "SELECT chunk_id FROM manifest_parts WHERE part_index=0")[0]["chunk_id"])
    (control / "hold-eof").touch()
    for index in range(2):
        atomic(control / f"storage-{index}-fault.json", dict(mode="hold_eof", chunk_id=chunk))
    (control / "phase").write_text("sixteen-streams")
    started = time.monotonic()
    with ThreadPoolExecutor(16) as pool:
        futures = [pool.submit(download, origin, session) for _ in range(16)]
        try:
            until(lambda: all(row["downloads"] == 8 for row in metrics(services).values()), 25)
            until(
                lambda: len(sql(repo, "SELECT * FROM download_leases WHERE state='ACTIVE'")) == 16,
                20,
            )
            assert all(not future.done() for future in futures)
            rejected = [httpx.get(origin + path(session), timeout=5) for _ in range(4)]
            assert all(r.status_code == 429 and int(r.headers["Retry-After"]) > 0 for r in rejected)
            assert len(sql(repo, "SELECT * FROM download_leases")) == 16
            response = httpx.delete(origin + f"/api/v2/files/{session['file_id']}", timeout=10)
            assert response.status_code == 202 and response.json()["cleanup_pending"]
            assert sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 2 * len(
                data
            )
            until(
                lambda: bool(
                    sql(
                        repo,
                        "SELECT 1 FROM jobs WHERE kind='CLEANUP' "
                        "AND last_error='READ_LEASE_ACTIVE'",
                    )
                ),
                15,
            )
            for label, service in services.items():
                if label.startswith("api"):
                    assert (
                        httpx.get(service.url + f"/api/v2/files/{session['file_id']}").status_code
                        == 404
                    )
        finally:
            (control / "hold-eof").unlink(missing_ok=True)
        results = [future.result(timeout=40) for future in futures]
    assert all(
        r["bytes"] == len(data) and r["sha256"] == hashlib.sha256(data).hexdigest() for r in results
    )
    assert len({r["upstream"] for r in results}) == 2
    drained(workload)
    sql(
        repo,
        "UPDATE jobs SET run_after=clock_timestamp() WHERE kind='CLEANUP' "
        "AND state IN ('READY','RETRY')",
    )
    until(lambda: sql(repo, "SELECT state FROM files")[0]["state"] == "DELETED", 20)
    assert sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 0
    assert not sql(repo, "SELECT 1 FROM allocations WHERE state!='RELEASED'")
    assert not sql(repo, "SELECT 1 FROM write_attempts WHERE state!='CLOSED'")
    for root in roots.values():
        assert not list((root / "chunks").iterdir())
        assert all(
            row["state"] == "DELETED" and row["cleanup_complete"] for row in local_rows(root)
        )
    report["measurements"].update(
        streams=16,
        file_bytes=len(data),
        downloads=results,
        rejections=4,
        elapsed_seconds=time.monotonic() - started,
    )
