"""Bounded multi-process session workload; uses D5's actual ingress and services."""

import hashlib
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import httpx
import pytest
from test_v2_d5 import d5 as d5
from test_v2_d5 import drained, finish, local_rows, source
from test_v2_e2 import accounting
from test_v2_metadata import foundation as foundation
from test_v2_metadata import sql


@pytest.mark.parametrize("count", [1, 4, 16])
def test_active_sessions_and_bounded_concurrent_transfers(d5, count):
    repo, roots, control, origin, services, report = d5
    size = 65536
    data = [source(size, index) for index in range(count)]
    if count == 16:
        # The shared foundation fixture defaults to eight sessions; E4 needs
        # sixteen simultaneous owners in this test's disposable schema.
        sql(
            repo,
            "UPDATE admission_counters SET limit_count=16 WHERE resource_key='upload_sessions'",
        )
    start = time.monotonic()
    (control / "phase").write_text(f"sessions-{count}")

    def create(index):
        response = httpx.post(
            origin + "/api/v2/uploads",
            headers={"Idempotency-Key": str(uuid4())},
            json={
                "original_name": f"e4-{index}.bin",
                "size_bytes": size,
                "expected_sha256": hashlib.sha256(data[index]).hexdigest(),
            },
            timeout=15,
        )
        assert response.status_code == 201, response.text
        return response.json()

    with ThreadPoolExecutor(max_workers=4) as pool:
        sessions = list(pool.map(create, range(count)))
    assert len({session["upload_id"] for session in sessions}) == count
    accounting(repo, sessions=count, claims=0, charged=0)

    def transfer(index):
        session, payload = sessions[index], data[index]
        response = httpx.put(
            f"{origin}/api/v2/uploads/{session['upload_id']}/parts/0",
            content=payload,
            headers={
                "Content-Type": "application/octet-stream",
                "X-Part-SHA256": hashlib.sha256(payload).hexdigest(),
            },
            timeout=30,
        )
        assert response.status_code == 200, response.text
        finish(d5, session)

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(transfer, range(count)))
    accounting(repo, sessions=0, claims=0, charged=2 * count * size)

    def read(session):
        started = time.monotonic()
        for attempt in range(8):
            response = httpx.get(f"{origin}/api/v2/files/{session['file_id']}/download", timeout=30)
            if response.status_code == 429:
                assert int(response.headers["Retry-After"]) > 0
                assert response.headers.get("X-Request-ID")
                time.sleep(0.1)
                continue
            assert response.status_code == 200, response.text
            return dict(
                bytes=len(response.content),
                sha256=hashlib.sha256(response.content).hexdigest(),
                upstream=response.headers["X-C5-Upstream"],
                attempts=attempt + 1,
                seconds=time.monotonic() - started,
            )
        raise AssertionError("Download remained overloaded after eight bounded attempts")

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(read, sessions))
    assert all(
        result["bytes"] == size and result["sha256"] == hashlib.sha256(payload).hexdigest()
        for result, payload in zip(results, data)
    )
    drained(d5)
    rows = sql(repo, "SELECT allocation_id,node_id FROM allocations WHERE state='STORED'")
    assert len(rows) == 2 * count
    for root in roots.values():
        records = local_rows(root)
        assert len(records) == count and all(row["state"] == "STORED" for row in records)
        assert all(
            hashlib.sha256((root / "chunks" / row["allocation_id"]).read_bytes()).hexdigest()
            == row["actual_hash"]
            for row in records
        )
    if count >= 4:
        assert len({result["upstream"] for result in results}) == 2
    report["measurements"].update(
        sessions=count,
        max_active_sessions=count,
        max_transfer_concurrency=4,
        file_bytes=size,
        elapsed_seconds=time.monotonic() - start,
        downloads=results,
    )
