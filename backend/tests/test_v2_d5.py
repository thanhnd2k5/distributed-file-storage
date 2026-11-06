"""D01-D04 through independent processes, real ingress, gRPC, DB and local disks."""

import hashlib
import json
import os
import shutil
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
from threading import Event
from uuid import UUID, uuid4

import grpc
import httpx
import pytest
from test_v2_c3 import until
from test_v2_c5 import Runtime, ingress
from test_v2_metadata import foundation as foundation
from test_v2_metadata import sql
from v2_c5_fixture import Sampler

from storage.v2.ledger import Ledger, Scope

CHUNK = 8 * 1024**2


class Storage(Runtime):
    def start(self):
        import subprocess
        import sys

        with self.path.open("ab") as log:
            self.process = subprocess.Popen(
                [sys.executable, "-m", self.module], env=self.env, stdout=log, stderr=log
            )
        with grpc.insecure_channel(f"127.0.0.1:{self.port}") as channel:
            grpc.channel_ready_future(channel).result(timeout=15)
        return self


def atomic(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value))
    temporary.replace(path)


def events(root, label):
    path = root / f"{label}.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def local_rows(root):
    with sqlite3.connect(f"file:{root / 'ledger.sqlite3'}?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute("SELECT * FROM allocations")]


@pytest.fixture
def d5(foundation, tmp_path, request):
    repo = foundation
    repo.policy = replace(repo.policy, chunk_size_bytes=CHUNK)
    control = tmp_path / "control"
    control.mkdir()
    (control / "phase").write_text("setup")
    services, roots, report = {}, {}, {"gate": request.node.name, "measurements": {}}
    with ExitStack() as stack:

        def runtime(role, index=0):
            label = f"{role}-{index}"
            service = (Storage if role == "storage" else Runtime)(
                role,
                repo,
                tmp_path,
                label,
                module="v2_d5_service",
            )
            service.env.update(
                V2_D5_CONTROL=str(control),
                V2_D5_LABEL=label,
                V2_POLICY_CHUNK_SIZE_BYTES=str(CHUNK),
                V2_DOWNLOAD_STREAMS=os.environ.get("V2_D5_DOWNLOAD_STREAMS", "2"),
                V2_DOWNLOAD_BUFFERED_BYTES=os.environ.get(
                    "V2_D5_DOWNLOAD_BUFFERED_BYTES", "37748736"
                ),
                V2_DOWNLOAD_RENEW_SECONDS="10",
                V2_DOWNLOAD_RPC_SECONDS="20",
                PYTHONPATH="/app/tests:/app:/app/generated",
            )
            services[label] = service
            stack.callback(service.close)
            return service

        for i in range(2):
            service, node, inc = runtime("storage", i), f"node-{i}", uuid4()
            root = roots[node] = tmp_path / node
            Ledger(
                root, Scope(str(repo.namespace_id), str(repo.epoch), node, str(inc)), provision=True
            ).close()
            service.env.update(
                V2_DATA_DIR=str(root),
                NODE_ID=node,
                FAILURE_DOMAIN=node,
                V2_NODE_INCARNATION=str(inc),
                V2_GRPC_BIND=f"127.0.0.1:{service.port}",
            )
            service.start()
            repo.provision_node(
                node, inc, host="127.0.0.1", port=service.port, failure_domain=node, budget=1024**3
            )
            repo.activate_node(
                node, inc, observed_namespace=repo.namespace_id, observed_epoch=repo.epoch
            )
        apis = [runtime("api", i).start() for i in range(2)]
        internals = [runtime("internal", i).start() for i in range(2)]
        transfers = [runtime("transfer", i) for i in range(2)]
        origin, internal, audit = stack.enter_context(ingress(apis, internals, transfers, tmp_path))
        for service in transfers:
            service.env.update(V2_API_URL=origin, V2_INTERNAL_URL=internal)
            service.start()
        runtime("worker").start()
        sampler = Sampler(services, control)
        stack.callback(sampler.close)
        report["topology"] = {
            label: {"pid": s.process.pid, "port": s.port} for label, s in services.items()
        }
        report["config"] = dict(
            chunk_bytes=CHUNK,
            rf=2,
            download_streams_per_transfer=int(os.environ.get("V2_D5_DOWNLOAD_STREAMS", "2")),
            renew_seconds=10,
            rpc_seconds=20,
            routing="nginx round robin, no sticky session, no upstream retry",
        )
        try:
            yield repo, roots, control, origin, services, report
        finally:
            report["resources"] = sampler.snapshot()
            report["requests"] = [json.loads(line) for line in audit.read_text().splitlines()]
            report["storage_events"] = {
                label: events(control, label) for label in services if label.startswith("storage")
            }
            report["transfer_events"] = {
                label: events(control, label) for label in services if label.startswith("transfer")
            }
            report["metrics"] = metrics(services)
            if destination := os.environ.get("V2_D5_REPORT_DIR"):
                target = Path(destination) / request.node.name
                target.mkdir(parents=True, exist_ok=True)
                (target / "report.json").write_text(json.dumps(report, indent=2))
                shutil.copyfile(tmp_path / "nginx.conf", target / "nginx.conf")
                for log in tmp_path.glob("*.log"):
                    shutil.copyfile(log, target / log.name)


def metrics(services):
    return {
        label: httpx.get(service.url + "/_d5/metrics", timeout=2).json()
        for label, service in services.items()
        if label.startswith("transfer")
    }


def source(size, seed=0):
    return (bytes((n + seed) % 256 for n in range(256)) * (size // 256 + 1))[:size]


def uploaded(env, data, *, complete=True):
    repo, _, _, origin, _, _ = env
    response = httpx.post(
        origin + "/api/v2/uploads",
        headers={"Idempotency-Key": str(uuid4())},
        json=dict(
            original_name="d5.bin",
            size_bytes=len(data),
            expected_sha256=hashlib.sha256(data).hexdigest(),
        ),
    )
    assert response.status_code == 201, response.text
    session = response.json()
    for i in range(session["part_count"]):
        part = data[i * CHUNK : (i + 1) * CHUNK]
        response = httpx.put(
            f"{origin}/api/v2/uploads/{session['upload_id']}/parts/{i}",
            content=part,
            timeout=30,
            headers={
                "Content-Type": "application/octet-stream",
                "X-Part-SHA256": hashlib.sha256(part).hexdigest(),
            },
        )
        assert response.status_code == 200, response.text
    if complete:
        finish(env, session)
    return session


def finish(env, session):
    repo, _, _, origin, _, _ = env
    response = httpx.post(f"{origin}/api/v2/uploads/{session['upload_id']}/complete", json={})
    assert response.status_code == 202, response.text
    until(lambda: repo.get_session(UUID(session["upload_id"]))["state"] == "COMPLETED", 30)


def download(origin, session, *, headers=None, pause=None, entered=None, throttle=0):
    started, total, digest, first = time.monotonic(), 0, hashlib.sha256(), None
    with httpx.stream(
        "GET", f"{origin}/api/v2/files/{session['file_id']}/download", headers=headers, timeout=35
    ) as response:
        assert response.status_code in (200, 206), response.read()
        for block in response.iter_raw(chunk_size=65536):
            if first is None:
                first = time.monotonic() - started
                if entered:
                    entered.set()
                if pause:
                    assert pause.wait(30)
            total += len(block)
            digest.update(block)
            if throttle:
                time.sleep(throttle)
        assert total == int(response.headers["Content-Length"])
        return dict(
            bytes=total,
            sha256=digest.hexdigest(),
            ttfb_seconds=first,
            seconds=time.monotonic() - started,
            upstream=response.headers["X-C5-Upstream"],
        )


def path(session):
    return f"/api/v2/files/{session['file_id']}/download"


def drained(env):
    repo, _, _, _, services, _ = env
    until(
        lambda: all(row["downloads"] == row["buffers"] == 0 for row in metrics(services).values()),
        5,
    )
    until(lambda: not sql(repo, "SELECT 1 FROM download_leases WHERE state='ACTIVE'"), 5)


def payloads(control):
    return sum(
        e["payload_bytes"]
        for i in range(2)
        for e in events(control, f"storage-{i}")
        if e["kind"] == "read_end"
    )


def test_round_robin_range_ttfb_and_read_amplification(d5):
    repo, roots, control, origin, services, report = d5
    data = source(CHUNK + 17)
    session = uploaded(d5, data)
    (control / "phase").write_text("download")
    before = payloads(control)
    results = [download(origin, session) for _ in range(2)]
    assert {r["upstream"] for r in results} == {
        f"127.0.0.1:{s.port}" for name, s in services.items() if name.startswith("transfer")
    }
    assert all(
        r["bytes"] == len(data) and r["sha256"] == hashlib.sha256(data).hexdigest() for r in results
    )
    until(lambda: payloads(control) >= before + 2 * len(data))
    before = payloads(control)
    tiny = download(origin, session, headers={"Range": "bytes=1-1"})
    assert tiny["bytes"] == 1 and tiny["sha256"] == hashlib.sha256(data[1:2]).hexdigest()
    until(lambda: payloads(control) == before + CHUNK)
    report["measurements"].update(
        full=results, tiny_range=tiny, tiny_range_wire_amplification=CHUNK
    )
    etag = httpx.head(origin + path(session)).headers["ETag"]
    for headers, expected in [
        ({"Range": "bytes=-17", "If-Range": etag}, data[-17:]),
        ({"Range": "bytes=0-1,4-5"}, data),
        ({"Range": "bytes=8388607-"}, data[CHUNK - 1 :]),
    ]:
        response = httpx.get(origin + path(session), headers=headers)
        assert response.content == expected
    empty = uploaded(d5, b"")
    assert httpx.get(origin + path(empty)).content == b""
    assert httpx.get(origin + path(empty), headers={"Range": "bytes=0-"}).status_code == 416
    drained(d5)
    for root in roots.values():
        assert all(row["state"] == "STORED" for row in local_rows(root))
    assert sql(repo, "SELECT count(*) AS n FROM download_leases")[0]["n"] == 7


@pytest.mark.parametrize("outcome", ["finish", "expire"])
def test_reader_delete_cleanup_across_instances(d5, outcome):
    repo, roots, control, origin, services, report = d5
    data = source(CHUNK + 17)
    session = uploaded(d5, data)
    chunk = str(sql(repo, "SELECT chunk_id FROM manifest_parts WHERE part_index=0")[0]["chunk_id"])
    (control / "hold-eof").touch()
    atomic(control / "storage-0-fault.json", dict(mode="hold_eof", chunk_id=chunk))
    (control / "phase").write_text("read-delete")
    with ThreadPoolExecutor(1) as pool:
        reader = pool.submit(httpx.get, origin + path(session), timeout=30)
        until(lambda: any(e["kind"] == "held_eof" for e in events(control, "storage-0")))
        response = httpx.delete(origin + f"/api/v2/files/{session['file_id']}")
        assert response.status_code == 202 and response.json()["cleanup_pending"]
        until(
            lambda: bool(
                sql(
                    repo,
                    "SELECT 1 FROM jobs WHERE kind='CLEANUP' AND last_error='READ_LEASE_ACTIVE'",
                )
            )
        )
        assert sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 2 * len(
            data
        )
        for label, service in services.items():
            if label.startswith("api"):
                assert (
                    httpx.get(service.url + f"/api/v2/files/{session['file_id']}").status_code
                    == 404
                )
            if label.startswith("transfer"):
                assert (
                    httpx.get(
                        service.url + path(session), headers={"If-None-Match": "*"}
                    ).status_code
                    == 404
                )
        assert len(sql(repo, "SELECT * FROM download_leases")) == 1
        for root in roots.values():
            assert len(list((root / "chunks").iterdir())) == 2
        if outcome == "finish":
            until(
                lambda: any(
                    e["kind"] == "renewed"
                    for label in ("transfer-0", "transfer-1")
                    for e in events(control, label)
                ),
                15,
            )
            (control / "hold-eof").unlink()
            response = reader.result(timeout=10)
            assert response.status_code == 200 and response.content == data
        else:
            sql(
                repo,
                "UPDATE download_leases SET created_at=clock_timestamp()-interval '2 seconds',"
                "expires_at=clock_timestamp()-interval '1 second'",
            )
            response = reader.result(timeout=15)
            assert response.status_code == 503
            assert response.json()["error"]["code"] == "STORAGE_UNAVAILABLE"
            (control / "hold-eof").unlink()
        drained(d5)
    sql(
        repo,
        "UPDATE jobs SET run_after=clock_timestamp() "
        "WHERE kind='CLEANUP' AND state IN ('READY','RETRY')",
    )
    until(lambda: sql(repo, "SELECT state FROM files")[0]["state"] == "DELETED", 15)
    assert sql(repo, "SELECT sum(charged_bytes) AS n FROM node_budgets")[0]["n"] == 0
    for root in roots.values():
        assert not list((root / "chunks").iterdir())
        assert all(
            row["state"] == "DELETED" and row["cleanup_complete"] for row in local_rows(root)
        )
    report["measurements"]["outcome"] = outcome


def test_delete_wins_before_acquire_on_other_process(d5):
    repo, _, control, origin, _, _ = d5
    session = uploaded(d5, source(17))
    (control / "hold-acquire").touch()
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(httpx.get, origin + path(session))
        until(
            lambda: any(
                e["kind"] == "held_acquire"
                for label in ("transfer-0", "transfer-1")
                for e in events(control, label)
            )
        )
        assert httpx.delete(origin + f"/api/v2/files/{session['file_id']}").status_code == 202
        (control / "hold-acquire").unlink()
        response = future.result(timeout=5)
        assert response.status_code == 404 and response.json()["error"]["code"] == "FILE_NOT_FOUND"
    assert not sql(repo, "SELECT * FROM download_leases")


def test_bad_replica_and_failure_after_headers_through_ingress(d5):
    repo, roots, control, origin, _, report = d5
    data = source(CHUNK + 17)
    session = uploaded(d5, data)
    parts = sql(repo, "SELECT * FROM manifest_parts ORDER BY part_index")
    first = str(parts[0]["chunk_id"])
    atomic(control / "storage-0-fault.json", dict(mode="missing_footer", chunk_id=first))
    before = payloads(control)
    response = download(origin, session)
    assert response["sha256"] == hashlib.sha256(data).hexdigest()
    until(lambda: payloads(control) >= before + len(data) + CHUNK)
    report["measurements"]["fallback_wire_amplification"] = (len(data) + CHUNK) / len(data)
    second = str(parts[1]["chunk_id"])
    atomic(control / "storage-0-fault.json", dict(mode="missing_footer", chunk_id=second))
    atomic(control / "storage-1-fault.json", dict(mode="eof_error", chunk_id=second))
    received = bytearray()
    with httpx.stream("GET", origin + path(session), timeout=30) as response:
        assert response.status_code == 200
        with pytest.raises(httpx.RemoteProtocolError):
            for block in response.iter_raw():
                received.extend(block)
    assert received == data[:CHUNK]
    drained(d5)
    assert sql(repo, "SELECT state FROM files")[0]["state"] == "AVAILABLE"
    for root in roots.values():
        assert all(row["state"] == "STORED" for row in local_rows(root))


def test_mixed_load_slow_clients_replica_finalize_and_rss(d5):
    repo, _, control, origin, services, report = d5
    data = source(8 * CHUNK)
    # Four readers of the same physical chunks must work even with RF=2.
    shared = uploaded(d5, data)
    sessions = [shared] * 4
    finalizing = uploaded(d5, source(2 * CHUNK, 1), complete=False)
    (control / "phase").write_text("mixed")
    atomic(control / "storage-0-fault.json", dict(delay=0.003))
    baseline = {
        label: int(
            next(
                line.split()[1]
                for line in Path(f"/proc/{s.process.pid}/status").read_text().splitlines()
                if line.startswith("VmRSS:")
            )
        )
        for label, s in services.items()
    }
    release = Event()
    entered = [Event() for _ in sessions]
    with ThreadPoolExecutor(4) as pool:
        futures = [
            pool.submit(download, origin, session, pause=release, entered=ready, throttle=0.005)
            for session, ready in zip(sessions, entered)
        ]
        try:
            assert all(ready.wait(15) for ready in entered)
            until(lambda: all(row["downloads"] == 2 for row in metrics(services).values()))
            leases = len(sql(repo, "SELECT * FROM download_leases"))
            rejected = [httpx.get(origin + path(sessions[0])).status_code for _ in range(16)]
            assert rejected == [429] * 16
            assert len(sql(repo, "SELECT * FROM download_leases")) == leases == 4
            # Independent upload/finalize budgets must progress under download saturation.
            started = time.monotonic()
            finish(d5, finalizing)
            uploaded(d5, source(CHUNK + 17, 2))
            report["measurements"]["upload_finalize_while_saturated_seconds"] = (
                time.monotonic() - started
            )
            until(
                lambda: (
                    sum(
                        e["kind"] == "renewed"
                        for label in ("transfer-0", "transfer-1")
                        for e in events(control, label)
                    )
                    >= 4
                ),
                15,
            )
            assert all(not future.done() for future in futures)
        finally:
            release.set()
        results = [future.result(timeout=30) for future in futures]
    assert all(
        r["bytes"] == len(data) and r["sha256"] == hashlib.sha256(data).hexdigest() for r in results
    )
    drained(d5)
    current = metrics(services)
    assert all(row["max_buffers"] <= 2 and row["max_downloads"] == 2 for row in current.values())
    assert max(row["max_send_seconds"] for row in current.values()) > 1
    report["measurements"].update(
        downloads=results, baseline_rss_kib=baseline, admission_rejections=16
    )
    # Fixture guard for Python/gRPC/socket overhead, distinct from chunk reservation.
    for label, service in services.items():
        if label.startswith("transfer"):
            peak = int(
                next(
                    line.split()[1]
                    for line in Path(f"/proc/{service.process.pid}/status").read_text().splitlines()
                    if line.startswith("VmHWM:")
                )
            )
            assert peak < 256 * 1024, (label, peak)
    assert not sql(repo, "SELECT 1 FROM upload_sessions WHERE state IN ('OPEN','FINALIZING')")
