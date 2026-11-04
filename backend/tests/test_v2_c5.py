"""C5 small-file gates with independent runtime processes and round-robin ingress.

These are HTTP clients, not the C4 browser client or a large-file/RSS benchmark.
Only disposable test schemas/Storage directories are used.
"""

import hashlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from contextlib import ExitStack, contextmanager
from dataclasses import replace
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from test_v2_c2 import cluster as cluster
from test_v2_c3 import until
from test_v2_metadata import foundation as foundation
from test_v2_metadata import sql

CHUNK = 65536


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Runtime:
    def __init__(self, role, repo, directory, label, *, module="metadata.v2.runtime", **env):
        self.module = module
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.path = directory / f"{label}.log"
        self.env = dict(
            os.environ,
            V2_SERVICE_ROLE=role,
            V2_PORT=str(self.port),
            V2_BIND_HOST="127.0.0.1",
            V2_DATABASE_URL=repo.engine.url.render_as_string(hide_password=False),
            V2_DATABASE_SCHEMA=repo.schema,
            V2_NAMESPACE_ID=str(repo.namespace_id),
            V2_RECOVERY_EPOCH=str(repo.epoch),
            V2_POLICY_CHUNK_SIZE_BYTES=str(CHUNK),
            V2_WORKER_POLL_SECONDS="0.02",
            V2_SHUTDOWN_SECONDS="2",
            PYTHONPATH=os.pathsep.join([".", "generated"]),
            **env,
        )
        self.process = None

    def start(self):
        with self.path.open("ab") as log:
            self.process = subprocess.Popen(
                [sys.executable, "-m", self.module],
                env=self.env,
                stdout=log,
                stderr=log,
            )

        def ready():
            assert self.process.poll() is None, self.path.read_text()
            try:
                return httpx.get(self.url + "/health/ready", timeout=0.5).status_code == 200
            except httpx.TransportError:
                return False

        until(ready, seconds=15)
        return self

    def kill(self):
        self.process.kill()
        assert self.process.wait(timeout=5) == -signal.SIGKILL

    def close(self):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
                raise


@contextmanager
def ingress(apis, internals, transfers, directory, *, port=None, bind="127.0.0.1", control=None):
    executable = shutil.which("nginx")
    assert executable, "C5 requires compose.v2-http-tests.yml"
    port, internal_port = port or free_port(), free_port()
    config = Path("/app/v2-ingress.conf").read_text()

    def upstream(instances):
        return " ".join(f"server 127.0.0.1:{instance.port};" for instance in instances)

    config = config.replace("server api:8080;", upstream(apis))
    config = config.replace("server transfer:8080;", upstream(transfers))
    config = config.replace("listen 8080;", f"listen {bind}:{port};")
    if control is not None:
        config = config.replace(
            "location /api/v2/ {",
            f"location /api/v2/_c5/ {{ proxy_pass http://127.0.0.1:{control}; }}\n"
            "        location /api/v2/ {",
        )
    config = config.replace("/tmp/v2-nginx.pid", str(directory / "nginx.pid"))
    audit = directory / "requests.jsonl"
    config = config.replace(
        "access_log off;",
        'log_format c5 escape=json \'{"path":"$uri","method":"$request_method",'
        '"upstream":"$upstream_addr","status":$status}\';\n'
        f"access_log {audit} c5;",
    )
    config = config.replace(
        "proxy_http_version 1.1;",
        "add_header X-C5-Upstream $upstream_addr always;\n        proxy_http_version 1.1;",
    )
    # Service-only listener, reachable only on this container's loopback.
    internal = f"""
    upstream v2_internal {{ {upstream(internals)} }}
    server {{
        listen 127.0.0.1:{internal_port};
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_request_buffering off;
        proxy_buffering off;
        proxy_next_upstream off;
        location /internal/v2/ {{ proxy_pass http://v2_internal; }}
        location = /health/ready {{ proxy_pass http://v2_internal; }}
        location / {{ return 404; }}
    }}
"""
    position = config.rfind("}")
    config = config[:position] + internal + config[position:]
    path = directory / "nginx.conf"
    path.write_text(config)
    with (directory / "nginx.log").open("ab") as log:
        process = subprocess.Popen(
            [executable, "-c", str(path), "-g", "daemon off;"], stdout=log, stderr=log
        )
    try:
        origin = f"http://127.0.0.1:{port}"

        def ready():
            assert process.poll() is None, (directory / "nginx.log").read_text()
            try:
                return httpx.get(origin + "/api/v2/config", timeout=0.5).status_code == 200
            except httpx.TransportError:
                return False

        until(ready)
        yield origin, f"http://127.0.0.1:{internal_port}", audit
    finally:
        process.terminate()
        process.wait(timeout=5)


@pytest.fixture
def multi(cluster, tmp_path, request):
    repo, resources = cluster
    repo.policy = replace(repo.policy, chunk_size_bytes=CHUNK)
    for ledger, _, _ in resources:
        ledger.chunk_limit = CHUNK
        repo.set_budget(ledger.scope.node_id, UUID(ledger.scope.node_incarnation), 16 * 1024**2)
    evidence = {"gate": request.node.name, "schema": repo.schema, "files": []}
    with ExitStack() as stack:
        apis, internals, transfers = [], [], []
        for role, collection in (("api", apis), ("internal", internals)):
            for index in range(2):
                runtime = Runtime(role, repo, tmp_path, f"{role}-{index}")
                stack.callback(runtime.close)
                collection.append(runtime.start())
        # Reserve Transfer ports before generating the proxy upstreams.
        for index in range(2):
            runtime = Runtime("transfer", repo, tmp_path, f"transfer-{index}")
            stack.callback(runtime.close)
            transfers.append(runtime)
        origin, internal, audit = stack.enter_context(ingress(apis, internals, transfers, tmp_path))
        for runtime in transfers:
            runtime.env.update(V2_API_URL=origin, V2_INTERNAL_URL=internal)
            runtime.start()
        evidence["topology"] = {
            "api": [r.port for r in apis],
            "internal": [r.port for r in internals],
            "transfer": [r.port for r in transfers],
            "pids": [r.process.pid for r in apis + internals + transfers],
            "chunk_size_bytes": CHUNK,
            "replication_factor": 2,
            "distinct_domains": 2,
            "routing": "nginx round robin, no sticky session, proxy_next_upstream off",
        }
        evidence["audit_path"] = str(audit)
        assert len(set(evidence["topology"]["pids"])) == 6
        worker = Runtime("worker", repo, tmp_path, "worker")
        stack.callback(worker.close)
        try:
            yield repo, resources, origin, apis, internals, transfers, worker, evidence
        finally:
            evidence["requests"] = [json.loads(line) for line in audit.read_text().splitlines()]
            destination = os.environ.get("V2_C5_REPORT_DIR")
            if destination:
                root = Path(destination)
                root.mkdir(parents=True, exist_ok=True)
                (root / f"{request.node.name}.json").write_text(json.dumps(evidence, indent=2))
                for log in tmp_path.glob("*.log"):
                    shutil.copyfile(log, root / f"{request.node.name}-{log.name}")


def create(origin, data):
    key = str(uuid4())
    payload = dict(
        original_name="c5.bin",
        size_bytes=len(data),
        expected_sha256=hashlib.sha256(data).hexdigest(),
    )
    response = httpx.post(
        origin + "/api/v2/uploads", json=payload, headers={"Idempotency-Key": key}
    )
    assert response.status_code == 201, response.text
    # Same key crosses API processes and cannot allocate another session/file.
    replay = httpx.post(origin + "/api/v2/uploads", json=payload, headers={"Idempotency-Key": key})
    assert replay.status_code == 200 and replay.json() == response.json()
    assert replay.headers["X-C5-Upstream"] != response.headers["X-C5-Upstream"]
    return response.json()


def put(origin, upload, index, data):
    response = httpx.put(
        f"{origin}/api/v2/uploads/{upload}/parts/{index}",
        content=iter(data[i : i + 8192] for i in range(0, len(data), 8192)),
        headers={
            "Content-Type": "application/octet-stream",
            "X-Part-SHA256": hashlib.sha256(data).hexdigest(),
        },
        timeout=15,
    )
    assert response.status_code == 200, response.text
    assert response.json()["state"] == "VERIFIED"
    return response


def resume(origin, session, data):
    upload = session["upload_id"]
    known, after = set(), -1
    while True:
        page = httpx.get(
            f"{origin}/api/v2/uploads/{upload}/parts", params={"limit": 1, "after_index": after}
        )
        assert page.status_code == 200, page.text
        page = page.json()
        known.update(item["index"] for item in page["items"] if item["state"] == "VERIFIED")
        if not page["has_more"]:
            break
        after = page["next_after_index"]
    sent = []
    for index in range(session["part_count"]):
        if index not in known:
            put(origin, upload, index, data[index * CHUNK : (index + 1) * CHUNK])
            sent.append(index)
    return sent


def verify(repo, resources, session, data):
    upload = UUID(session["upload_id"])
    assert repo.get_session(upload)["state"] == "COMPLETED"
    file = sql(repo, "SELECT * FROM files WHERE file_id=:id", id=UUID(session["file_id"]))[0]
    assert file["state"] == "AVAILABLE"
    expected = hashlib.sha256(data).hexdigest()
    assert file["expected_sha256"] == file["actual_sha256"] == expected
    parts = sql(
        repo,
        "SELECT * FROM manifest_parts WHERE version_id=:id ORDER BY part_index",
        id=file["version_id"],
    )
    assert len(parts) == session["part_count"] == (len(data) + CHUNK - 1) // CHUNK
    ledgers = {ledger.scope.node_id: ledger for ledger, _, _ in resources}
    digests = {node: hashlib.sha256() for node in ledgers}
    totals = {node: 0 for node in ledgers}
    for part in parts:
        copies = sql(repo, "SELECT * FROM allocations WHERE chunk_id=:id", id=part["chunk_id"])
        assert len(copies) == 2
        assert {copy["state"] for copy in copies} == {"STORED"}
        assert len({copy["failure_domain_snapshot"] for copy in copies}) == 2
        for copy in copies:
            ledger = ledgers[copy["node_id"]]
            record = ledger.inspect(ledger.scope, str(copy["allocation_id"]), copy["generation"])
            assert record["state"] == "STORED" and record["generation"] == copy["generation"]
            block = (ledger.root / "chunks" / str(copy["allocation_id"])).read_bytes()
            assert len(block) == part["size_bytes"]
            assert hashlib.sha256(block).hexdigest() == part["checksum_sha256"]
            digests[copy["node_id"]].update(block)
            totals[copy["node_id"]] += len(block)
    assert set(totals.values()) == {len(data)}
    assert {digest.hexdigest() for digest in digests.values()} == {expected}
    return dict(
        upload_id=str(upload),
        size_bytes=len(data),
        sha256=expected,
        replicas=2,
        parts=len(parts),
        state=file["state"],
    )


def complete(origin, repo, session):
    upload = session["upload_id"]
    response = httpx.post(f"{origin}/api/v2/uploads/{upload}/complete", json={})
    assert response.status_code == 202, response.text
    until(lambda: repo.get_session(UUID(upload))["state"] == "COMPLETED", seconds=15)
    assert httpx.get(f"{origin}/api/v2/uploads/{upload}").json()["state"] == "COMPLETED"


def test_round_robin_upload_resume_boundaries(multi):
    repo, resources, origin, apis, internals, transfers, worker, evidence = multi
    worker.start()
    for size in (0, CHUNK - 1, CHUNK, CHUNK + 1, 3 * CHUNK + 17):
        data = (bytes(range(251)) * (size // 251 + 1))[:size]
        session = create(origin, data)
        if size:
            first = put(origin, session["upload_id"], 0, data[:CHUNK])
            # Retry on the other Transfer verifies body without new allocation/charge.
            replay = put(origin, session["upload_id"], 0, data[:CHUNK])
            assert first.headers["X-C5-Upstream"] != replay.headers["X-C5-Upstream"]
        assert resume(origin, session, data) == list(range(1, session["part_count"]))
        complete(origin, repo, session)
        evidence["files"].append(verify(repo, resources, session, data))
    assert sql(repo, "SELECT count(*) n FROM upload_sessions")[0]["n"] == 5
    assert sql(repo, "SELECT sum(charged_bytes) n FROM node_budgets")[0]["n"] == (
        2 * sum(file["size_bytes"] for file in evidence["files"])
    )
    assert httpx.post(origin + "/internal/v2/anything", json={}).status_code == 404
    requests = [json.loads(line) for line in Path(evidence["audit_path"]).read_text().splitlines()]
    for role, prefix in (("api", "/api/v2/"), ("internal", "/internal/v2/")):
        visited = {r["upstream"] for r in requests if r["path"].startswith(prefix)}
        assert {f"127.0.0.1:{port}" for port in evidence["topology"][role]} <= visited
    visited = {r["upstream"] for r in requests if r["method"] == "PUT"}
    assert visited == {f"127.0.0.1:{port}" for port in evidence["topology"]["transfer"]}


def test_restart_api_transfer_resume_confirmed_parts(multi):
    repo, resources, origin, apis, internals, transfers, worker, evidence = multi
    data = b"c" * (3 * CHUNK + 17)
    session = create(origin, data)
    put(origin, session["upload_id"], 0, data[:CHUNK])
    old_pids = [apis[0].process.pid, transfers[0].process.pid]
    apis[0].kill()
    transfers[0].kill()
    # Remaining processes can recover the session without the original RAM state.
    status = httpx.get(f"{apis[1].url}/api/v2/uploads/{session['upload_id']}")
    assert status.status_code == 200 and status.json()["confirmed_parts"] == 1
    apis[0].start()
    transfers[0].start()
    assert old_pids != [apis[0].process.pid, transfers[0].process.pid]
    assert resume(origin, session, data) == [1, 2, 3]
    worker.start()
    complete(origin, repo, session)
    evidence["files"].append(verify(repo, resources, session, data))
    assert sql(repo, "SELECT count(*) n FROM allocations")[0]["n"] == 8
    assert sql(repo, "SELECT sum(charged_bytes) n FROM node_budgets")[0]["n"] == 2 * len(data)
    evidence["restart"] = {
        "old_pids": old_pids,
        "new_pids": [apis[0].process.pid, transfers[0].process.pid],
        "resent_parts": [1, 2, 3],
    }


def test_sigkill_active_transfer_real_lease_resume_abort_cleanup(multi):
    repo, resources, origin, apis, internals, transfers, worker, evidence = multi
    data = b"d" * CHUNK
    session = create(origin, data)
    upload = session["upload_id"]
    with socket.create_connection(("127.0.0.1", transfers[0].port)) as stream:
        stream.sendall(
            (
                f"PUT /api/v2/uploads/{upload}/parts/0 HTTP/1.1\r\nHost: localhost\r\n"
                "Content-Type: application/octet-stream\r\n"
                f"X-Part-SHA256: {hashlib.sha256(data).hexdigest()}\r\n"
                "Transfer-Encoding: chunked\r\n\r\n1\r\nd\r\n"
            ).encode()
        )
        until(lambda: repo.upload_status(UUID(upload))["active_claims"] == 1)
        until(
            lambda: all(
                ledger.inventory(ledger.scope)[0]
                and ledger.inventory(ledger.scope)[0][0]["state"] == "WRITING"
                for ledger, _, _ in resources
            )
        )
        transfers[0].kill()
    response = httpx.put(
        f"{transfers[1].url}/api/v2/uploads/{upload}/parts/0",
        content=data,
        headers={
            "Content-Type": "application/octet-stream",
            "X-Part-SHA256": hashlib.sha256(data).hexdigest(),
        },
    )
    assert response.status_code == 409 and response.json()["error"]["code"] == "PART_BUSY"
    assert int(response.headers["Retry-After"]) > 0
    started = time.monotonic()
    worker.start()
    # Wait for the real DB lease/sweeper; do not move expiry timestamps in SQL.
    until(lambda: repo.part_record(UUID(upload), 0)["state"] != "CLAIMED", seconds=70)
    evidence["lease_wait_seconds"] = time.monotonic() - started
    # Shutdown deliberately retains an in-flight job lease. Wait for the
    # incomplete stream's background reconcile to commit RETRY before stopping.
    until(
        lambda: sql(repo, "SELECT state FROM jobs WHERE kind='RECONCILE'") == [{"state": "RETRY"}],
        seconds=10,
    )
    worker.close()
    # Let a background reconcile release its job and observe its real backoff.
    # The dedicated internal consumer then owns the fenced handoff.
    until(
        lambda: all(
            row["state"] != "RUNNING" and row["due"]
            for row in sql(
                repo,
                "SELECT state,run_after<=clock_timestamp() AS due FROM jobs WHERE kind='RECONCILE'",
            )
        ),
        seconds=15,
    )
    put(transfers[1].url, upload, 0, data)
    assert {row["generation"] for row in sql(repo, "SELECT generation FROM allocations")} == {2}
    assert sql(repo, "SELECT sum(charged_bytes) n FROM node_budgets")[0]["n"] == 2 * len(data)
    response = httpx.post(f"{origin}/api/v2/uploads/{upload}/abort", json={})
    assert response.status_code in (200, 202), response.text
    worker.start()
    until(lambda: not repo.upload_status(UUID(upload))["cleanup_pending"], seconds=15)
    assert httpx.post(f"{origin}/api/v2/uploads/{upload}/abort", json={}).status_code == 200
    assert sql(repo, "SELECT sum(charged_bytes) n FROM node_budgets")[0]["n"] == 0
    for ledger, _, _ in resources:
        records = ledger.inventory(ledger.scope)[0]
        assert len(records) == 1 and records[0]["state"] == "DELETED"
        assert list((ledger.root / "chunks").iterdir()) == []
    evidence["cleanup"] = {
        "state": "ABORTED",
        "charged_bytes": 0,
        "generation": 2,
        "replicas_deleted": 2,
    }
