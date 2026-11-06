"""Disposable browser/large-file C5 fixture; real processes, DB, gRPC and disks."""

import asyncio
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from contextlib import ExitStack, contextmanager
from pathlib import Path
from threading import Event, Lock, Thread
from uuid import UUID, uuid4

import grpc
import httpx
from fastapi import FastAPI, Request
from test_v2_c3 import serve, until
from test_v2_c5 import Runtime, ingress
from test_v2_metadata import foundation, sql

from storage.v2.ledger import Ledger, Scope

CHUNK = 8 * 1024**2


class Sampler:
    def __init__(self, services, root):
        self.services, self.root = services, root
        self.stop, self.lock = Event(), Lock()
        self.data = {}
        self.thread = Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        tick = 0
        while not self.stop.wait(0.1):
            phase = (self.root / "phase").read_text()
            with self.lock:
                group = self.data.setdefault(phase, {})
                for label, service in list(self.services.items()):
                    process = service.process
                    if process is None or process.poll() is not None:
                        continue
                    try:
                        status = Path(f"/proc/{process.pid}/status").read_text()
                    except FileNotFoundError:
                        continue
                    row = group.setdefault(label, {"samples": 0})
                    row["samples"] += 1
                    for key in ("VmRSS", "VmHWM", "RssAnon"):
                        match = re.search(rf"^{key}:\s+(\d+)", status, re.M)
                        if match:
                            row[key + "_kib"] = max(row.get(key + "_kib", 0), int(match[1]))
            tick += 1
            if tick % 10:
                continue
            output = subprocess.run(["ss", "-tmnpH"], capture_output=True, text=True, check=True)
            lines, by_pid = output.stdout.splitlines(), {}
            for index, line in enumerate(lines):
                pids = re.findall(r"pid=(\d+)", line)
                if not pids:
                    continue
                memory = line + (lines[index + 1] if index + 1 < len(lines) else "")
                match = re.search(r"skmem:\(([^)]+)\)", memory)
                values = dict(re.findall(r"([a-z]+)(\d+)", match[1])) if match else {}
                for pid in pids:
                    row = by_pid.setdefault(
                        int(pid), {"socket_count": 0, "socket_used": 0, "socket_capacity": 0}
                    )
                    row["socket_count"] += 1
                    row["socket_used"] += sum(int(values.get(k, 0)) for k in ("r", "t", "w"))
                    row["socket_capacity"] += sum(int(values.get(k, 0)) for k in ("rb", "tb"))
            with self.lock:
                for label, service in list(self.services.items()):
                    if service.process is None:
                        continue
                    row = self.data.setdefault(phase, {}).setdefault(label, {})
                    for key, value in by_pid.get(service.process.pid, {}).items():
                        row[key + "_peak"] = max(row.get(key + "_peak", 0), value)

    def snapshot(self):
        with self.lock:
            return json.loads(json.dumps(self.data))

    def close(self):
        self.stop.set()
        self.thread.join(5)
        assert not self.thread.is_alive()


class Storage(Runtime):
    def start(self):
        with self.path.open("ab") as log:
            self.process = subprocess.Popen(
                [sys.executable, "-m", "v2_c5_storage"], env=self.env, stdout=log, stderr=log
            )
        with grpc.insecure_channel(f"127.0.0.1:{self.port}") as channel:
            grpc.channel_ready_future(channel).result(timeout=15)
        assert self.process.poll() is None, self.path.read_text()
        return self


def reference(size, seed):
    block = bytes((i * 31 + seed) % 256 for i in range(256)) * 4096
    digest = hashlib.sha256()
    for offset in range(0, size, len(block)):
        digest.update(block[: min(len(block), size - offset)])
    return digest.hexdigest()


def verify(repo, roots, upload):
    session = repo.upload_status(UUID(upload))
    assert session["state"] == "COMPLETED", session
    file = sql(repo, "SELECT * FROM files WHERE file_id=:id", id=session["file_id"])[0]
    assert file["state"] == "AVAILABLE"
    digests = {node: hashlib.sha256() for node in roots}
    sizes = {node: 0 for node in roots}
    parts = sql(
        repo,
        "SELECT * FROM manifest_parts WHERE version_id=:id ORDER BY part_index",
        id=file["version_id"],
    )
    assert len(parts) == session["part_count"]
    for index, part in enumerate(parts):
        assert part["part_index"] == index
        copies = sql(repo, "SELECT * FROM allocations WHERE chunk_id=:id", id=part["chunk_id"])
        assert len(copies) == 2 and len({c["failure_domain_snapshot"] for c in copies}) == 2
        for copy in copies:
            assert copy["state"] == "STORED"
            local, size = hashlib.sha256(), 0
            with (roots[copy["node_id"]] / "chunks" / str(copy["allocation_id"])).open("rb") as f:
                while block := f.read(262144):
                    local.update(block)
                    digests[copy["node_id"]].update(block)
                    size += len(block)
            assert size == part["size_bytes"]
            assert local.hexdigest() == part["checksum_sha256"]
            sizes[copy["node_id"]] += size
    assert set(sizes.values()) == {session["size_bytes"]}
    assert {d.hexdigest() for d in digests.values()} == {file["expected_sha256"]}
    assert file["actual_sha256"] == file["expected_sha256"]
    return {
        "upload_id": upload,
        "size_bytes": session["size_bytes"],
        "parts": len(parts),
        "sha256": file["actual_sha256"],
        "state": "AVAILABLE",
        "replica_sizes": sizes,
    }


def main():
    stopping = Event()
    restarting = Event()
    destination = Path(os.environ.get("V2_C5_REPORT_DIR", "/evidence"))
    destination.mkdir(parents=True, exist_ok=True)
    report, services, roots = {}, {}, {}
    with ExitStack() as stack:
        repo = stack.enter_context(contextmanager(foundation.__wrapped__)())
        directory = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="c5-")))
        control_root = directory / "control"
        control_root.mkdir()
        for name, value in (("hold-worker", ""), ("phase", "setup"), ("slow-storage", "0")):
            (control_root / name).write_text(value)

        def runtime(role, index=0):
            label = f"{role}-{index}"
            cls = Storage if role == "storage" else Runtime
            result = cls(role, repo, directory, label, module="v2_c5_service")
            result.env.update(
                PYTHONPATH="/app/tests:/app:/app/generated",
                V2_C5_CONTROL=str(control_root),
                V2_C5_LABEL=label,
                V2_POLICY_CHUNK_SIZE_BYTES=str(CHUNK),
                V2_RENEW_SECONDS="5",
                V2_WORKER_POLL_SECONDS="0.1",
            )
            services[label] = result
            stack.callback(result.close)
            return result

        for index in range(2):
            service = runtime("storage", index)
            node, incarnation = f"node-{index}", uuid4()
            root = directory / node
            scope = Scope(str(repo.namespace_id), str(repo.epoch), node, str(incarnation))
            Ledger(root, scope, provision=True).close()
            roots[node] = root
            service.env.update(
                V2_DATA_DIR=str(root),
                NODE_ID=node,
                FAILURE_DOMAIN=node,
                V2_NODE_INCARNATION=str(incarnation),
                V2_GRPC_BIND=f"127.0.0.1:{service.port}",
            )
            service.start()
            repo.provision_node(
                node,
                incarnation,
                host="127.0.0.1",
                port=service.port,
                failure_domain=node,
                budget=16 * 1024**3,
            )
            repo.activate_node(
                node, incarnation, observed_namespace=repo.namespace_id, observed_epoch=repo.epoch
            )
        apis = [runtime("api", i).start() for i in range(2)]
        internals = [runtime("internal", i).start() for i in range(2)]
        transfers = [runtime("transfer", i) for i in range(2)]
        worker = runtime("worker")
        sampler = Sampler(services, control_root)
        stack.callback(sampler.close)
        control_app = FastAPI()
        verified = []

        def save():
            snapshots = sampler.snapshot()
            for label, service in services.items():
                if not label.startswith("transfer") or service.process.poll() is not None:
                    continue
                try:
                    metrics = httpx.get(service.url + "/_c5/metrics", timeout=1).json()
                except httpx.TransportError:
                    continue
                assert metrics["queue_high_water"] <= 2 and metrics["max_streams"] <= 8
                with (destination / "transfer-probes.jsonl").open("a") as output:
                    output.write(
                        json.dumps(
                            {
                                "phase": (control_root / "phase").read_text(),
                                "label": label,
                                "pid": service.process.pid,
                                "metrics": metrics,
                            }
                        )
                        + "\n"
                    )
            (destination / "resources.json").write_text(json.dumps(snapshots, indent=2))
            (destination / "browser-report.json").write_text(json.dumps(report, indent=2))
            (destination / "disk-evidence.json").write_text(json.dumps(verified, indent=2))
            return snapshots

        def action(data):
            kind = data["action"]
            if kind == "phase":
                (control_root / "phase").write_text(data["name"])
            elif kind == "fault":
                (control_root / "lose-response").write_text(data["kind"])
            elif kind == "hold-confirm":
                (control_root / "hold-confirm").touch()
            elif kind == "kill-transfer":
                path = control_root / "transfer-event.json"
                until(path.exists, seconds=20)
                event = json.loads(path.read_text())
                restarting.set()
                try:
                    services[event["label"]].kill()
                    (control_root / "hold-confirm").unlink()
                    services[event["label"]].start()
                finally:
                    restarting.clear()
                return event
            elif kind == "wait-verified":
                upload = UUID(data["upload_id"])
                until(lambda: repo.part_record(upload, 0)["state"] == "VERIFIED", seconds=75)
            elif kind == "hold-worker":
                (control_root / "worker-event.json").unlink(missing_ok=True)
                (control_root / "hold-worker").write_text("before_publish")
            elif kind == "kill-worker":
                path = control_root / "worker-event.json"
                until(path.exists, seconds=20)
                event = json.loads(path.read_text())
                assert (
                    sql(repo, "SELECT count(*) n FROM upload_sessions WHERE state='FINALIZING'")[0][
                        "n"
                    ]
                    == 1
                )
                restarting.set()
                try:
                    worker.kill()
                    (control_root / "hold-worker").write_text("")
                    worker.start()
                finally:
                    restarting.clear()
                return event
            elif kind == "lose-delete-ack":
                (control_root / "lose-delete-ack").touch()
            elif kind == "observe-cleanup-pending":
                until(lambda: (control_root / "delete-ack-lost").exists(), seconds=15)
                pending = sql(
                    repo, "SELECT count(*) n FROM allocations WHERE state='DELETE_PENDING'"
                )[0]["n"]
                charge = sql(repo, "SELECT sum(charged_bytes) n FROM node_budgets")[0]["n"]
                assert pending > 0 and charge > 0
                return {"pending": pending, "charge": charge}
            elif kind == "slow":
                (control_root / "slow-storage").write_text(str(data["seconds"]))
            elif kind == "verify":
                start = time.monotonic()
                entry = verify(repo, roots, data["upload_id"])
                assert entry["sha256"] == reference(entry["size_bytes"], data["seed"])
                entry["verify_both_replicas_seconds"] = time.monotonic() - start
                verified.append(entry)
                save()
                return entry
            elif kind == "cleanup-status":
                session = repo.upload_status(UUID(data["upload_id"]))
                assert session["state"] == "ABORTED" and not session["cleanup_pending"]
                held = sql(
                    repo,
                    "SELECT coalesce(sum(charged_bytes),0) n FROM allocations "
                    "WHERE chunk_id IN (SELECT chunk_id FROM upload_parts WHERE upload_id=:id) "
                    "AND state!='DELETED'",
                    id=UUID(data["upload_id"]),
                )[0]["n"]
                assert held == 0
            elif kind == "snapshot":
                return save()
            elif kind == "finish":
                save()
                stopping.set()
            else:
                raise ValueError("Unknown C5 action")
            return {"ok": True}

        @control_app.post("/api/v2/_c5/control")
        async def control(request: Request):
            return await asyncio.to_thread(action, await request.json())

        @control_app.get("/api/v2/_c5/reference")
        async def expected(size: int, seed: int):
            assert 0 <= size <= 10 * 1024**3 and 0 <= seed < 256
            return {"sha256": await asyncio.to_thread(reference, size, seed)}

        @control_app.post("/api/v2/_c5/report")
        async def receive_report(request: Request):
            report.update(await request.json())
            save()
            return {"ok": True}

        @control_app.get("/api/v2/_c5/status")
        async def status():
            return {
                "phase": (control_root / "phase").read_text(),
                "schema": repo.schema,
                "report": report,
                "verified": verified,
                "resources": sampler.snapshot(),
            }

        controller = stack.enter_context(serve(control_app))
        origin, internal, audit = stack.enter_context(
            ingress(
                apis,
                internals,
                transfers,
                directory,
                port=18080,
                bind="0.0.0.0",
                control=int(controller.rsplit(":", 1)[1]),
            )
        )
        for transfer in transfers:
            transfer.env.update(V2_API_URL=origin, V2_INTERNAL_URL=internal)
            transfer.start()
        worker.start()
        topology = {
            "schema": repo.schema,
            "chunk_size": CHUNK,
            "replication_factor": 2,
            "services": {
                label: {"pid": s.process.pid, "port": s.port} for label, s in services.items()
            },
            "disk": shutil.disk_usage(directory)._asdict(),
            "note": "loopback processes; Nginx RR; probes/faults test-only",
        }
        (destination / "topology.json").write_text(json.dumps(topology, indent=2))
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda *_: stopping.set())
        print(json.dumps({"ready": True, "schema": repo.schema, "port": 18080}), flush=True)
        try:
            while not stopping.wait(1):
                assert sampler.thread.is_alive(), "Resource sampler stopped"
                if not restarting.is_set():
                    for label, service in list(services.items()):
                        assert service.process.poll() is None, (
                            f"{label}: {service.path.read_text()}"
                        )
                if int(time.monotonic()) % 10 == 0:
                    save()
        finally:
            save()
            for path in directory.glob("*.log"):
                shutil.copyfile(path, destination / path.name)
            shutil.copyfile(audit, destination / "requests.jsonl")
            events = control_root / "worker-events.jsonl"
            if events.exists():
                shutil.copyfile(events, destination / events.name)
            faults = {p.name: p.read_text() for p in control_root.glob("lost-*.json")}
            (destination / "faults.json").write_text(json.dumps(faults, indent=2))
        print(json.dumps({"stopped": True, "schema": repo.schema}), flush=True)


if __name__ == "__main__":
    main()
