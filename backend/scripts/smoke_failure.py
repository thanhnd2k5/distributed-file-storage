"""M4 live repair/delete smoke; mutations are restricted to manifest-owned files."""

import argparse
import hashlib
import json
import re
import urllib.error
import urllib.request
from pathlib import Path
from time import monotonic, sleep
from uuid import UUID, uuid4

import grpc
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from metadata.config import MetadataSettings
from metadata.models import Chunk, ChunkReplica, File
from metadata.storage_client import StorageClient, StorageRpcError
from scripts.smoke_files import BASE, SIZE, get, mapping, payload, request, upload, verify
from scripts.smoke_metadata import require


def save(path, state):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=2), encoding="utf-8")
    temporary.replace(path)


def owned(record):
    summary = record["file"]
    UUID(summary["file_id"])
    require(
        re.fullmatch(r"m[34]-smoke-[0-9a-f]{32}\.bin", summary["original_name"]) is not None,
        "Expected a manifest-owned M3/M4 fixture name",
    )
    return summary["file_id"]


def mutate(base, path, method, body=None):
    req = urllib.request.Request(
        base + path,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=300) as response:
        return response.status, json.load(response)


def error(base, path, status, code, *, method="GET", body=None):
    try:
        mutate(base, path, method, body)
    except urllib.error.HTTPError as exc:
        with exc:
            require(exc.code == status, f"{path}: expected {status}, got {exc.code}")
            require(json.load(exc)["error"]["code"] == code, f"{path}: wrong error")
        return
    raise RuntimeError(f"{path}: unexpectedly succeeded")


def download(base, record):
    summary = record["file"]
    status, _, data = request(base, f"/files/{owned(record)}/download")
    require(status == 200 and data == payload(summary["size_bytes"]), "Download bytes changed")
    require(hashlib.sha256(data).hexdigest() == summary["checksum_sha256"], "Full SHA changed")


def actual(settings, chunks, *, absent=False, skip=None):
    client = StorageClient(settings)
    client.start()
    checked = size = 0
    try:
        for chunk in chunks:
            for node_id in chunk["nodes"]:
                if node_id == skip:
                    continue
                try:
                    data = client.get_chunk(
                        node_id, chunk["chunk_id"], chunk["size_bytes"], chunk["checksum_sha256"]
                    )
                except StorageRpcError as exc:
                    require(absent and exc.grpc_status == grpc.StatusCode.NOT_FOUND, str(exc))
                else:
                    require(not absent, "Deleted chunk still exists on Storage")
                    size += len(data)
                checked += 1
    finally:
        client.close()
    return {"rpc_checked_mappings": checked, "verified_bytes": size, "absent": absent}


def scan(base, record, *, node_id=None):
    body = {"file_id": owned(record), "max_chunks": 2}
    if node_id:
        body["node_id"] = node_id
    checked = repaired = 0
    pages = []
    for _ in range(30):
        status, page = mutate(base, "/admin/repair", "POST", body)
        require(status == 200, "Repair must return 200")
        checked += page["checked_chunks"]
        repaired += page["repaired_replicas"]
        pages.append(page)
        if page["remaining_chunks"] == 0:
            return {"checked": checked, "repaired": repaired, "pages": pages}
        require(page["checked_chunks"] > 0, "Repair made no progress in this smoke")
        body["after"] = page["next_after"]
    raise RuntimeError("Repair did not terminate within 30 pages")


def deleting(base, record):
    file_id = owned(record)
    detail = get(base, f"/files/{file_id}")
    require(detail["status"] == "DELETING", "Tombstone changed across restart")
    require(detail["cleanup_pending_replicas"] > 0, "Offline mappings must stay pending")
    error(base, f"/files/{file_id}/download", 409, "FILE_DELETING")
    error(base, "/admin/repair", 409, "FILE_DELETING", method="POST", body={"file_id": file_id})
    return detail


def recover_failed_upload(state):
    failure = state.get("upload_failure")
    if failure is None:
        return
    file_id = failure["error"].get("error", {}).get("details", {}).get("file_id")
    if file_id is None:
        return  # The API did not report an allocated file ID.
    record = {"file": {"file_id": file_id, "original_name": failure["name"]}}
    owned(record)
    for existing in state["files"].values():
        if owned(existing) == file_id:
            require(existing["file"]["original_name"] == failure["name"], "Ownership mismatch")
            return
    require("failed_upload" not in state["files"], "Conflicting failed upload record")
    state["files"]["failed_upload"] = record


def recover_fixture(record, settings):
    """Read durable identity/history before DELETE, even when HTTP hides a tombstone."""
    file_id = owned(record)
    engine = create_engine(settings.database_url, connect_args={"connect_timeout": 5})
    try:
        with Session(engine) as session:
            file = session.get(File, UUID(file_id))
            require(file is not None, "Owned fixture is missing from durable metadata")
            require(file.original_name == record["file"]["original_name"], "Ownership mismatch")
            if "chunks" not in record:
                chunks = session.scalars(
                    select(Chunk).where(Chunk.file_id == file.id).order_by(Chunk.chunk_index)
                ).all()
                record["chunks"] = [
                    {
                        "chunk_id": str(chunk.id),
                        "size_bytes": chunk.size_bytes,
                        "checksum_sha256": chunk.checksum_sha256,
                        "nodes": sorted(
                            session.scalars(
                                select(ChunkReplica.node_id).where(
                                    ChunkReplica.chunk_id == chunk.id
                                )
                            )
                        ),
                    }
                    for chunk in chunks
                ]
    finally:
        engine.dispose()
    return file_id


def finish(base, record, settings, *, checkpoint=None):
    file_id = owned(record)
    recover_fixture(record, settings)
    if checkpoint is not None:
        checkpoint()  # Persist recovered history before the first mutation.
    deadline = monotonic() + 45
    result = None
    while monotonic() < deadline:
        try:
            status, result = mutate(base, f"/files/{file_id}", "DELETE")
        except urllib.error.HTTPError as exc:
            # The cleanup worker shares the nonblocking operation lock.
            with exc:
                require(exc.code == 409, "Unexpected DELETE error")
                require(json.load(exc)["error"]["code"] == "OPERATION_BUSY", "Wrong busy error")
        else:
            require(status in (200, 202), "DELETE must return 200/202")
            if result["status"] == "DELETED":
                break
        sleep(0.5)
    require(
        result is not None and status == 200 and result["status"] == "DELETED",
        "Cleanup did not converge",
    )
    require(result["cleanup_pending_replicas"] == 0, "Cleanup flags remain")
    error(base, f"/files/{file_id}", 404, "FILE_NOT_FOUND")
    error(base, f"/files/{file_id}/download", 404, "FILE_NOT_FOUND")
    engine = create_engine(settings.database_url, connect_args={"connect_timeout": 5})
    try:
        with Session(engine) as session:
            file = session.get(File, UUID(file_id))
            require(file.status == "DELETED" and file.deleted_at is not None, "Missing tombstone")
            require(file.original_name == record["file"]["original_name"], "Ownership mismatch")
            chunks = session.scalars(
                select(Chunk).where(Chunk.file_id == file.id).order_by(Chunk.chunk_index)
            ).all()
            require(
                [str(c.id) for c in chunks] == [c["chunk_id"] for c in record["chunks"]],
                "Chunk history changed",
            )
            durable = []
            recorded_nodes = {c["chunk_id"]: set(c["nodes"]) for c in record["chunks"]}
            for chunk in chunks:
                replicas = session.scalars(
                    select(ChunkReplica).where(ChunkReplica.chunk_id == chunk.id)
                ).all()
                require(
                    recorded_nodes[str(chunk.id)] <= {r.node_id for r in replicas},
                    "Replica history was discarded",
                )
                require(
                    all(r.status == "DELETED" and not r.cleanup_pending for r in replicas),
                    "Durable cleanup did not finish",
                )
                durable.append(
                    {
                        "chunk_id": str(chunk.id),
                        "size_bytes": chunk.size_bytes,
                        "checksum_sha256": chunk.checksum_sha256,
                        "nodes": sorted(r.node_id for r in replicas),
                    }
                )
        record["terminal"] = {
            "delete": result,
            **actual(settings, durable, absent=True),
            "removed_replica_bytes": sum(c["size_bytes"] * len(c["nodes"]) for c in durable),
        }
    finally:
        engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode",
        choices=(
            "create",
            "down-repair",
            "recovered",
            "pending-delete",
            "pending-verify",
            "finish",
            "final",
        ),
    )
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--legacy-manifest", type=Path)
    parser.add_argument("--base", default=BASE)
    args = parser.parse_args()
    settings = MetadataSettings()
    if args.mode not in {"finish", "final"}:
        require(settings.replication_factor == 2, "This smoke requires RF=2")
    if args.mode == "create":
        require(not args.manifest.exists(), "Refusing duplicate uploads into an existing manifest")
        state = {"version": "M4-P6", "files": {}, "evidence": {}}
        save(args.manifest, state)
        if args.legacy_manifest:
            legacy = json.loads(args.legacy_manifest.read_text(encoding="utf-8-sig"))
            owned(legacy)
            verify(args.base, legacy)
            legacy["initial_storage"] = actual(settings, legacy["chunks"])
            state["files"]["legacy"] = legacy
            save(args.manifest, state)
        for label in ("repair", "delete"):
            name = "m4-smoke-" + uuid4().hex + ".bin"
            try:
                summary = upload(args.base, payload(SIZE), name)
            except urllib.error.HTTPError as exc:
                with exc:
                    state["upload_failure"] = {"name": name, "error": json.load(exc)}
                save(args.manifest, state)
                raise
            record = state["files"][label] = {"file": summary}
            save(args.manifest, state)  # ID persisted before checks/next upload.
            record["chunks"] = mapping(get(args.base, f"/files/{owned(record)}/chunks")["items"])
            save(args.manifest, state)
            verify(args.base, record)
            record["initial_storage"] = actual(settings, record["chunks"])
            save(args.manifest, state)
    else:
        state = json.loads(args.manifest.read_text(encoding="utf-8-sig"))
    require(state["version"] == "M4-P6", "Wrong manifest version")
    if args.mode in {"finish", "final"}:
        recover_failed_upload(state)
        save(args.manifest, state)
    if args.mode == "down-repair":
        repair_record = state["files"]["repair"]
        download(args.base, repair_record)
        expected = sum("node-2" in c["nodes"] for c in repair_record["chunks"])
        result = scan(args.base, repair_record)
        require(result["checked"] == repair_record["file"]["total_chunks"], "Scan skipped chunks")
        require(result["repaired"] == expected > 0, "Unexpected repair Store acknowledgments")
        chunks = get(args.base, f"/files/{owned(repair_record)}/chunks")["items"]
        require(all(c["live_replica_count"] == 2 for c in chunks), "RF=2 not restored")
        require(all(c["state"] == "AVAILABLE" for c in chunks), "Repair state invalid")
        repair_record["chunks"] = mapping(chunks)
        result["storage"] = actual(settings, repair_record["chunks"], skip="node-2")
        download(args.base, repair_record)
        state["evidence"][args.mode] = result
    elif args.mode == "recovered":
        repair_record = state["files"]["repair"]
        before = repair_record["chunks"]
        result = scan(args.base, repair_record, node_id="node-2")
        require(result["repaired"] == 0, "Returned bytes must reconcile without Store")
        chunks = get(args.base, f"/files/{owned(repair_record)}/chunks")["items"]
        require(mapping(chunks) == before, "Repair pruned or added a returned mapping")
        extras = sum(len(c["nodes"]) == 3 for c in before)
        detail = get(args.base, f"/files/{owned(repair_record)}")
        require(detail["over_replicated_chunks"] == extras > 0, "Extra replica count invalid")
        require(
            all(
                c["live_replica_count"] == len(m["nodes"])
                for c, m in zip(chunks, before, strict=True)
            ),
            "Live counts invalid",
        )
        require(
            all(r["status"] == "VERIFIED" for c in chunks for r in c["replicas"]),
            "Returned probe not verified",
        )
        result["storage"] = actual(settings, before)
        download(args.base, repair_record)
        state["evidence"][args.mode] = result
    elif args.mode == "pending-delete":
        delete_record = state["files"]["delete"]
        status, result = mutate(args.base, f"/files/{owned(delete_record)}", "DELETE")
        require(status == 202 and result["status"] == "DELETING", "Offline DELETE must be 202")
        state["evidence"][args.mode] = deleting(args.base, delete_record)
    elif args.mode == "pending-verify":
        delete_record = state["files"]["delete"]
        deadline = monotonic() + 40
        while monotonic() < deadline:
            try:
                ready = get(args.base, "/health/ready") == {"status": "READY"}
                nodes = get(args.base, "/nodes")["items"]
            except (urllib.error.URLError, TimeoutError):
                pass  # Only while the Metadata container deliberately restarts.
            else:
                if ready and sum(n["status"] == "ACTIVE" for n in nodes) == 2:
                    break
            sleep(0.5)
        else:
            raise RuntimeError("Restarted Metadata did not become ready with two ACTIVE nodes")
        state["evidence"][args.mode] = deleting(args.base, delete_record)
    elif args.mode == "finish":
        for record in state["files"].values():
            finish(args.base, record, settings, checkpoint=lambda: save(args.manifest, state))
            save(args.manifest, state)
    elif args.mode == "final":
        require(all("terminal" in r for r in state["files"].values()), "Owned fixtures remain")
        cluster = get(args.base, "/cluster")
        require(cluster["cleanup_pending_replicas"] == 0, "Cluster still has pending cleanup")
        require(
            cluster["nodes"] == {"active": 3, "suspected": 0, "down": 0, "disabled": 0},
            "Cluster not restored",
        )
        require(not cluster["operation_busy"], "Operation lock not released")
        state["evidence"][args.mode] = cluster
    save(args.manifest, state)
    print(
        json.dumps(
            {
                "result": "passed",
                "mode": args.mode,
                "files": {k: owned(r) for k, r in state["files"].items()},
                "evidence": state["evidence"].get(args.mode),
            }
        )
    )


if __name__ == "__main__":
    main()
