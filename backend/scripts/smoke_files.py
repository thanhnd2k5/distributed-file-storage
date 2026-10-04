"""RF=2 HTTP smoke. Live files remain with a manifest until M4 provides DELETE."""

import argparse
import hashlib
import json
import urllib.error
import urllib.request
from pathlib import Path
from uuid import uuid4

from scripts.smoke_metadata import require

BASE = "http://127.0.0.1:8000/api/v1"
SIZE = 10 * 1024 * 1024 + 17


def payload(size):
    return (bytes(range(256)) * ((size + 255) // 256))[:size]


def request(base, path, *, data=None, headers=None):
    # An HTTP transfer timeout, independent of Uvicorn's keep-alive setting.
    req = urllib.request.Request(base + path, data=data, headers=headers or {})
    with urllib.request.urlopen(req, timeout=300) as response:
        return response.status, response.headers, response.read()


def get(base, path):
    status, _, body = request(base, path)
    require(status == 200, f"{path}: expected 200")
    return json.loads(body)


def upload(base, data, name):
    boundary = "dfs-" + uuid4().hex
    body = (
        (
            f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{name}"\r\n'
            "Content-Type: application/octet-stream\r\n\r\n"
        ).encode()
        + data
        + f"\r\n--{boundary}--\r\n".encode()
    )
    status, headers, result = request(
        base,
        "/files",
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    require(status == 201, "Upload must return 201")
    summary = json.loads(result)
    require(headers["Location"] == f"/api/v1/files/{summary['file_id']}", "Invalid Location")
    return summary


def mapping(chunks):
    return [
        {
            **{
                key: chunk[key]
                for key in ("chunk_id", "chunk_index", "size_bytes", "checksum_sha256")
            },
            "nodes": sorted(replica["node_id"] for replica in chunk["replicas"]),
            "domains": sorted({replica["failure_domain"] for replica in chunk["replicas"]}),
        }
        for chunk in chunks
    ]


def verify(base, manifest, down_node=None):
    summary = manifest["file"]
    file_id = summary["file_id"]
    detail = get(base, f"/files/{file_id}")
    for key, value in summary.items():
        require(detail[key] == value, f"File snapshot changed: {key}")
    chunks = get(base, f"/files/{file_id}/chunks")["items"]
    require(mapping(chunks) == manifest["chunks"], "Chunk mapping changed")
    require(len(chunks) == summary["total_chunks"], "Wrong chunk count")
    data = payload(summary["size_bytes"])
    chunk_size = summary["chunk_size_bytes"]
    cluster = get(base, "/cluster")
    domain_target = min(summary["replication_factor"], cluster["configured_failure_domains"])
    under = domain_degraded = 0
    used_nodes = set()
    for index, chunk in enumerate(chunks):
        expected = data[index * chunk_size : (index + 1) * chunk_size]
        require(chunk["chunk_index"] == index, "Non-contiguous indexes")
        require(chunk["size_bytes"] == len(expected), "Wrong final chunk size")
        require(chunk["checksum_sha256"] == hashlib.sha256(expected).hexdigest(), "Wrong chunk SHA")
        replicas = chunk["replicas"]
        require(
            len(replicas) == 2 and len({r["node_id"] for r in replicas}) == 2, "Expected exact RF=2"
        )
        require(
            all(r["status"] == "VERIFIED" and not r["cleanup_pending"] for r in replicas), "Bad ack"
        )
        used_nodes.update(r["node_id"] for r in replicas)
        live = [r for r in replicas if r["node_id"] != down_node]
        expected_live = len(live)
        require(all(r["node_status"] == "ACTIVE" for r in live), "Unexpected inactive source")
        if down_node:
            require(
                all(r["node_status"] == "DOWN" for r in replicas if r["node_id"] == down_node),
                "Node not DOWN",
            )
        under += expected_live < 2
        live_domains = len({r["failure_domain"] for r in live})
        domain_degraded += live_domains < domain_target
        require(chunk["live_failure_domain_count"] == live_domains, "Wrong live domain count")
        require(chunk["domain_degraded"] == (live_domains < domain_target), "Wrong domain flag")
        require(chunk["live_replica_count"] == expected_live, "Wrong live count")
        require(
            chunk["state"] == ("UNDER_REPLICATED" if expected_live < 2 else "AVAILABLE"),
            "Wrong state",
        )
    require(len(used_nodes) == 3, "Fixture must exercise all three Storage nodes")
    require(detail["known_readable"] and detail["unavailable_chunks"] == 0, "File not readable")
    require(detail["under_replicated_chunks"] == under, "Wrong file under-replication count")
    require(detail["domain_degraded_chunks"] == domain_degraded, "Wrong domain count")
    require(
        detail["over_replicated_chunks"] == detail["cleanup_pending_replicas"] == 0,
        "Unexpected pending/extra replica",
    )
    found = False
    for offset in range(0, 10000, 100):
        page = get(base, f"/files?limit=100&offset={offset}")
        found |= any(item == summary for item in page["items"])
        if found or offset + 100 >= page["total"]:
            break
    require(found, "File missing from list")
    require(
        cluster["files_available"] >= 1 and cluster["under_replicated_chunks"] >= under,
        "Wrong cluster counters",
    )
    status, headers, actual = request(base, f"/files/{file_id}/download")
    require(status == 200 and actual == data, "Downloaded bytes changed")
    require(headers.get_content_type() == "application/octet-stream", "Wrong binary type")
    require(int(headers["Content-Length"]) == len(data), "Wrong content length")
    require(
        headers["X-File-Checksum-SHA256"] == summary["checksum_sha256"], "Wrong checksum header"
    )
    require(hashlib.sha256(actual).hexdigest() == summary["checksum_sha256"], "Wrong full SHA")
    return {
        "file_id": file_id,
        "sha256": summary["checksum_sha256"],
        "chunks": len(chunks),
        "under_replicated_chunks": under,
        "domain_degraded_chunks": domain_degraded,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("create", "verify"))
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--base", default=BASE)
    parser.add_argument("--down-node")
    args = parser.parse_args()
    if args.mode == "create":
        require(not args.manifest.exists(), "Manifest already exists; refusing another upload")
        try:
            summary = upload(args.base, payload(SIZE), "m3-smoke-" + uuid4().hex + ".bin")
        except urllib.error.HTTPError as error:
            # A failed upload can own durable attempted mappings too.
            args.manifest.write_text(
                json.dumps({"upload_error": json.loads(error.read())}, indent=2), encoding="utf-8"
            )
            raise
        # Persist the ID before subsequent checks, even if those checks fail.
        manifest = {"file": summary}
        args.manifest.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        manifest["chunks"] = mapping(get(args.base, f"/files/{summary['file_id']}/chunks")["items"])
        args.manifest.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    else:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    print(json.dumps({"result": "passed", **verify(args.base, manifest, args.down_node)}))


if __name__ == "__main__":
    main()
