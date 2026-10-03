"""Phased Storage smoke; preserve the manifest across server/container restart."""

import argparse
import hashlib
import json
from pathlib import Path
from uuid import uuid4

import grpc
import storage_pb2 as pb
import storage_pb2_grpc

from common.config import TransferSettings
from storage.validation import validate_chunk_id


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def fixture(size):
    return (bytes(range(256)) * ((size + 255) // 256))[:size]


def missing(stub, chunk_id, timeout):
    try:
        stub.GetChunk(pb.GetChunkRequest(chunk_id=chunk_id), timeout=timeout)
    except grpc.RpcError as error:
        require(error.code() == grpc.StatusCode.NOT_FOUND, f"Expected NOT_FOUND: {error.code()}")
    else:
        raise RuntimeError("Expected missing chunk")


def run(args):
    settings = TransferSettings(_env_file=None)
    if args.mode == "store":
        require(args.target and args.node_id, "store requires --target and --node-id")
        size = args.size
        require(1 <= size <= settings.chunk_size_bytes, "Fixture outside chunk size limit")
        chunk_id = validate_chunk_id(args.chunk_id or str(uuid4()))
        state = {
            "target": args.target,
            "node_id": args.node_id,
            "chunk_id": chunk_id,
            "size": size,
            "checksum": hashlib.sha256(fixture(size)).hexdigest(),
        }
    else:
        state = json.loads(args.state.read_text())
        validate_chunk_id(state["chunk_id"])
    target = args.target or state["target"]
    node_id = args.node_id or state["node_id"]
    if args.mode != "absent":
        require(target == state["target"] and node_id == state["node_id"], "Manifest node mismatch")
    else:
        require(args.target and args.node_id, "absent requires the other --target and --node-id")
        require(target != state["target"] and node_id != state["node_id"], "Use a different node")
    options = settings.grpc_options() + [("grpc.enable_retries", 0)]
    with grpc.insecure_channel(target, options=options) as channel:
        grpc.channel_ready_future(channel).result(timeout=args.timeout)
        stub = storage_pb2_grpc.StorageServiceStub(channel)
        health = stub.HealthCheck(pb.HealthCheckRequest(), timeout=args.timeout)
        require(health.node_id == node_id and health.storage_writable, "Health/identity mismatch")
        if args.mode == "store":
            missing(stub, state["chunk_id"], args.timeout)
            state.update(baseline=health.used_bytes, failure_domain=health.failure_domain)
            # Persist attempted ID before RPC, so even unknown outcomes can be cleaned up.
            args.state.parent.mkdir(parents=True, exist_ok=True)
            with args.state.open("x") as handle:
                json.dump(state, handle, indent=2)
            request = pb.StoreChunkRequest(
                chunk_id=state["chunk_id"],
                data=fixture(state["size"]),
                checksum_sha256=state["checksum"],
            )
            for duplicate in (False, True):
                ack = stub.StoreChunk(request, timeout=args.timeout)
                require(
                    ack.chunk_id == state["chunk_id"]
                    and ack.size_bytes == state["size"]
                    and ack.checksum_sha256 == state["checksum"]
                    and ack.already_existed == duplicate,
                    "Store ack mismatch",
                )
        elif args.mode == "verify":
            response = stub.GetChunk(
                pb.GetChunkRequest(chunk_id=state["chunk_id"]), timeout=args.timeout
            )
            require(
                response.chunk_id == state["chunk_id"]
                and response.data == fixture(state["size"])
                and response.checksum_sha256 == state["checksum"]
                and hashlib.sha256(response.data).hexdigest() == state["checksum"],
                "Get bytes/hash mismatch",
            )
        elif args.mode == "absent":
            missing(stub, state["chunk_id"], args.timeout)
        else:
            for expected in (True, False):
                response = stub.DeleteChunk(
                    pb.DeleteChunkRequest(chunk_id=state["chunk_id"]), timeout=args.timeout
                )
                require(
                    response.chunk_id == state["chunk_id"] and response.existed == expected,
                    "Delete ack mismatch",
                )
            missing(stub, state["chunk_id"], args.timeout)
        after = stub.HealthCheck(pb.HealthCheckRequest(), timeout=args.timeout)
        if args.mode != "absent":
            require(after.failure_domain == state["failure_domain"], "Failure domain changed")
            expected = state["baseline"] + (0 if args.mode == "delete" else state["size"])
            require(after.used_bytes == expected, "used_bytes did not match baseline/fixture")
        else:
            require(after.used_bytes == health.used_bytes, "Other node accounting changed")
    print(json.dumps({**state, "mode": args.mode, "result": "PASS", "target": target}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["store", "verify", "absent", "delete"])
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--target")
    parser.add_argument("--node-id")
    parser.add_argument("--chunk-id")
    parser.add_argument("--size", type=int, default=2097152)
    parser.add_argument("--timeout", type=float, default=5)
    args = parser.parse_args()
    require(args.timeout > 0, "Timeout must be positive")
    run(args)


if __name__ == "__main__":
    main()
