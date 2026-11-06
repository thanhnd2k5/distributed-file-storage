"""Compile and inspect the isolated V2 wire contract without touching V1 stubs."""

import importlib
import sys
import tempfile
from pathlib import Path

from grpc_tools import protoc

ROOT = Path(__file__).resolve().parents[1]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    scratch = (ROOT / ".runtime" / "contract-checks").resolve()
    require(scratch.is_relative_to(ROOT.resolve()), "Scratch path escaped backend workspace")
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="dfs-v2-contract-", dir=scratch) as output:
        require(Path(output).resolve().is_relative_to(scratch), "Unsafe cleanup target")
        result = protoc.main(
            [
                "grpc_tools.protoc",
                f"-I{ROOT / 'contracts'}",
                f"--python_out={output}",
                f"--grpc_python_out={output}",
                str(ROOT / "contracts/v2_storage.proto"),
            ]
        )
        require(result == 0, f"protoc failed: {result}")
        sys.path.insert(0, output)
        try:
            proto = importlib.import_module("v2_storage_pb2")
            importlib.import_module("v2_storage_pb2_grpc")
            require(proto.DESCRIPTOR.package == "dfs.storage.v2", "Wrong V2 package")
            service = proto.DESCRIPTOR.services_by_name["StorageService"]
            expected = {
                "PrepareAllocation": (False, False),
                "StoreChunk": (True, False),
                "GetChunk": (False, True),
                "InspectAllocation": (False, False),
                "DeleteAllocation": (False, False),
                "HealthCheck": (False, False),
                "ListAllocations": (False, False),
            }
            actual = {
                method.name: (method.client_streaming, method.server_streaming)
                for method in service.methods
            }
            require(actual == expected, f"Unexpected RPC directions: {actual}")

            record = proto.AllocationRecord()
            require(not record.HasField("actual_size_bytes"), "Missing scalar presence")
            record.actual_size_bytes = 0
            require(record.HasField("actual_size_bytes"), "Zero must differ from absent")
            require(not record.HasField("actual_checksum_sha256"), "Hash must be optional")
            require(record.state == proto.ALLOCATION_STATE_UNSPECIFIED, "Unsafe enum default")
            listing = proto.ListAllocationsRequest()
            require(not listing.HasField("upper_sequence"), "Scan bound needs presence")

            store = proto.StoreChunkFrame(
                block=proto.DataBlock(offset=2**53 + 1, data=b"x" * 262144)
            )
            encoded = store.SerializeToString()
            decoded = proto.StoreChunkFrame.FromString(encoded)
            require(decoded.block.offset == 2**53 + 1, "Wire offset lost precision")
            require(decoded.block.data == b"x" * 262144, "Block bytes changed")
            require(decoded.WhichOneof("payload") == "block", "Wrong frame payload")
            require(len(encoded) < 263168, "Frame overhead exceeds 1 KiB budget")
            decoded.header.generation = 1
            require(decoded.WhichOneof("payload") == "header", "Frame oneof not exclusive")

            read = proto.GetChunkFrame(
                footer=proto.GetChunkFooter(
                    actual_size_bytes=8388608, actual_checksum_sha256="a" * 64
                )
            )
            require(
                proto.GetChunkFrame.FromString(read.SerializeToString()) == read,
                "Read footer roundtrip failed",
            )
            require(
                proto.AllocationRecord.DESCRIPTOR.fields_by_name["cleanup_complete"].number == 8,
                "Cleanup evidence wire field changed",
            )
            print(
                "V2 contract passed: compile/import, 7 RPC directions, presence, frame roundtrips"
            )
            print(f"256 KiB block wire size: {len(encoded)} bytes")
            print("No Storage server, HTTP, PostgreSQL, or filesystem persistence was exercised")
        finally:
            sys.path.remove(output)
            for name in ("v2_storage_pb2_grpc", "v2_storage_pb2"):
                sys.modules.pop(name, None)


if __name__ == "__main__":
    main()
