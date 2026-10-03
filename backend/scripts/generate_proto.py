"""Generate stubs from the sole implementation wire contract, then verify imports."""

import importlib
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "generated"


def main():
    OUTPUT.mkdir(exist_ok=True)
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            "-I",
            str(ROOT / "contracts"),
            f"--python_out={OUTPUT}",
            f"--grpc_python_out={OUTPUT}",
            str(ROOT / "contracts/storage.proto"),
        ],
        check=True,
    )
    sys.path.insert(0, str(OUTPUT))
    proto = importlib.import_module("storage_pb2")
    importlib.import_module("storage_pb2_grpc")
    methods = proto.DESCRIPTOR.services_by_name["StorageService"].methods
    assert len(methods) == 4 and all(
        not m.client_streaming and not m.server_streaming for m in methods
    )
    print("Generated/imported four unary RPCs successfully")


if __name__ == "__main__":
    main()
