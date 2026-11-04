"""Generate V2 stubs alongside V1, without changing the V1 wire contract."""

import subprocess
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            "-I",
            str(root / "contracts"),
            f"--python_out={root / 'generated'}",
            f"--grpc_python_out={root / 'generated'}",
            str(root / "contracts/v2_storage.proto"),
        ],
        check=True,
    )


if __name__ == "__main__":
    main()
