"""Storage entrypoint probes for the disposable C5 fixture only."""

import os
import time
from pathlib import Path
from uuid import uuid4

import grpc

from storage.v2.ledger import Ledger
from storage.v2.server import main
from storage.v2.service import StorageService

root = Path(os.environ["V2_C5_CONTROL"])
original_write = Ledger.write_block
original_delete = StorageService.DeleteAllocation


def write(self, handle, block):
    delay = float((root / "slow-storage").read_text())
    if delay:
        time.sleep(delay)
    return original_write(self, handle, block)


def delete(self, request, context):
    result = original_delete(self, request, context)
    flag = root / "lose-delete-ack"
    try:
        flag.rename(root / f"consumed-delete-{uuid4()}")
    except FileNotFoundError:
        return result
    (root / "delete-ack-lost").write_text(self.ledger.scope.node_id)
    context.abort(grpc.StatusCode.UNAVAILABLE, "C5 injected lost durable Delete ACK")


Ledger.write_block = write
StorageService.DeleteAllocation = delete

if __name__ == "__main__":
    main()
