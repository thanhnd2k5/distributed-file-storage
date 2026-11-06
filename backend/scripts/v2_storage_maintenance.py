"""Offline epoch maintenance; operator must isolate cluster writers beforehand.

Requires the old scope in env to open the volume, and an explicit new epoch.
Every adoption is an individual manifest decision. No bulk auto-adoption/cleanup.
"""

import argparse
import os
from pathlib import Path

from storage.v2.ledger import Ledger, Scope


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["begin", "adopt", "close", "activate"])
    parser.add_argument("--epoch", required=True)
    parser.add_argument("--allocation")
    args = parser.parse_args()
    if args.action in ("adopt", "close") and not args.allocation:
        parser.error("--allocation is required for a manifest decision")
    scope = Scope(
        os.environ["V2_NAMESPACE_ID"],
        os.environ["V2_RECOVERY_EPOCH"],
        os.environ["NODE_ID"],
        os.environ["V2_NODE_INCARNATION"],
    )
    ledger = Ledger(Path(os.environ["V2_DATA_DIR"]), scope, maintenance=True)
    try:
        ledger.begin_recovery(args.epoch)
        if args.action in ("adopt", "close"):
            ledger.adopt(args.allocation, close=args.action == "close")
        elif args.action == "activate":
            ledger.activate_epoch(args.epoch)
    finally:
        ledger.close()


if __name__ == "__main__":
    main()
