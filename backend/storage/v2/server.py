"""Separate V2 entry point. Provisioning requires an explicit administrative flag."""

import argparse
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc
import v2_storage_pb2_grpc as rpc

from storage.v2.ledger import Ledger, Scope
from storage.v2.service import StorageService
from storage.v2.streaming import StreamingStore


def create_server(ledger, *, address="127.0.0.1:0", failure_domain="local"):
    executor = ThreadPoolExecutor(max_workers=24)
    server = grpc.server(
        executor,
        options=[
            ("grpc.max_receive_message_length", 300000),
            ("grpc.max_send_message_length", 300000),
        ],
        maximum_concurrent_rpcs=24,
    )
    rpc.add_StorageServiceServicer_to_server(
        StorageService(StreamingStore(ledger), failure_domain), server
    )
    port = server.add_insecure_port(address)
    if not port:
        executor.shutdown(wait=True)
        raise RuntimeError("Unable to bind V2 server")
    return server, port, executor


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--provision", action="store_true")
    args = parser.parse_args()
    scope = Scope(
        os.environ["V2_NAMESPACE_ID"],
        os.environ["V2_RECOVERY_EPOCH"],
        os.environ["NODE_ID"],
        os.environ["V2_NODE_INCARNATION"],
    )
    ledger = Ledger(Path(os.environ["V2_DATA_DIR"]), scope, provision=args.provision)
    server, _, executor = create_server(
        ledger,
        address=os.environ.get("V2_GRPC_BIND", "127.0.0.1:50061"),
        failure_domain=os.environ["FAILURE_DOMAIN"],
    )
    try:
        server.start()
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop(5).wait()
        executor.shutdown(wait=True)
        ledger.close()


if __name__ == "__main__":
    main()
