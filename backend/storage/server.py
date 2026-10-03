import logging
import signal
from concurrent.futures import ThreadPoolExecutor

import grpc
import storage_pb2_grpc

from common.logging import configure_logging
from storage.config import StorageSettings
from storage.service import StorageService


def create_server(settings: StorageSettings):
    server = grpc.server(ThreadPoolExecutor(max_workers=4), options=settings.grpc_options())
    storage_pb2_grpc.add_StorageServiceServicer_to_server(StorageService(settings), server)
    address = f"{settings.grpc_bind_host}:{settings.grpc_port}"
    if not server.add_insecure_port(address):
        raise RuntimeError(f"Cannot bind gRPC server to {address}")
    return server


def main():
    settings = StorageSettings()
    configure_logging(settings.log_level)
    server = create_server(settings)

    def stop(signum, frame):
        server.stop(grace=5)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    server.start()
    logging.getLogger(__name__).info(
        "Storage node %s listening on %s:%s",
        settings.node_id,
        settings.grpc_bind_host,
        settings.grpc_port,
    )
    server.wait_for_termination()


if __name__ == "__main__":
    main()
