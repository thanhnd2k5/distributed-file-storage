import logging
import signal
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock, Thread

import grpc
import storage_pb2_grpc

from common.logging import configure_logging
from storage.config import StorageSettings
from storage.service import StorageService


class _HealthExecutor(grpc.ServerInterceptor):
    def __init__(self, executor):
        self.executor = executor

    def intercept_service(self, continuation, details):
        handler = continuation(details)
        if handler is None or details.method != "/dfs.storage.v1.StorageService/HealthCheck":
            return handler

        def health(request, context):
            return handler.unary_unary(request, context)

        # grpcio's pinned synchronous server supports a per-handler executor.
        # The saturation regression checks this hook through the production factory.
        health.experimental_thread_pool = self.executor
        return grpc.unary_unary_rpc_method_handler(
            health,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


class StorageServer:
    """Own RPC executors and release both after handlers have terminated."""

    def __init__(self, settings, service):
        self.service = service
        self._transfers = ThreadPoolExecutor(max_workers=4, thread_name_prefix="chunk")
        self._health = ThreadPoolExecutor(max_workers=1, thread_name_prefix="health")
        self._server = grpc.server(
            self._transfers,
            options=settings.grpc_options(),
            interceptors=(_HealthExecutor(self._health),),
        )
        storage_pb2_grpc.add_StorageServiceServicer_to_server(service, self._server)
        self._stop_lock = Lock()
        self._stopped = Event()
        self._shutdown_started = False

    def add_insecure_port(self, address):
        return self._server.add_insecure_port(address)

    def start(self):
        return self._server.start()

    def stop(self, grace):
        terminated = self._server.stop(grace)
        with self._stop_lock:
            if not self._shutdown_started:
                self._shutdown_started = True

                def shutdown():
                    terminated.wait()
                    try:
                        self._transfers.shutdown(wait=True)
                        self._health.shutdown(wait=True)
                    finally:
                        self._stopped.set()

                Thread(target=shutdown, name="storage-shutdown", daemon=True).start()
        return self._stopped

    def wait_for_termination(self, timeout=None):
        return self._server.wait_for_termination(timeout)


def create_server(settings: StorageSettings, *, service=None, bind_address=None):
    server = StorageServer(settings, service if service is not None else StorageService(settings))
    address = bind_address or f"{settings.grpc_bind_host}:{settings.grpc_port}"
    try:
        server.port = server.add_insecure_port(address)
        if not server.port:
            raise RuntimeError(f"Cannot bind gRPC server to {address}")
    except Exception:
        server.stop(0).wait()
        raise
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
    try:
        server.wait_for_termination()
    finally:
        server.stop(0).wait()


if __name__ == "__main__":
    main()
