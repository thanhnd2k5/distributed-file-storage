"""Synchronous data-operation scopes for use in FastAPI's transfer threadpool."""

from contextlib import contextmanager
from threading import get_ident

from sqlalchemy import text


class OperationError(Exception):
    def __init__(self, status_code, code, message):
        super().__init__(message)
        self.status_code = status_code
        self.code = code


class DataOperation:
    """Short private Sessions around RPCs; do not return lazy ORM objects."""

    def __init__(self, sessions, storage_client):
        self._sessions = sessions
        self._client = storage_client
        self._owner = get_ident()
        self._open = True
        self._in_transaction = False

    def _check_scope(self):
        if not self._open or get_ident() != self._owner:
            raise RuntimeError("DataOperation must stay in its owning thread and scope")

    @contextmanager
    def transaction(self):
        self._check_scope()
        if self._in_transaction:
            raise RuntimeError("Nested data-operation transactions are not supported")
        self._in_transaction = True
        try:
            with self._sessions.begin() as session:
                # Match the health worker's bound so shutdown cannot hang on a DB row lock.
                session.execute(text("SET LOCAL statement_timeout = '5s'"))
                yield session
        finally:
            self._in_transaction = False

    def _check_rpc(self):
        self._check_scope()
        if self._in_transaction:
            raise RuntimeError("End the data-operation transaction before calling Storage")

    def store_chunk(self, *args, **kwargs):
        self._check_rpc()
        return self._client.store_chunk(*args, **kwargs)

    def get_chunk(self, *args, **kwargs):
        self._check_rpc()
        return self._client.get_chunk(*args, **kwargs)

    def delete_chunk(self, *args, **kwargs):
        self._check_rpc()
        return self._client.delete_chunk(*args, **kwargs)


def operations_available(state):
    """Share process-liveness admission with readiness; DB checks stay at the boundary."""
    worker = state.health_worker
    client = state.storage_client
    return (
        state.initialized
        and worker is not None
        and worker.running
        and client is not None
        and client.running
    )


@contextmanager
def data_operation(state):
    """Nonblocking admission; hold lock through all DB/RPC work, release on error.

    P3/P5 handlers must keep this entire scope in one threadpool invocation.
    HTTP response sending is outside the download preparation scope.
    """
    if not operations_available(state):
        raise OperationError(503, "METADATA_UNAVAILABLE", "Metadata chưa sẵn sàng.")
    if not state.operation_lock.acquire(blocking=False):
        raise OperationError(409, "OPERATION_BUSY", "Một thao tác dữ liệu khác đang chạy.")
    operation = None
    try:
        client = state.storage_client
        if not operations_available(state):
            raise OperationError(503, "METADATA_UNAVAILABLE", "Metadata chưa sẵn sàng.")
        operation = DataOperation(state.session_factory, client)
        yield operation
    finally:
        if operation is not None:
            operation._open = False
        state.operation_lock.release()
