import hashlib
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from threading import Event, Lock
from time import monotonic, sleep

import grpc
import pytest

from metadata.config import MetadataSettings, NodeConfig
from metadata.storage_client import StorageClient, StorageRpcError
from storage.config import StorageSettings
from storage.server import create_server
from storage.service import StorageService

CHUNK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
DATA = b"rpc-payload"
HASH = hashlib.sha256(DATA).hexdigest()


def wait_until(predicate, timeout=3):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate():
            return
        sleep(0.01)
    raise AssertionError("Timed out waiting for RPC/lifecycle observation")


class FaultStorage(StorageService):
    """Faults surround production handlers; filesystem semantics stay real."""

    def __init__(self, settings):
        super().__init__(settings)
        self.requests = {"StoreChunk": [], "GetChunk": []}
        self.deadlines = []
        self.errors = {"StoreChunk": [], "GetChunk": []}
        self.lost_ack = None
        self.store_mutation = None
        self.get_mutation = None
        self.block = False
        self.entered = Event()
        self.finished = Event()
        self.release = Event()
        self.on_entry = lambda: None
        self.mutex = Lock()

    def _enter(self, method, request, context):
        with self.mutex:
            self.requests[method].append(request.SerializeToString())
            self.deadlines.append(context.time_remaining())
            error = self.errors[method].pop(0) if self.errors[method] else None
        self.on_entry()
        self.entered.set()
        if self.block:
            while context.is_active() and not self.release.wait(0.01):
                pass
            if not context.is_active():
                context.abort(grpc.StatusCode.CANCELLED, "caller stopped")
        if error is not None:
            context.abort(error, "private RPC diagnostics")

    def StoreChunk(self, request, context):
        try:
            self._enter("StoreChunk", request, context)
            response = super().StoreChunk(request, context)
            if self.lost_ack is not None and len(self.requests["StoreChunk"]) == 1:
                if self.lost_ack == "actual_deadline":
                    while context.is_active():
                        self.release.wait(0.01)
                    context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "ack lost after commit")
                context.abort(self.lost_ack, "ack lost after production commit")
            if self.store_mutation is not None:
                self.store_mutation(response)
            return response
        finally:
            self.finished.set()

    def GetChunk(self, request, context):
        self._enter("GetChunk", request, context)
        response = super().GetChunk(request, context)
        if self.get_mutation is not None:
            self.get_mutation(response)
        return response


@pytest.fixture
def rpc_nodes(tmp_path):
    with ExitStack() as resources:

        def create(node_id="node-1", *, faults=False, chunk_size=2 * 1024 * 1024):
            directory = tmp_path / node_id
            settings = StorageSettings(
                _env_file=None,
                node_id=node_id,
                failure_domain="test",
                data_dir=directory,
                chunk_size_bytes=chunk_size,
            )
            service = FaultStorage(settings) if faults else StorageService(settings)
            server = create_server(settings, service=service, bind_address="127.0.0.1:0")
            server.start()
            resources.callback(lambda: server.stop(0).wait())
            if faults:
                resources.callback(service.release.set)
            config = NodeConfig(
                node_id=node_id, host="127.0.0.1", port=server.port, failure_domain="test"
            )
            return config, service, directory

        yield create


def settings_for(nodes, **overrides):
    return MetadataSettings(
        **(
            dict(
                _env_file=None,
                database_url="postgresql+psycopg://dfs:dfs@127.0.0.1:1/dfs",
                storage_nodes_json=nodes,
                replication_factor=1,
                chunk_rpc_timeout_seconds=0.5,
            )
            | overrides
        )
    )


@pytest.fixture
def clients():
    with ExitStack() as resources:

        def create(configs, **overrides):
            client = StorageClient(settings_for(configs, **overrides))
            client.start()
            resources.callback(client.close)
            for channel in client._channels.values():
                grpc.channel_ready_future(channel).result(timeout=3)
            return client

        yield create


def test_production_two_mib_store_get_duplicate_and_node_isolation(rpc_nodes, clients):
    first, service, directory = rpc_nodes("node-1")
    second, _, second_dir = rpc_nodes("node-2")
    client = clients([first, second])
    data = bytes(range(256)) * 8192
    checksum = hashlib.sha256(data).hexdigest()
    ack = client.store_chunk(first.node_id, CHUNK_ID, data, checksum)
    assert (ack.chunk_id, ack.size_bytes, ack.checksum_sha256, ack.already_existed) == (
        CHUNK_ID,
        len(data),
        checksum,
        False,
    )
    assert client.get_chunk(first.node_id, CHUNK_ID, len(data), checksum) == data
    duplicate = client.store_chunk(first.node_id, CHUNK_ID, data, checksum)
    assert duplicate.already_existed and service.store.used_bytes == len(data)
    assert (directory / f"{CHUNK_ID}.chunk").read_bytes() == data
    assert not (second_dir / f"{CHUNK_ID}.chunk").exists()
    with pytest.raises(StorageRpcError) as caught:
        client.get_chunk(second.node_id, CHUNK_ID, len(data), checksum)
    assert caught.value.replica_status == "MISSING" and caught.value.attempts == 1


def test_old_chunk_can_be_larger_than_current_metadata_default(rpc_nodes, clients):
    config, _, _ = rpc_nodes(chunk_size=4 * 1024 * 1024)
    client = clients([config], chunk_size_bytes=256 * 1024)
    data = b"old" * (1024 * 1024)
    checksum = hashlib.sha256(data).hexdigest()
    assert client.store_chunk(config.node_id, CHUNK_ID, data, checksum).size_bytes == len(data)
    assert client.get_chunk(config.node_id, CHUNK_ID, len(data), checksum) == data


@pytest.mark.parametrize(
    "status", [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED, "actual_deadline"]
)
def test_lost_store_ack_retries_same_payload_and_commits_only_once(rpc_nodes, clients, status):
    config, service, directory = rpc_nodes(faults=True)
    service.lost_ack = status
    client = clients([config], chunk_rpc_timeout_seconds=0.1)
    ack = client.store_chunk(config.node_id, CHUNK_ID, DATA, HASH)
    assert ack.already_existed
    assert service.requests["StoreChunk"][0] == service.requests["StoreChunk"][1]
    assert len(service.requests["StoreChunk"]) == 2 and service.store.used_bytes == len(DATA)
    assert (directory / f"{CHUNK_ID}.chunk").read_bytes() == DATA
    assert all(0 < deadline <= 0.6 for deadline in service.deadlines)


@pytest.mark.parametrize("method", ["StoreChunk", "GetChunk"])
@pytest.mark.parametrize("status", [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED])
@pytest.mark.parametrize("attempts", [1, 2])
def test_transient_failures_stop_at_configured_attempt_count(
    rpc_nodes,
    clients,
    method,
    status,
    attempts,
):
    config, service, _ = rpc_nodes(faults=True)
    service.errors[method] = [status] * 3
    client = clients([config], rpc_max_attempts=attempts)
    with pytest.raises(StorageRpcError) as caught:
        if method == "StoreChunk":
            client.store_chunk(config.node_id, CHUNK_ID, DATA, HASH)
        else:
            client.get_chunk(config.node_id, CHUNK_ID, len(DATA), HASH)
    assert caught.value.grpc_status == status and caught.value.attempts == attempts
    assert caught.value.replica_status is None and not caught.value.cancelled
    assert len(service.requests[method]) == attempts
    assert "private RPC diagnostics" not in str(caught.value)


@pytest.mark.parametrize("method", ["StoreChunk", "GetChunk"])
@pytest.mark.parametrize(
    "status",
    [
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.ALREADY_EXISTS,
        grpc.StatusCode.NOT_FOUND,
        grpc.StatusCode.DATA_LOSS,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.FAILED_PRECONDITION,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.CANCELLED,
    ],
)
def test_nontransient_codes_are_not_retried_and_classify_only_get_outcomes(
    rpc_nodes,
    clients,
    caplog,
    method,
    status,
):
    config, service, _ = rpc_nodes(faults=True)
    service.errors[method] = [status]
    client = clients([config])
    with pytest.raises(StorageRpcError) as caught:
        if method == "StoreChunk":
            client.store_chunk(config.node_id, CHUNK_ID, DATA, HASH)
        else:
            client.get_chunk(config.node_id, CHUNK_ID, len(DATA), HASH)
    error = caught.value
    expected = (
        {grpc.StatusCode.NOT_FOUND: "MISSING", grpc.StatusCode.DATA_LOSS: "CORRUPTED"}.get(status)
        if method == "GetChunk"
        else None
    )
    assert error.grpc_status == status and error.attempts == 1 and error.replica_status == expected
    assert error.cancelled is (status == grpc.StatusCode.CANCELLED)
    assert len(service.requests[method]) == 1
    assert "private RPC diagnostics" not in str(error) + caplog.text
    if status == grpc.StatusCode.INTERNAL:
        assert "(INTERNAL)" in caplog.text


@pytest.mark.parametrize(
    "field, value",
    [("chunk_id", str(uuid.uuid4())), ("size_bytes", 999), ("checksum_sha256", "f" * 64)],
)
@pytest.mark.parametrize("already_existed", [False, True])
def test_bad_store_ack_is_not_verified_or_retried(
    rpc_nodes, clients, field, value, already_existed
):
    config, service, _ = rpc_nodes(faults=True)

    def mutate(response):
        setattr(response, field, value)
        response.already_existed = already_existed

    service.store_mutation = mutate
    client = clients([config])
    with pytest.raises(StorageRpcError) as caught:
        client.store_chunk(config.node_id, CHUNK_ID, DATA, HASH)
    assert caught.value.reason == "INVALID_STORE_ACK" and caught.value.replica_status is None
    assert caught.value.grpc_status is None and caught.value.attempts == 1
    assert len(service.requests["StoreChunk"]) == 1


@pytest.mark.parametrize(
    "mutation", ["id", "short", "bad_hash_field", "bad_bytes", "rehash_bad_bytes"]
)
def test_get_verifies_id_size_actual_hash_node_hash_and_db_hash(rpc_nodes, clients, mutation):
    config, service, _ = rpc_nodes(faults=True)
    client = clients([config])
    client.store_chunk(config.node_id, CHUNK_ID, DATA, HASH)

    def mutate(response):
        if mutation == "id":
            response.chunk_id = str(uuid.uuid4())
        elif mutation == "short":
            response.data = DATA[:-1]
        elif mutation == "bad_hash_field":
            response.checksum_sha256 = "f" * 64
        else:
            response.data = b"!" * len(DATA)
            if mutation == "rehash_bad_bytes":
                response.checksum_sha256 = hashlib.sha256(response.data).hexdigest()

    service.get_mutation = mutate
    with pytest.raises(StorageRpcError) as caught:
        client.get_chunk(config.node_id, CHUNK_ID, len(DATA), HASH)
    assert (
        caught.value.reason == "INVALID_GET_RESPONSE" and caught.value.replica_status == "CORRUPTED"
    )
    assert len(service.requests["GetChunk"]) == 1


def test_production_corrupted_bytes_have_valid_node_hash_but_fail_db_integrity(rpc_nodes, clients):
    config, _, directory = rpc_nodes()
    client = clients([config])
    client.store_chunk(config.node_id, CHUNK_ID, DATA, HASH)
    (directory / f"{CHUNK_ID}.chunk").write_bytes(b"!" * len(DATA))
    with pytest.raises(StorageRpcError) as caught:
        client.get_chunk(config.node_id, CHUNK_ID, len(DATA), HASH)
    assert caught.value.replica_status == "CORRUPTED"


@pytest.mark.parametrize("method", ["StoreChunk", "GetChunk"])
def test_actual_deadline_is_bounded_without_infinite_retry(rpc_nodes, clients, method):
    config, service, _ = rpc_nodes(faults=True)
    service.block = True
    client = clients([config], chunk_rpc_timeout_seconds=0.06)
    started = monotonic()
    with pytest.raises(StorageRpcError) as caught:
        if method == "StoreChunk":
            client.store_chunk(config.node_id, CHUNK_ID, DATA, HASH)
        else:
            client.get_chunk(config.node_id, CHUNK_ID, len(DATA), HASH)
    assert caught.value.grpc_status == grpc.StatusCode.DEADLINE_EXCEEDED
    assert caught.value.attempts == 2 and caught.value.replica_status is None
    assert len(service.requests[method]) == 2 and monotonic() - started < 2


def test_cancel_before_call_sends_nothing(rpc_nodes, clients):
    config, service, _ = rpc_nodes(faults=True)
    client = clients([config])
    cancel = Event()
    cancel.set()
    with pytest.raises(StorageRpcError) as caught:
        client.store_chunk(config.node_id, CHUNK_ID, DATA, HASH, cancel=cancel)
    assert caught.value.cancelled and caught.value.attempts == 0
    assert not service.requests["StoreChunk"]


def test_cancel_inflight_rpc_stops_retries_and_does_not_write_late(rpc_nodes, clients):
    config, service, directory = rpc_nodes(faults=True)
    service.block = True
    client = clients([config], chunk_rpc_timeout_seconds=1)
    cancel = Event()
    with ThreadPoolExecutor(max_workers=1) as executor:
        call = executor.submit(
            client.store_chunk, config.node_id, CHUNK_ID, DATA, HASH, cancel=cancel
        )
        assert service.entered.wait(2)
        cancel.set()
        with pytest.raises(StorageRpcError) as caught:
            call.result(timeout=2)
        assert caught.value.cancelled and caught.value.attempts == 1
    service.release.set()
    assert service.finished.wait(2)
    assert len(service.requests["StoreChunk"]) == 1
    assert not (directory / f"{CHUNK_ID}.chunk").exists()


def test_cancel_during_transient_failure_prevents_second_attempt(rpc_nodes, clients):
    config, service, _ = rpc_nodes(faults=True)

    class CancelOnBackoff(Event):
        def wait(self, timeout=None):
            assert timeout == 0.2
            self.set()
            return True

    cancel = CancelOnBackoff()
    service.errors["StoreChunk"] = [grpc.StatusCode.UNAVAILABLE]
    client = clients([config])
    with pytest.raises(StorageRpcError) as caught:
        client.store_chunk(config.node_id, CHUNK_ID, DATA, HASH, cancel=cancel)
    assert caught.value.cancelled and len(service.requests["StoreChunk"]) == 1


def test_close_rejects_new_calls_drains_inflight_and_is_idempotent(rpc_nodes, clients):
    config, service, _ = rpc_nodes(faults=True)
    service.block = True
    client = clients([config], chunk_rpc_timeout_seconds=2)
    with ThreadPoolExecutor(max_workers=3) as executor:
        call = executor.submit(client.store_chunk, config.node_id, CHUNK_ID, DATA, HASH)
        assert service.entered.wait(2)
        closing = executor.submit(client.close)
        wait_until(lambda: not client.running)
        repeated_close = executor.submit(client.close)
        try:
            assert not closing.done()
            with pytest.raises(RuntimeError, match="not running"):
                client.get_chunk(config.node_id, CHUNK_ID, len(DATA), HASH)
        finally:
            service.release.set()
        assert call.result(timeout=3).size_bytes == len(DATA)
        closing.result(timeout=3)
        repeated_close.result(timeout=3)
    client.close()
    assert not client.running
    with pytest.raises(RuntimeError):
        client.start()


def test_unconfigured_node_is_rejected_without_rpc(rpc_nodes, clients):
    config, service, _ = rpc_nodes(faults=True)
    client = clients([config])
    with pytest.raises(ValueError, match="not configured"):
        client.get_chunk("removed-node", CHUNK_ID, len(DATA), HASH)
    assert not service.requests["GetChunk"]


def test_channel_options_ipv6_and_construction_has_no_side_effects(monkeypatch):
    original = grpc.insecure_channel
    calls = []

    def observe(target, *, options):
        calls.append((target, dict(options)))
        return original(target, options=options)

    monkeypatch.setattr("metadata.storage_client.grpc.insecure_channel", observe)
    configs = [
        NodeConfig(node_id="v6", host="::1", port=50051, failure_domain="test"),
        NodeConfig(node_id="bracketed", host="[::1]", port=50052, failure_domain="test"),
    ]
    client = StorageClient(settings_for(configs))
    assert not calls and not client.running
    with pytest.raises(RuntimeError, match="not running"):
        client.get_chunk("v6", CHUNK_ID, len(DATA), HASH)
    try:
        client.start()
        assert [target for target, _ in calls] == ["[::1]:50051", "[::1]:50052"]
        for _, options in calls:
            assert options["grpc.max_send_message_length"] == 8 * 1024 * 1024
            assert options["grpc.max_receive_message_length"] == 8 * 1024 * 1024
            assert options["grpc.enable_retries"] == options["grpc.enable_http_proxy"] == 0
    finally:
        client.close()


def test_partial_client_start_closes_constructed_channels(monkeypatch):
    closed = []

    class Channel:
        def close(self):
            closed.append(self)

    calls = []

    def create_channel(*args, **kwargs):
        calls.append(args[0])
        if len(calls) == 2:
            raise RuntimeError("channel construction failed")
        return Channel()

    monkeypatch.setattr("metadata.storage_client.grpc.insecure_channel", create_channel)
    monkeypatch.setattr(
        "metadata.storage_client.storage_pb2_grpc.StorageServiceStub", lambda _: object()
    )
    configs = [
        NodeConfig(node_id=f"node-{i}", host="storage", port=50050 + i, failure_domain="test")
        for i in (1, 2)
    ]
    client = StorageClient(settings_for(configs))
    with pytest.raises(RuntimeError, match="construction failed"):
        client.start()
    assert len(closed) == 1 and not client.running
    client.close()
    assert len(closed) == 1
