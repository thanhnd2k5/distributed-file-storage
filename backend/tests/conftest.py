from contextlib import ExitStack

import grpc
import pytest
import storage_pb2_grpc

from storage.config import StorageSettings
from storage.server import create_server


@pytest.fixture
def storage_node_factory(tmp_path):
    with ExitStack() as resources:

        def create(node_id="test-node", failure_domain="test"):
            settings = StorageSettings(
                _env_file=None, node_id=node_id, failure_domain=failure_domain, data_dir=tmp_path
            )
            server = create_server(settings, bind_address="127.0.0.1:0")
            server.start()
            resources.callback(lambda: server.stop(0).wait())
            channel = resources.enter_context(
                grpc.insecure_channel(f"127.0.0.1:{server.port}", options=settings.grpc_options())
            )
            grpc.channel_ready_future(channel).result(timeout=5)
            return storage_pb2_grpc.StorageServiceStub(channel), server.service, tmp_path

        yield create
