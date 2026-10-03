import os
import uuid
from contextlib import ExitStack

import grpc
import pytest
import storage_pb2_grpc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from metadata import models  # noqa: F401 -- register all tables before create_all
from metadata.db import Base
from storage.config import StorageSettings
from storage.server import create_server


@pytest.fixture
def database():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("PostgreSQL integration: run the Compose tests service")
    schema = f"test_base_{uuid.uuid4().hex}"
    engine = create_engine(url, connect_args={"connect_timeout": 5})
    with engine.begin() as connection:
        connection.execute(CreateSchema(schema))
    scoped_engine = engine.execution_options(schema_translate_map={None: schema})
    try:
        Base.metadata.create_all(scoped_engine)
        yield sessionmaker(bind=scoped_engine, expire_on_commit=False), url
    finally:
        # Only the unique schema created by this test is removed; app tables are untouched.
        with engine.begin() as connection:
            connection.execute(DropSchema(schema, cascade=True))
        engine.dispose()


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
