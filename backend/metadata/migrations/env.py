from alembic import context
from sqlalchemy import create_engine, pool

from metadata import models  # noqa: F401
from metadata.config import MetadataSettings
from metadata.db import Base

config = context.config
database_url = MetadataSettings().database_url

if context.is_offline_mode():
    context.configure(url=database_url, target_metadata=Base.metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    engine = create_engine(database_url, poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=Base.metadata)
        with context.begin_transaction():
            context.run_migrations()
