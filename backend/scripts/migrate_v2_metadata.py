"""Explicit V2 migration command, using V2_DATABASE_URL (never V1 DATABASE_URL)."""

import os
from uuid import UUID

from sqlalchemy import create_engine

from metadata.v2.migrate import migrate


def main():
    engine = create_engine(os.environ["V2_DATABASE_URL"], connect_args={"connect_timeout": 5})
    try:
        changed = migrate(
            engine, UUID(os.environ["V2_NAMESPACE_ID"]), UUID(os.environ["V2_RECOVERY_EPOCH"])
        )
        print("V2 migration applied" if changed else "V2 migration already current")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
