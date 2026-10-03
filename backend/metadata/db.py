from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker


class Base(DeclarativeBase):
    pass


def create_database(database_url: str):
    engine = create_engine(database_url, pool_pre_ping=True, connect_args={"connect_timeout": 5})
    return engine, sessionmaker(bind=engine, expire_on_commit=False)
