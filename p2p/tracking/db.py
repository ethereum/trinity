from pathlib import Path

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import (
    OperationalError,
)
from sqlalchemy.orm.exc import (
    NoResultFound,
    MultipleResultsFound,
)
from sqlalchemy.orm import (
    sessionmaker,
    Session as BaseSession,
)
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
)

from p2p.exceptions import (
    BadDatabaseError,
)


Base = declarative_base()


SCHEMA_VERSION = '2'


class SchemaVersion(Base):
    __tablename__ = 'schema_version'

    id = Column(Integer, primary_key=True)
    version = Column(String, unique=True, nullable=False, index=True)


#
# SQL Based Trackers
#
def get_session(path: Path) -> 'BaseSession':
    # python 3.6 does not support sqlite3.connect(Path)
    is_memory = path.name == ':memory:'

    if is_memory:
        database_uri = 'sqlite:///:memory:'
    else:
        database_uri = f'sqlite:///{path.resolve()}'

    engine = create_engine(database_uri)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def setup_schema(session: BaseSession) -> None:
    Base.metadata.create_all(session.get_bind())
    session.add(SchemaVersion(version=SCHEMA_VERSION))
    session.commit()


def get_schema_version(session: BaseSession) -> str:
    schema_version = session.query(SchemaVersion).one()
    return schema_version.version


def check_empty(session: BaseSession) -> bool:
    engine = session.get_bind()
    for table_name in Base.metadata.tables.keys():
        if engine.has_table(table_name):
            return False
    return True


def check_schema_version(session: BaseSession) -> bool:
    if not session.get_bind().has_table(SchemaVersion.__tablename__):
        return False

    try:
        schema_version = get_schema_version(session)
    except NoResultFound:
        return False
    except MultipleResultsFound:
        return False
    except OperationalError:
        # table is present but schema doesn't match query
        return False
    else:
        return schema_version == SCHEMA_VERSION


def get_tracking_database(db_path: Path) -> BaseSession:
    session = get_session(db_path)

    if check_schema_version(session):
        # TODO: maybe use Alembic for this: https://alembic.sqlalchemy.org/en/latest/index.html
        return session
    elif not check_empty(session):
        raise BadDatabaseError(f"Invalid tracking database: {db_path.resolve()}")

    setup_schema(session)
    return session
