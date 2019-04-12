from pathlib import Path

import pytest

from p2p.exceptions import BadDatabaseError
from p2p.tracking.db import (
    SchemaVersion,
    setup_schema,
    check_empty,
    check_schema_version,
    get_schema_version,
    get_session,
    get_tracking_database,
    SCHEMA_VERSION,
)


@pytest.fixture
def session():
    path = Path(':memory:')
    return get_session(path)


@pytest.fixture
def db_path(tmpdir):
    path = Path(str(tmpdir.join('nodedb.sqlite')))
    return path


#
# Schema initialization tests
#
def test_get_schema_version(session):
    setup_schema(session)
    version = get_schema_version(session)
    assert version == SCHEMA_VERSION


def test_setup_schema(session):
    assert check_schema_version(session) is False
    setup_schema(session)
    assert check_schema_version(session) is True


def test_check_schema_version_false_when_no_tables(session):
    assert check_empty(session)
    assert check_schema_version(session) is False


def test_check_schema_version_false_when_no_entry(session):
    setup_schema(session)
    assert check_schema_version(session) is True

    # delete the entry
    schema_version = session.query(SchemaVersion).one()
    session.delete(schema_version)
    session.commit()

    assert check_schema_version(session) is False


def test_check_schema_version_false_when_wrong_version(session):
    setup_schema(session)

    assert check_schema_version(session) is True

    # change version to unknown value
    schema_version = session.query(SchemaVersion).one()
    schema_version.version = 'unknown'

    session.add(schema_version)
    session.commit()

    assert check_schema_version(session) is False


def test_node_db_check_schema_version_false_when_multiple_entries(session):
    setup_schema(session)

    assert check_schema_version(session) is True

    session.add(SchemaVersion(version='unknown'))
    session.commit()

    assert check_schema_version(session) is False


def test_get_tracking_db_from_empty():
    session = get_tracking_database(Path(':memory:'))
    assert check_schema_version(session) is True


def test_get_tracking_db_from_valid_existing(db_path):
    session_a = get_tracking_database(db_path)
    assert check_schema_version(session_a) is True
    del session_a

    # ensure the session was persisted to disk
    session_b = get_session(db_path)
    assert check_schema_version(session_b) is True
    del session_b

    session_c = get_tracking_database(db_path)
    assert check_schema_version(session_c) is True


def test_get_tracking_db_errors_bad_schema_version(db_path):
    session_a = get_tracking_database(db_path)
    assert check_schema_version(session_a) is True

    # change version to unknown value
    schema_version = session_a.query(SchemaVersion).one()
    schema_version.version = 'unknown'

    session_a.add(schema_version)
    session_a.commit()
    del session_a

    # ensure the session was persisted to disk
    session_b = get_session(db_path)
    assert check_schema_version(session_b) is False
    del session_b

    with pytest.raises(BadDatabaseError):
        get_tracking_database(db_path)
