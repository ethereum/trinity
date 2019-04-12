import datetime
from pathlib import Path
import pytest

from p2p.exceptions import (
    HandshakeFailure,
    WrongGenesisFailure,
)
from p2p.tracking.db import (
    get_tracking_database,
)
from p2p.tracking.connection import (
    SQLiteTracker,
    MemoryTracker,
    get_timeout_for_failure,
)
from p2p.tools.factories import (
    NodeFactory,
)


def test_get_timeout_for_failure():
    assert get_timeout_for_failure(WrongGenesisFailure()) == 60 * 60 * 24
    assert get_timeout_for_failure(HandshakeFailure()) == 10

    class UnknownException(Exception):
        pass

    with pytest.raises(Exception, match="Unknown failure type"):
        assert get_timeout_for_failure(UnknownException()) is None


def test_records_failures():
    # where can you get a random pubkey from?
    connection_tracker = MemoryTracker()

    node = NodeFactory()
    assert connection_tracker.should_connect_to(node) is True

    connection_tracker.record_failure(node, HandshakeFailure())

    assert connection_tracker.should_connect_to(node) is False
    assert connection_tracker._record_exists(node.uri())


def test_memory_does_not_persist():
    node = NodeFactory()

    connection_tracker_a = MemoryTracker()
    assert connection_tracker_a.should_connect_to(node) is True
    connection_tracker_a.record_failure(node, HandshakeFailure())
    assert connection_tracker_a.should_connect_to(node) is False

    # open a second instance
    connection_tracker_b = MemoryTracker()

    # the second instance has no memory of the failure
    assert connection_tracker_b.should_connect_to(node) is True
    assert connection_tracker_a.should_connect_to(node) is False


def test_sql_does_persist(tmpdir):
    db_path = Path(str(tmpdir.join("nodedb")))
    node = NodeFactory()

    connection_tracker_a = SQLiteTracker(get_tracking_database(db_path))
    assert connection_tracker_a.should_connect_to(node) is True
    connection_tracker_a.record_failure(node, HandshakeFailure())
    assert connection_tracker_a.should_connect_to(node) is False
    del connection_tracker_a

    # open a second instance
    connection_tracker_b = SQLiteTracker(get_tracking_database(db_path))
    # the second instance remembers the failure
    assert connection_tracker_b.should_connect_to(node) is False


def test_timeout_works(monkeypatch):
    node = NodeFactory()

    connection_tracker = MemoryTracker()
    assert connection_tracker.should_connect_to(node) is True

    connection_tracker.record_failure(node, HandshakeFailure())
    assert connection_tracker.should_connect_to(node) is False

    record = connection_tracker._get_record(node.uri())
    record.expires_at -= datetime.timedelta(seconds=120)
    connection_tracker.session.add(record)
    connection_tracker.session.commit()

    assert connection_tracker.should_connect_to(node) is True
