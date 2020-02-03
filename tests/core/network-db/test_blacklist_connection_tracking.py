from pathlib import Path

import pytest

from p2p.exceptions import (
    HandshakeFailure,
)
from p2p.tools.factories import (
    NodeFactory,
)

from trinity.components.builtin.network_db.connection.tracker import (
    SQLiteConnectionTracker,
    MemoryConnectionTracker,
)
from trinity.db.orm import (
    get_tracking_database,
)


@pytest.mark.asyncio
async def test_records_failures():
    connection_tracker = MemoryConnectionTracker()

    node = NodeFactory()
    blacklisted_ids = await connection_tracker.get_blacklisted()
    assert node.id not in blacklisted_ids

    connection_tracker.record_failure(node, HandshakeFailure())

    blacklisted_ids = await connection_tracker.get_blacklisted()
    assert node.id in blacklisted_ids
    assert connection_tracker._record_exists(node.id)


@pytest.mark.asyncio
async def test_memory_does_not_persist():
    node = NodeFactory()

    connection_tracker_a = MemoryConnectionTracker()
    blacklisted_ids = await connection_tracker_a.get_blacklisted()
    assert node.id not in blacklisted_ids
    connection_tracker_a.record_failure(node, HandshakeFailure())
    blacklisted_ids = await connection_tracker_a.get_blacklisted()
    assert node.id in blacklisted_ids

    # open a second instance
    connection_tracker_b = MemoryConnectionTracker()

    # the second instance has no memory of the failure
    tracker_b_blacklisted_ids = await connection_tracker_b.get_blacklisted()
    assert node.id not in tracker_b_blacklisted_ids

    tracker_a_blacklisted_ids = await connection_tracker_a.get_blacklisted()
    assert node.id in tracker_a_blacklisted_ids


@pytest.mark.asyncio
async def test_sql_does_persist(tmpdir):
    db_path = Path(tmpdir.join("nodedb"))
    node = NodeFactory()

    connection_tracker_a = SQLiteConnectionTracker(get_tracking_database(db_path))
    blacklisted_ids = await connection_tracker_a.get_blacklisted()
    assert node.id not in blacklisted_ids
    connection_tracker_a.record_failure(node, HandshakeFailure())
    blacklisted_ids = await connection_tracker_a.get_blacklisted()
    assert node.id in blacklisted_ids
    del connection_tracker_a

    # open a second instance
    connection_tracker_b = SQLiteConnectionTracker(get_tracking_database(db_path))
    blacklisted_ids = await connection_tracker_b.get_blacklisted()
    # the second instance remembers the failure
    assert node.id in blacklisted_ids


@pytest.mark.asyncio
async def test_get_blacklisted():
    node1, node2 = NodeFactory(), NodeFactory()
    connection_tracker = MemoryConnectionTracker()

    connection_tracker.record_blacklist(node1, timeout_seconds=10, reason='')
    connection_tracker.record_blacklist(node2, timeout_seconds=0, reason='')

    blacklisted_ids = await connection_tracker.get_blacklisted()

    assert blacklisted_ids == tuple([node1.id])
