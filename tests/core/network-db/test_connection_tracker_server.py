import asyncio
import pytest

from async_service import background_asyncio_service
from lahja import BroadcastConfig

from p2p.tools.factories import NodeFactory

from trinity.constants import (
    NETWORKING_EVENTBUS_ENDPOINT,
)
from trinity.components.builtin.network_db.connection.events import ShouldConnectToPeerRequest
from trinity.components.builtin.network_db.connection.server import ConnectionTrackerServer
from trinity.components.builtin.network_db.connection.tracker import (
    ConnectionTrackerClient,
    MemoryConnectionTracker,
)


@pytest.mark.asyncio
async def test_connection_tracker_server_and_client(event_loop, event_bus):
    tracker = MemoryConnectionTracker()
    remote_a = NodeFactory()
    tracker.record_blacklist(remote_a, 60, "testing")

    assert await tracker.should_connect_to(remote_a) is False

    service = ConnectionTrackerServer(event_bus, tracker)

    # start the server
    async with background_asyncio_service(service):
        config = BroadcastConfig(filter_endpoint=NETWORKING_EVENTBUS_ENDPOINT)
        bus_tracker = ConnectionTrackerClient(event_bus, config=config)

        # Give `bus_tracker` a moment to setup subscriptions
        await event_bus.wait_until_any_endpoint_subscribed_to(ShouldConnectToPeerRequest)
        # ensure we can read from the tracker over the event bus
        assert await bus_tracker.should_connect_to(remote_a) is False

        # ensure we can write to the tracker over the event bus
        remote_b = NodeFactory()

        assert await bus_tracker.should_connect_to(remote_b) is True

        bus_tracker.record_blacklist(remote_b, 60, "testing")
        # let the underlying broadcast_nowait execute
        await asyncio.sleep(0.01)

        assert await bus_tracker.should_connect_to(remote_b) is False
        assert await tracker.should_connect_to(remote_b) is False

        bus_blacklisted_ids = await bus_tracker.get_blacklisted()
        blacklisted_ids = await tracker.get_blacklisted()
        assert bus_blacklisted_ids == blacklisted_ids

        assert sorted(blacklisted_ids) == sorted([remote_a.id, remote_b.id])
