import pytest

from async_service import background_asyncio_service

from trinity.protocol.common.events import PeerCountRequest
from trinity.protocol.common.peer_pool_event_bus import (
    DefaultPeerPoolEventServer,
)

from p2p.tools.paragon import (
    ParagonMockPeerPoolWithConnectedPeers,
)
from p2p.tools.factories import (
    ParagonPeerPairFactory,
)


@pytest.mark.asyncio
async def test_event_bus_requests_against_peer_pool(request, event_loop, event_bus):
    async with ParagonPeerPairFactory() as (alice, bob):
        peer_pool = ParagonMockPeerPoolWithConnectedPeers([alice, bob])
        event_server = DefaultPeerPoolEventServer(event_bus, peer_pool)
        async with background_asyncio_service(event_server):
            await event_bus.wait_until_any_endpoint_subscribed_to(PeerCountRequest)

            res = await event_bus.request(PeerCountRequest())

            assert res.peer_count == 2
