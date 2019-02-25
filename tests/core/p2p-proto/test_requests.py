import asyncio

import pytest

from trinity.constants import TO_NETWORKING_BROADCAST_CONFIG
from trinity.protocol.common.peer_pool_event_bus import (
    BasePeerPoolMessageRelayer
)
from trinity.protocol.eth.peer import (
    ETHProxyPeer,
    ETHPeerPoolEventBusRequestHandler,
)
from trinity.protocol.eth.servers import (
    ETHIsolatedRequestServer,
)
from tests.core.integration_test_helpers import (
    FakeAsyncChainDB,
    FakeAsyncHeaderDB,
)
from tests.core.peer_helpers import (
    get_directly_linked_peers,
    MockPeerPoolWithConnectedPeers,
)


async def make_peer_pool_answer_event_bus_requests(event_bus, peer_pool):
    peer_pool_event_bus_request_handler = ETHPeerPoolEventBusRequestHandler(
        event_bus,
        peer_pool,
        peer_pool.cancel_token
    )
    asyncio.ensure_future(peer_pool_event_bus_request_handler.run())
    await peer_pool_event_bus_request_handler.events.started.wait()


async def make_peer_pool_relay_messages_on_event_bus(event_bus, peer_pool):
    peer_pool_message_relayer = BasePeerPoolMessageRelayer(peer_pool, event_bus)
    asyncio.ensure_future(peer_pool_message_relayer.run())
    await peer_pool_message_relayer.events.started.wait()


async def run_isolated_request_server(event_bus, chaindb):
    request_server = ETHIsolatedRequestServer(
        event_bus,
        TO_NETWORKING_BROADCAST_CONFIG,
        FakeAsyncChainDB(chaindb)
    )
    asyncio.ensure_future(request_server.run())
    await request_server.events.started.wait()


@pytest.mark.asyncio
async def test_proxy_peer_requests(request,
                                   event_bus,
                                   other_event_bus,
                                   event_loop,
                                   chaindb_fresh,
                                   chaindb_20):

    client_peer, server_peer = await get_directly_linked_peers(
        request,
        event_loop,
        alice_headerdb=FakeAsyncHeaderDB(chaindb_fresh.db),
        bob_headerdb=FakeAsyncHeaderDB(chaindb_20.db),
    )

    client_peer_pool = MockPeerPoolWithConnectedPeers([client_peer], event_bus=other_event_bus)
    await make_peer_pool_answer_event_bus_requests(other_event_bus, client_peer_pool)
    await make_peer_pool_relay_messages_on_event_bus(other_event_bus, client_peer_pool)

    server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer], event_bus=event_bus)
    await make_peer_pool_answer_event_bus_requests(event_bus, server_peer_pool)
    await make_peer_pool_relay_messages_on_event_bus(event_bus, server_peer_pool)
    await run_isolated_request_server(event_bus, chaindb_20.db)

    proxy_peer = ETHProxyPeer.from_dto_peer(
        client_peer.to_dto(),
        other_event_bus,
        TO_NETWORKING_BROADCAST_CONFIG,
    )

    headers = await proxy_peer.requests.get_block_headers(0, 1, 0, False)
    assert len(headers) == 1
    block_header = headers[0]
    assert block_header.block_number == 0

    receipts = await proxy_peer.requests.get_receipts(headers)
    assert len(receipts) == 1
    receipt = receipts[0]
    assert receipt[1][0] == block_header.receipt_root

    block_bundles = await proxy_peer.requests.get_block_bodies(headers)
    assert len(block_bundles) == 1
    first_bundle = block_bundles[0]
    assert first_bundle[1][0] == block_header.transaction_root

    node_data = await proxy_peer.requests.get_node_data((block_header.state_root,))
    assert node_data[0][0] == block_header.state_root


@pytest.mark.asyncio
async def test_proxy_peer_requests_with_timeouts(request,
                                                 event_bus,
                                                 other_event_bus,
                                                 event_loop,
                                                 chaindb_fresh,
                                                 chaindb_20):

    client_peer, server_peer = await get_directly_linked_peers(
        request,
        event_loop,
        alice_headerdb=FakeAsyncHeaderDB(chaindb_fresh.db),
        bob_headerdb=FakeAsyncHeaderDB(chaindb_20.db),
    )

    client_peer_pool = MockPeerPoolWithConnectedPeers([client_peer], event_bus=other_event_bus)
    await make_peer_pool_answer_event_bus_requests(other_event_bus, client_peer_pool)
    await make_peer_pool_relay_messages_on_event_bus(other_event_bus, client_peer_pool)

    # There is no server in this tests so that requests naturally time out

    proxy_peer = ETHProxyPeer.from_dto_peer(
        client_peer.to_dto(),
        other_event_bus,
        TO_NETWORKING_BROADCAST_CONFIG,
    )

    with pytest.raises(TimeoutError):
        await proxy_peer.requests.get_block_headers(0, 1, 0, False, timeout=0.01)

    with pytest.raises(TimeoutError):
        await proxy_peer.requests.get_receipts((), timeout=0.01)

    with pytest.raises(TimeoutError):
        await proxy_peer.requests.get_block_bodies((), timeout=0.01)

    with pytest.raises(TimeoutError):
        await proxy_peer.requests.get_node_data((), timeout=0.01)
