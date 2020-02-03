import asyncio

import pytest

from async_exit_stack import AsyncExitStack
from async_service import background_asyncio_service

from p2p.exceptions import PeerConnectionLost

from trinity.constants import TO_NETWORKING_BROADCAST_CONFIG
from trinity.db.eth1.chain import AsyncChainDB
from trinity.protocol.eth.peer import (
    ETHProxyPeerPool,
    ETHPeerPoolEventServer,
)
from trinity.protocol.eth.servers import ETHRequestServer

from trinity.tools.factories import (
    ChainContextFactory,
    ETHPeerPairFactory,
)

from tests.core.integration_test_helpers import (
    run_peer_pool_event_server,
)
from tests.core.peer_helpers import (
    MockPeerPoolWithConnectedPeers,
)


@pytest.fixture
async def client_and_server(chaindb_fresh, chaindb_20):
    peer_pair = ETHPeerPairFactory(
        alice_peer_context=ChainContextFactory(headerdb__db=chaindb_fresh.db),
        bob_peer_context=ChainContextFactory(headerdb__db=chaindb_20.db),
    )
    async with peer_pair as (client_peer, server_peer):
        yield client_peer, server_peer


@pytest.mark.asyncio
async def test_proxy_peer_requests(request,
                                   event_bus,
                                   other_event_bus,
                                   event_loop,
                                   chaindb_20,
                                   client_and_server):
    server_event_bus = event_bus
    client_event_bus = other_event_bus
    client_peer, server_peer = client_and_server

    client_peer_pool = MockPeerPoolWithConnectedPeers([client_peer], event_bus=client_event_bus)
    server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer], event_bus=server_event_bus)

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(run_peer_pool_event_server(
            client_event_bus, client_peer_pool, handler_type=ETHPeerPoolEventServer
        ))

        await stack.enter_async_context(run_peer_pool_event_server(
            server_event_bus, server_peer_pool, handler_type=ETHPeerPoolEventServer
        ))

        await stack.enter_async_context(background_asyncio_service(ETHRequestServer(
            server_event_bus,
            TO_NETWORKING_BROADCAST_CONFIG,
            AsyncChainDB(chaindb_20.db)
        )))

        client_proxy_peer_pool = ETHProxyPeerPool(client_event_bus, TO_NETWORKING_BROADCAST_CONFIG)
        await stack.enter_async_context(background_asyncio_service(client_proxy_peer_pool))

        proxy_peer_pool = ETHProxyPeerPool(server_event_bus, TO_NETWORKING_BROADCAST_CONFIG)
        await stack.enter_async_context(background_asyncio_service(proxy_peer_pool))

        proxy_peer = await client_proxy_peer_pool.ensure_proxy_peer(client_peer.session)

        headers = await proxy_peer.eth_api.get_block_headers(0, 1, 0, False)

        assert len(headers) == 1
        block_header = headers[0]
        assert block_header.block_number == 0

        receipts = await proxy_peer.eth_api.get_receipts(headers)
        assert len(receipts) == 1
        receipt = receipts[0]
        assert receipt[1][0] == block_header.receipt_root

        block_bundles = await proxy_peer.eth_api.get_block_bodies(headers)
        assert len(block_bundles) == 1
        first_bundle = block_bundles[0]
        assert first_bundle[1][0] == block_header.transaction_root

        node_data = await proxy_peer.eth_api.get_node_data((block_header.state_root,))
        assert node_data[0][0] == block_header.state_root


@pytest.mark.asyncio
async def test_proxy_peer_requests_with_timeouts(request,
                                                 event_bus,
                                                 other_event_bus,
                                                 event_loop,
                                                 client_and_server):

    server_event_bus = event_bus
    client_event_bus = other_event_bus
    client_peer, server_peer = client_and_server

    client_peer_pool = MockPeerPoolWithConnectedPeers([client_peer], event_bus=client_event_bus)
    server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer], event_bus=server_event_bus)

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(run_peer_pool_event_server(
            client_event_bus, client_peer_pool, handler_type=ETHPeerPoolEventServer
        ))
        await stack.enter_async_context(run_peer_pool_event_server(
            server_event_bus, server_peer_pool, handler_type=ETHPeerPoolEventServer
        ))

        client_proxy_peer_pool = ETHProxyPeerPool(client_event_bus, TO_NETWORKING_BROADCAST_CONFIG)
        await stack.enter_async_context(background_asyncio_service(client_proxy_peer_pool))

        server_proxy_peer_pool = ETHProxyPeerPool(server_event_bus, TO_NETWORKING_BROADCAST_CONFIG)
        await stack.enter_async_context(background_asyncio_service(server_proxy_peer_pool))

        proxy_peer = await client_proxy_peer_pool.ensure_proxy_peer(client_peer.session)

        with pytest.raises(asyncio.TimeoutError):
            await proxy_peer.eth_api.get_block_headers(0, 1, 0, False, timeout=0.01)

        with pytest.raises(asyncio.TimeoutError):
            await proxy_peer.eth_api.get_receipts((), timeout=0.01)

        with pytest.raises(asyncio.TimeoutError):
            await proxy_peer.eth_api.get_block_bodies((), timeout=0.01)

        with pytest.raises(asyncio.TimeoutError):
            await proxy_peer.eth_api.get_node_data((), timeout=0.01)


@pytest.mark.asyncio
async def test_requests_when_peer_in_client_vanishs(request,
                                                    event_bus,
                                                    other_event_bus,
                                                    event_loop,
                                                    chaindb_20,
                                                    client_and_server):

    server_event_bus = event_bus
    client_event_bus = other_event_bus
    client_peer, server_peer = client_and_server

    client_peer_pool = MockPeerPoolWithConnectedPeers([client_peer], event_bus=client_event_bus)
    server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer], event_bus=server_event_bus)

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(run_peer_pool_event_server(
            client_event_bus, client_peer_pool, handler_type=ETHPeerPoolEventServer
        ))
        await stack.enter_async_context(run_peer_pool_event_server(
            server_event_bus, server_peer_pool, handler_type=ETHPeerPoolEventServer
        ))

        await stack.enter_async_context(background_asyncio_service(ETHRequestServer(
            server_event_bus,
            TO_NETWORKING_BROADCAST_CONFIG,
            AsyncChainDB(chaindb_20.db)
        )))
        client_proxy_peer_pool = ETHProxyPeerPool(client_event_bus, TO_NETWORKING_BROADCAST_CONFIG)
        await stack.enter_async_context(background_asyncio_service(client_proxy_peer_pool))

        server_proxy_peer_pool = ETHProxyPeerPool(server_event_bus, TO_NETWORKING_BROADCAST_CONFIG)
        await stack.enter_async_context(background_asyncio_service(server_proxy_peer_pool))

        proxy_peer = await client_proxy_peer_pool.ensure_proxy_peer(client_peer.session)

        # We remove the peer from the client and assume to see PeerConnectionLost exceptions raised
        client_peer_pool.connected_nodes.pop(client_peer.session)

        with pytest.raises(PeerConnectionLost):
            await proxy_peer.eth_api.get_block_headers(0, 1, 0, False)

        with pytest.raises(PeerConnectionLost):
            await proxy_peer.eth_api.get_receipts(())

        with pytest.raises(PeerConnectionLost):
            await proxy_peer.eth_api.get_block_bodies(())

        with pytest.raises(PeerConnectionLost):
            await proxy_peer.eth_api.get_node_data(())
