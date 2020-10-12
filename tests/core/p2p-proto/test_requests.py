import asyncio
import contextlib

import pytest

from async_service import background_asyncio_service
from eth_utils import decode_hex

from p2p.exceptions import PeerConnectionLost
from trinity.components.builtin.tx_pool.pool import TxPool

from trinity.constants import TO_NETWORKING_BROADCAST_CONFIG
from trinity.db.eth1.chain import AsyncChainDB
from trinity.protocol.eth.peer import (
    ETHProxyPeerPool,
    ETHPeerPoolEventServer,
)
from trinity.protocol.wit.db import AsyncWitnessDB
from trinity.protocol.wit.servers import WitRequestServer
from trinity.protocol.eth.proto import ETHProtocolV65
from trinity.protocol.eth.servers import ETHRequestServer

from trinity.tools.factories import (
    ChainContextFactory,
    Hash32Factory,
    LatestETHPeerPairFactory,
    ALL_PEER_PAIR_FACTORIES,
)

from tests.core.integration_test_helpers import (
    run_peer_pool_event_server,
)
from tests.core.peer_helpers import (
    MockPeerPoolWithConnectedPeers,
)


def get_highest_eth_protocol_version(client):
    return max(
        protocol.as_capability()[1] for protocol in client.connection.get_protocols()
        if protocol.as_capability()[0] == 'eth'
    )


@pytest.fixture(
    params=ALL_PEER_PAIR_FACTORIES
)
async def client_and_server(chaindb_fresh, chaindb_20, request):
    peer_pair = request.param(
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

    async with contextlib.AsyncExitStack() as stack:
        await stack.enter_async_context(run_peer_pool_event_server(
            client_event_bus, client_peer_pool, handler_type=ETHPeerPoolEventServer
        ))

        await stack.enter_async_context(run_peer_pool_event_server(
            server_event_bus, server_peer_pool, handler_type=ETHPeerPoolEventServer
        ))

        base_db = chaindb_20.db
        await stack.enter_async_context(background_asyncio_service(ETHRequestServer(
            server_event_bus,
            TO_NETWORKING_BROADCAST_CONFIG,
            AsyncChainDB(base_db)
        )))
        await stack.enter_async_context(background_asyncio_service(WitRequestServer(
            server_event_bus,
            TO_NETWORKING_BROADCAST_CONFIG,
            base_db,
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

        block_hash = block_header.hash
        node_hashes = tuple(Hash32Factory.create_batch(5))
        # Populate the server's witness DB so that it can reply to our request.
        wit_db = AsyncWitnessDB(base_db)
        wit_db.persist_witness_hashes(block_hash, node_hashes)
        response = await proxy_peer.wit_api.get_block_witness_hashes(block_hash)
        assert set(response) == set(node_hashes)


@pytest.mark.asyncio
async def test_get_pooled_transactions_request(request,
                                               event_bus,
                                               other_event_bus,
                                               event_loop,
                                               chaindb_20,
                                               client_and_server):
    server_event_bus = event_bus
    client_event_bus = other_event_bus
    client_peer, server_peer = client_and_server

    if get_highest_eth_protocol_version(client_peer) < ETHProtocolV65.version:
        pytest.skip("Test not applicable below eth/65")

    client_peer_pool = MockPeerPoolWithConnectedPeers([client_peer], event_bus=client_event_bus)
    server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer], event_bus=server_event_bus)

    async with contextlib.AsyncExitStack() as stack:
        await stack.enter_async_context(run_peer_pool_event_server(
            client_event_bus, client_peer_pool, handler_type=ETHPeerPoolEventServer
        ))

        await stack.enter_async_context(run_peer_pool_event_server(
            server_event_bus, server_peer_pool, handler_type=ETHPeerPoolEventServer
        ))

        client_proxy_peer_pool = ETHProxyPeerPool(client_event_bus, TO_NETWORKING_BROADCAST_CONFIG)
        await stack.enter_async_context(background_asyncio_service(client_proxy_peer_pool))

        proxy_peer_pool = ETHProxyPeerPool(server_event_bus, TO_NETWORKING_BROADCAST_CONFIG)
        await stack.enter_async_context(background_asyncio_service(proxy_peer_pool))

        proxy_peer = await client_proxy_peer_pool.ensure_proxy_peer(client_peer.session)

        # The reason we run this test separately from the other request tests is because
        # GetPooledTransactions requests should be answered from the tx pool which the previous
        # test does not depend on.
        await stack.enter_async_context(background_asyncio_service(TxPool(
            server_event_bus,
            proxy_peer_pool,
            lambda _: True
        )))

        # The tx pool always answers these with an empty response
        txs = await proxy_peer.eth_api.get_pooled_transactions(
            (decode_hex('0x9ea39df6210064648ecbc465cd628fe52f69af53792e1c2f27840133435159d4'),)
        )
        assert len(txs) == 0


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

    async with contextlib.AsyncExitStack() as stack:
        await stack.enter_async_context(run_peer_pool_event_server(
            client_event_bus, client_peer_pool, handler_type=ETHPeerPoolEventServer
        ))
        await stack.enter_async_context(run_peer_pool_event_server(
            server_event_bus, server_peer_pool, handler_type=ETHPeerPoolEventServer
        ))

        # We just want an ETHRequestServer that doesn't answer us but we still have to run
        # *something* to at least subscribe to the events. Otherwise Lahja's safety check will yell
        # at us for sending requests into the void.
        for event_type in ETHRequestServer(None, None, None)._subscribed_events:
            server_event_bus.subscribe(event_type, lambda _: None)

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

    async with contextlib.AsyncExitStack() as stack:
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


#
# === ETH-specific server tests
#

@pytest.mark.asyncio
async def test_no_duplicate_node_data(request, event_loop, event_bus, chaindb_fresh, chaindb_20):
    """
    Test that when a peer calls GetNodeData to ETHRequestServer, with duplicate node hashes,
    that ETHRequestServer only responds with unique nodes.

    Note: A nice extension to the test would be to check that a warning is
        raised about sending the duplicate hashes in the first place.
    """
    client_context = ChainContextFactory(headerdb__db=chaindb_fresh.db)
    server_context = ChainContextFactory(headerdb__db=chaindb_20.db)
    peer_pair = LatestETHPeerPairFactory(
        alice_peer_context=client_context,
        bob_peer_context=server_context,
        event_bus=event_bus,
    )

    async with peer_pair as (client_to_server, server_to_client):

        server_peer_pool = MockPeerPoolWithConnectedPeers([server_to_client], event_bus=event_bus)

        async with run_peer_pool_event_server(
            event_bus, server_peer_pool, handler_type=ETHPeerPoolEventServer
        ), background_asyncio_service(ETHRequestServer(
            event_bus, TO_NETWORKING_BROADCAST_CONFIG, AsyncChainDB(chaindb_20.db)
        )):
            root_hash = chaindb_20.get_canonical_head().state_root
            state_root = chaindb_20.db[root_hash]

            returned_nodes = await client_to_server.eth_api.get_node_data((root_hash, root_hash))
            assert returned_nodes == (
                # Server must not send back duplicates, just the single root node
                (root_hash, state_root),
            )
