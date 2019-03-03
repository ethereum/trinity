import asyncio

import pytest

from trinity.protocol.eth.servers import (
    ETHRequestServer,
)
from trinity.protocol.les.peer import (
    LESPeer,
    LESPeerPoolEventBusRequestHandler,
)
from trinity.protocol.les.servers import (
    LightRequestServer,
    LightIsolatedRequestServer,
)
from trinity.sync.full.chain import (
    FastChainSyncer,
    RegularChainSyncer,
)
from trinity.sync.full.state import StateDownloader
from trinity.sync.light.chain import LightChainSyncer

from tests.core.integration_test_helpers import (
    ByzantiumTestChain,
    FakeAsyncChainDB,
    FakeAsyncHeaderDB,
    make_peer_pool_answer_event_bus_requests,
    make_peer_pool_relay_messages_on_event_bus,
    run_isolated_request_server,
)
from tests.core.peer_helpers import (
    get_directly_linked_peers,
    MockPeerPoolWithConnectedPeers,
)


# This causes the chain syncers to request/send small batches of things, which will cause us to
# exercise parts of the code that wouldn't otherwise be exercised if the whole sync was completed
# by requesting a single batch.
@pytest.fixture(autouse=True)
def small_header_batches(monkeypatch):
    from trinity.protocol.eth import constants
    monkeypatch.setattr(constants, 'MAX_HEADERS_FETCH', 10)
    monkeypatch.setattr(constants, 'MAX_BODIES_FETCH', 5)


@pytest.mark.asyncio
async def test_fast_syncer_with_isolated_server(request,
                                                event_bus,
                                                other_event_bus,
                                                event_loop,
                                                chaindb_fresh,
                                                chaindb_20):

    server_event_bus = event_bus
    client_event_bus = other_event_bus
    client_peer, server_peer = await get_directly_linked_peers(
        request, event_loop,
        alice_headerdb=FakeAsyncHeaderDB(chaindb_fresh.db),
        bob_headerdb=FakeAsyncHeaderDB(chaindb_20.db))
    client_peer_pool = MockPeerPoolWithConnectedPeers([client_peer], event_bus=client_event_bus)
    await make_peer_pool_answer_event_bus_requests(client_event_bus, client_peer_pool)
    await make_peer_pool_relay_messages_on_event_bus(client_event_bus, client_peer_pool)

    server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer], event_bus=server_event_bus)
    await make_peer_pool_answer_event_bus_requests(server_event_bus, server_peer_pool)
    await make_peer_pool_relay_messages_on_event_bus(server_event_bus, server_peer_pool)
    await run_isolated_request_server(server_event_bus, chaindb_20.db)

    server_peer.logger.info("%s is serving 20 blocks", server_peer)
    client_peer.logger.info("%s is syncing up 20", client_peer)

    client = FastChainSyncer(
        ByzantiumTestChain(chaindb_fresh.db),
        chaindb_fresh,
        client_event_bus,
    )
    # FastChainSyncer.run() will return as soon as it's caught up with the peer.
    await asyncio.wait_for(client.run(), timeout=2)

    head = chaindb_fresh.get_canonical_head()
    assert head == chaindb_20.get_canonical_head()

    # # Now download the state for the chain's head.
    state_downloader = StateDownloader(
        chaindb_fresh, chaindb_fresh.db, head.state_root, client_peer_pool)
    await asyncio.wait_for(state_downloader.run(), timeout=2)

    assert head.state_root in chaindb_fresh.db


# @pytest.mark.asyncio
# async def test_fast_syncer(request, event_loop, event_bus, chaindb_fresh, chaindb_20):
#     client_peer, server_peer = await get_directly_linked_peers(
#         request, event_loop,
#         alice_headerdb=FakeAsyncHeaderDB(chaindb_fresh.db),
#         bob_headerdb=FakeAsyncHeaderDB(chaindb_20.db))
#     client_peer_pool = MockPeerPoolWithConnectedPeers([client_peer], event_bus=event_bus)
#     client = FastChainSyncer(ByzantiumTestChain(chaindb_fresh.db), chaindb_fresh, event_bus)
#     await make_peer_pool_answer_event_bus_requests(event_bus, client_peer_pool)
#     await make_peer_pool_relay_messages_on_event_bus(event_bus, client_peer_pool)
#     server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer])
#     server_request_handler = ETHRequestServer(FakeAsyncChainDB(chaindb_20.db), server_peer_pool)
#     asyncio.ensure_future(server_request_handler.run())
#     server_peer.logger.info("%s is serving 20 blocks", server_peer)
#     client_peer.logger.info("%s is syncing up 20", client_peer)

#     # FastChainSyncer.run() will return as soon as it's caught up with the peer.
#     await asyncio.wait_for(client.run(), timeout=2)

#     head = chaindb_fresh.get_canonical_head()
#     assert head == chaindb_20.get_canonical_head()

#     # Now download the state for the chain's head.
#     state_downloader = StateDownloader(
#         chaindb_fresh, chaindb_fresh.db, head.state_root, client_peer_pool)
#     await asyncio.wait_for(state_downloader.run(), timeout=2)

#     assert head.state_root in chaindb_fresh.db


@pytest.mark.asyncio
async def test_skeleton_syncer(request, event_loop, chaindb_fresh, chaindb_1000):
    client_peer, server_peer = await get_directly_linked_peers(
        request, event_loop,
        alice_headerdb=FakeAsyncHeaderDB(chaindb_fresh.db),
        bob_headerdb=FakeAsyncHeaderDB(chaindb_1000.db))
    client_peer_pool = MockPeerPoolWithConnectedPeers([client_peer])
    client = FastChainSyncer(ByzantiumTestChain(chaindb_fresh.db), chaindb_fresh, client_peer_pool)
    server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer])

    server_request_handler = ETHRequestServer(FakeAsyncChainDB(chaindb_1000.db), server_peer_pool)
    asyncio.ensure_future(server_request_handler.run())
    client_peer.logger.info("%s is serving 1000 blocks", client_peer)
    server_peer.logger.info("%s is syncing up 1000 blocks", server_peer)

    await asyncio.wait_for(client.run(), timeout=20)

    head = chaindb_fresh.get_canonical_head()
    assert head == chaindb_1000.get_canonical_head()

    # Now download the state for the chain's head.
    state_downloader = StateDownloader(
        chaindb_fresh, chaindb_fresh.db, head.state_root, client_peer_pool)
    await asyncio.wait_for(state_downloader.run(), timeout=20)

    assert head.state_root in chaindb_fresh.db


@pytest.mark.asyncio
async def test_regular_syncer(request, event_loop, chaindb_fresh, chaindb_20):
    client_peer, server_peer = await get_directly_linked_peers(
        request, event_loop,
        alice_headerdb=FakeAsyncHeaderDB(chaindb_fresh.db),
        bob_headerdb=FakeAsyncHeaderDB(chaindb_20.db))
    client = RegularChainSyncer(
        ByzantiumTestChain(chaindb_fresh.db),
        chaindb_fresh,
        MockPeerPoolWithConnectedPeers([client_peer]))
    server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer])

    server_request_handler = ETHRequestServer(FakeAsyncChainDB(chaindb_20.db), server_peer_pool)
    asyncio.ensure_future(server_request_handler.run())
    server_peer.logger.info("%s is serving 20 blocks", server_peer)
    client_peer.logger.info("%s is syncing up 20", client_peer)

    def finalizer():
        event_loop.run_until_complete(client.cancel())
        # Yield control so that client/server.run() returns, otherwise asyncio will complain.
        event_loop.run_until_complete(asyncio.sleep(0.1))
    request.addfinalizer(finalizer)

    asyncio.ensure_future(client.run())

    await wait_for_head(chaindb_fresh, chaindb_20.get_canonical_head())
    head = chaindb_fresh.get_canonical_head()
    assert head.state_root in chaindb_fresh.db


@pytest.mark.asyncio
async def test_light_syncer_with_isolated_server(request,
                                                 event_loop,
                                                 event_bus,
                                                 chaindb_fresh,
                                                 chaindb_20):
    client_peer, server_peer = await get_directly_linked_peers(
        request, event_loop,
        alice_peer_class=LESPeer,
        alice_headerdb=FakeAsyncHeaderDB(chaindb_fresh.db),
        bob_headerdb=FakeAsyncHeaderDB(chaindb_20.db))
    client = LightChainSyncer(
        ByzantiumTestChain(chaindb_fresh.db),
        chaindb_fresh,
        MockPeerPoolWithConnectedPeers([client_peer]))
    server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer], event_bus=event_bus)

    await make_peer_pool_answer_event_bus_requests(
        event_bus,
        server_peer_pool,
        handler_type=LESPeerPoolEventBusRequestHandler
    )
    await make_peer_pool_relay_messages_on_event_bus(event_bus, server_peer_pool)
    await run_isolated_request_server(
        event_bus, chaindb_20.db, server_type=LightIsolatedRequestServer)

    server_peer.logger.info("%s is serving 20 blocks", server_peer)
    client_peer.logger.info("%s is syncing up 20", client_peer)

    def finalizer():
        event_loop.run_until_complete(client.cancel())
        # Yield control so that client/server.run() returns, otherwise asyncio will complain.
        event_loop.run_until_complete(asyncio.sleep(0.1))
    request.addfinalizer(finalizer)

    asyncio.ensure_future(client.run())

    await wait_for_head(chaindb_fresh, chaindb_20.get_canonical_head())


@pytest.mark.asyncio
async def test_light_syncer(request, event_loop, chaindb_fresh, chaindb_20):
    client_peer, server_peer = await get_directly_linked_peers(
        request, event_loop,
        alice_peer_class=LESPeer,
        alice_headerdb=FakeAsyncHeaderDB(chaindb_fresh.db),
        bob_headerdb=FakeAsyncHeaderDB(chaindb_20.db))
    client = LightChainSyncer(
        ByzantiumTestChain(chaindb_fresh.db),
        chaindb_fresh,
        MockPeerPoolWithConnectedPeers([client_peer]))
    server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer])

    server_request_handler = LightRequestServer(FakeAsyncHeaderDB(chaindb_20.db), server_peer_pool)
    asyncio.ensure_future(server_request_handler.run())
    server_peer.logger.info("%s is serving 20 blocks", server_peer)
    client_peer.logger.info("%s is syncing up 20", client_peer)

    def finalizer():
        event_loop.run_until_complete(client.cancel())
        # Yield control so that client/server.run() returns, otherwise asyncio will complain.
        event_loop.run_until_complete(asyncio.sleep(0.1))
    request.addfinalizer(finalizer)

    asyncio.ensure_future(client.run())

    await wait_for_head(chaindb_fresh, chaindb_20.get_canonical_head())


async def wait_for_head(headerdb, header):
    # A full header sync may involve several round trips, so we must be willing to wait a little
    # bit for them.
    HEADER_SYNC_TIMEOUT = 3

    async def wait_loop():
        while headerdb.get_canonical_head() != header:
            await asyncio.sleep(0.1)
    await asyncio.wait_for(wait_loop(), HEADER_SYNC_TIMEOUT)
