import pytest

import asyncio

from async_generator import asynccontextmanager
from eth2.beacon.types.blocks import BeaconBlock

from trinity.constants import TO_NETWORKING_BROADCAST_CONFIG
from trinity.protocol.bcc.peer import BCCPeerPoolEventServer
from trinity.protocol.bcc.servers import BCCRequestServer

from trinity.sync.beacon.chain import BeaconChainSyncer

from tests.core.integration_test_helpers import (
    run_peer_pool_event_server,
)
from .helpers import (
    SERENITY_GENESIS_CONFIG,
    get_directly_linked_peers_in_peer_pools,
    get_chain_db,
    create_test_block,
    create_branch,
)


class NoopBlockImporter:
    """
    Do nothing, to override the block validation in ``SyncBlockImporter``.
    """

    def import_block(self, block):
        return None, tuple(), tuple()


@asynccontextmanager
async def get_sync_setup(
        request,
        event_loop,
        event_bus,
        alice_chain_db,
        bob_chain_db,
        genesis_config=SERENITY_GENESIS_CONFIG):
    alice, alice_peer_pool, bob, bob_peer_pool = await get_directly_linked_peers_in_peer_pools(
        request,
        event_loop,
        alice_chain_db=alice_chain_db,
        bob_chain_db=bob_chain_db,
        bob_peer_pool_event_bus=event_bus,
    )

    bob_request_server = BCCRequestServer(
        event_bus, TO_NETWORKING_BROADCAST_CONFIG, bob.context.chain_db)

    alice_syncer = BeaconChainSyncer(
        alice_chain_db,
        alice_peer_pool,
        NoopBlockImporter(),
        genesis_config,
    )
    async with run_peer_pool_event_server(
        event_bus, bob_peer_pool, handler_type=BCCPeerPoolEventServer
    ):

        asyncio.ensure_future(bob_request_server.run())
        asyncio.ensure_future(alice_syncer.run())

        def finalizer():
            event_loop.run_until_complete(alice_syncer.cancel())
            event_loop.run_until_complete(bob_request_server.cancel())

        request.addfinalizer(finalizer)
        await alice_syncer.events.finished.wait()
        yield alice_syncer


@pytest.mark.asyncio
async def test_sync_from_genesis(request, event_loop, event_bus):
    genesis = create_test_block()
    bob_blocks = (genesis,) + create_branch(length=99, root=genesis)
    alice_chain_db = await get_chain_db((genesis,))
    bob_chain_db = await get_chain_db(bob_blocks)

    async with get_sync_setup(
        request, event_loop, event_bus, alice_chain_db, bob_chain_db
    ):

        alice_head = await alice_chain_db.coro_get_canonical_head(BeaconBlock)
        bob_head = await bob_chain_db.coro_get_canonical_head(BeaconBlock)
        assert alice_head.slot == genesis.slot + 99
        assert alice_head == bob_head
        for slot in range(genesis.slot, genesis.slot + 100):
            alice_block = await alice_chain_db.coro_get_canonical_block_by_slot(slot, BeaconBlock)
            bob_block = await bob_chain_db.coro_get_canonical_block_by_slot(slot, BeaconBlock)
            assert alice_block == bob_block


@pytest.mark.asyncio
async def test_sync_from_old_head(request, event_loop, event_bus):
    genesis = create_test_block()
    alice_blocks = (genesis,) + create_branch(length=49, root=genesis)
    bob_blocks = alice_blocks + create_branch(length=50, root=alice_blocks[-1])
    alice_chain_db = await get_chain_db(alice_blocks)
    bob_chain_db = await get_chain_db(bob_blocks)

    async with get_sync_setup(
        request, event_loop, event_bus, alice_chain_db, bob_chain_db
    ):

        alice_head = await alice_chain_db.coro_get_canonical_head(BeaconBlock)
        bob_head = await bob_chain_db.coro_get_canonical_head(BeaconBlock)

        assert alice_head.slot == genesis.slot + 99
        assert alice_head == bob_head
        for slot in range(genesis.slot, genesis.slot + 100):
            alice_block = await alice_chain_db.coro_get_canonical_block_by_slot(slot, BeaconBlock)
            bob_block = await bob_chain_db.coro_get_canonical_block_by_slot(slot, BeaconBlock)
            assert alice_block == bob_block


@pytest.mark.asyncio
async def test_reorg_sync(request, event_loop, event_bus):
    genesis = create_test_block()
    alice_blocks = (genesis,) + create_branch(length=49, root=genesis, state_root=b"\x11" * 32)
    bob_blocks = (genesis,) + create_branch(length=99, root=genesis, state_root=b"\x22" * 32)
    alice_chain_db = await get_chain_db(alice_blocks)
    bob_chain_db = await get_chain_db(bob_blocks)

    async with get_sync_setup(
        request, event_loop, event_bus, alice_chain_db, bob_chain_db
    ):

        alice_head = await alice_chain_db.coro_get_canonical_head(BeaconBlock)
        bob_head = await bob_chain_db.coro_get_canonical_head(BeaconBlock)

        assert alice_head.slot == genesis.slot + 99
        assert alice_head == bob_head
        for slot in range(genesis.slot, genesis.slot + 100):
            alice_block = await alice_chain_db.coro_get_canonical_block_by_slot(slot, BeaconBlock)
            bob_block = await bob_chain_db.coro_get_canonical_block_by_slot(slot, BeaconBlock)
            assert alice_block == bob_block


@pytest.mark.asyncio
async def test_sync_when_already_at_best_head(request, event_loop, event_bus):
    genesis = create_test_block()
    alice_blocks = (genesis,) + create_branch(length=99, root=genesis, state_root=b"\x11" * 32)
    bob_blocks = (genesis,) + create_branch(length=50, root=genesis, state_root=b"\x22" * 32)
    alice_chain_db = await get_chain_db(alice_blocks)
    bob_chain_db = await get_chain_db(bob_blocks)

    async with get_sync_setup(
        request, event_loop, event_bus, alice_chain_db, bob_chain_db
    ):

        alice_head = await alice_chain_db.coro_get_canonical_head(BeaconBlock)

        assert alice_head.slot == genesis.slot + 99
        assert alice_head == alice_blocks[-1]
        for slot in range(genesis.slot, genesis.slot + 100):
            alice_block = await alice_chain_db.coro_get_canonical_block_by_slot(slot, BeaconBlock)
            assert alice_block == alice_blocks[slot - genesis.slot]
