import asyncio

from async_generator import asynccontextmanager
import pytest

from trinity.tools.bcc_factories import (
    AsyncBeaconChainDBFactory,
    BeaconChainSyncerFactory,
    ConnectionPairFactory,
    SignedBeaconBlockFactory,
)


@asynccontextmanager
async def get_sync_setup(
    request,
    event_loop,
    event_bus,
    genesis_state,
    alice_branch,
    bob_branch,
    sync_timeout=6,
):
    alice_chaindb = AsyncBeaconChainDBFactory()
    bob_chaindb = AsyncBeaconChainDBFactory()
    peer_pair = ConnectionPairFactory(
        alice_chaindb=alice_chaindb,
        alice_branch=alice_branch,
        bob_chaindb=bob_chaindb,
        bob_branch=bob_branch,
        genesis_state=genesis_state,
        alice_event_bus=event_bus,
        handshake=False,
    )

    async with peer_pair as (alice, bob):
        alice_syncer = BeaconChainSyncerFactory(
            chain_db__db=alice.chain.chaindb.db,
            peer_pool=alice.handshaked_peers,
            event_bus=event_bus,
        )

        try:
            task = asyncio.ensure_future(alice_syncer.run())
            # sync will start when alice request_status
            await alice.request_status(bob.peer_id)
            # Wait sync to complete
            await asyncio.wait_for(task, timeout=sync_timeout)
        except asyncio.TimeoutError:
            # After sync is cancelled, return to let the caller do assertions about the state
            pass
        finally:
            yield alice, bob


def assert_synced(alice, bob, correct_branch):
    alice_head = alice.chain.get_canonical_head()
    bob_head = bob.chain.get_canonical_head()
    assert alice_head == correct_branch[-1]
    assert alice_head == bob_head
    for correct_block in correct_branch:
        slot = correct_block.slot
        alice_block = alice.chain.get_canonical_block_by_slot(slot)
        bob_block = bob.chain.get_canonical_block_by_slot(slot)
        assert alice_block == bob_block
        assert alice_block == correct_block


@pytest.mark.asyncio
async def test_sync_from_genesis(request, event_loop, event_bus, genesis_state):
    genesis_block = SignedBeaconBlockFactory(
        message__state_root=genesis_state.hash_tree_root
    )
    alice_branch = (genesis_block,) + SignedBeaconBlockFactory.create_branch(
        length=0, root=genesis_block
    )
    bob_branch = (genesis_block,) + SignedBeaconBlockFactory.create_branch(
        length=99, root=genesis_block
    )

    async with get_sync_setup(
        request, event_loop, event_bus, genesis_state, alice_branch, bob_branch
    ) as (alice, bob):
        assert_synced(alice, bob, bob_branch)


@pytest.mark.asyncio
async def test_sync_from_old_head(request, event_loop, event_bus, genesis_state):
    genesis_block = SignedBeaconBlockFactory(
        message__state_root=genesis_state.hash_tree_root
    )
    alice_branch = (genesis_block,) + SignedBeaconBlockFactory.create_branch(
        length=49, root=genesis_block
    )
    bob_branch = alice_branch + SignedBeaconBlockFactory.create_branch(
        length=50, root=alice_branch[-1]
    )
    async with get_sync_setup(
        request, event_loop, event_bus, genesis_state, alice_branch, bob_branch
    ) as (alice, bob):
        assert_synced(alice, bob, bob_branch)


@pytest.mark.asyncio
async def test_reorg_sync(request, event_loop, event_bus, genesis_state):
    genesis_block = SignedBeaconBlockFactory(
        message__state_root=genesis_state.hash_tree_root
    )
    alice_branch = (genesis_block,) + SignedBeaconBlockFactory.create_branch(
        length=49, root=genesis_block, message__state_root=b"\x11" * 32
    )
    bob_branch = (genesis_block,) + SignedBeaconBlockFactory.create_branch(
        length=99, root=genesis_block, message__state_root=b"\x22" * 32
    )

    async with get_sync_setup(
        request, event_loop, event_bus, genesis_state, alice_branch, bob_branch
    ) as (alice, bob):
        assert_synced(alice, bob, bob_branch)


@pytest.mark.asyncio
async def test_sync_when_already_at_best_head(
    request, event_loop, event_bus, genesis_state
):
    genesis_block = SignedBeaconBlockFactory(
        message__state_root=genesis_state.hash_tree_root
    )
    alice_branch = (genesis_block,) + SignedBeaconBlockFactory.create_branch(
        length=99, root=genesis_block, message__state_root=b"\x11" * 32
    )
    bob_branch = (genesis_block,) + SignedBeaconBlockFactory.create_branch(
        length=50, root=genesis_block, message__state_root=b"\x22" * 32
    )

    async with get_sync_setup(
        request, event_loop, event_bus, genesis_state, alice_branch, bob_branch
    ) as (alice, bob):
        alice_head = alice.chain.get_canonical_head()
        assert alice_head.slot == 99
        for correct_block in alice_branch:
            slot = correct_block.slot
            alice_block = alice.chain.get_canonical_block_by_slot(slot)
            assert alice_block == correct_block


@pytest.mark.asyncio
async def test_sync_skipped_slots(request, event_loop, event_bus, genesis_state):
    genesis_block = SignedBeaconBlockFactory(
        message__state_root=genesis_state.hash_tree_root
    )
    alice_branch = (genesis_block,) + SignedBeaconBlockFactory.create_branch(
        length=0, root=genesis_block
    )
    bob_branch = (genesis_block,) + SignedBeaconBlockFactory.create_branch_by_slots(
        slots=tuple(range(4, 99)), root=genesis_block
    )
    assert bob_branch[0].slot == 0
    assert bob_branch[1].slot == 4
    async with get_sync_setup(
        request, event_loop, event_bus, genesis_state, alice_branch, bob_branch
    ) as (alice, bob):
        assert_synced(alice, bob, bob_branch)
