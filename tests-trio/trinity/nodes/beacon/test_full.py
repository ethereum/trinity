import random

from eth_utils import to_tuple
import pytest
import trio

from eth2.beacon.committee_helpers import get_beacon_proposer_index
from eth2.beacon.types.blocks import BeaconBlock, SignedBeaconBlock


@pytest.mark.trio
async def test_beacon_node_can_count_slots(autojump_clock, eth2_config, beacon_node):
    some_slots = 10
    a_future_slot = beacon_node.current_tick.slot + some_slots
    seconds = some_slots * eth2_config.SECONDS_PER_SLOT
    with trio.move_on_after(seconds):
        await beacon_node.run()
    assert beacon_node.current_tick.slot == a_future_slot


@to_tuple
def _mk_minimum_viable_signed_beacon_blocks(
    slots, state_machine_provider, state, block, config
):
    for slot in slots:
        future_state = state_machine_provider(
            slot
        ).state_transition.apply_state_transition(state, future_slot=block.slot + 1)
        proposer_index = get_beacon_proposer_index(future_state, config)
        message = BeaconBlock.create(
            slot=slot,
            parent_root=block.message.hash_tree_root,
            proposer_index=proposer_index,
        )
        block = SignedBeaconBlock.create(message=message)
        state, block = state_machine_provider(slot).import_block(block, state)
        yield block


def _build_branch_across_slots(number_of_slots, chain, config):
    head = chain.get_canonical_head()
    state = chain.get_state_by_root(head.message.state_root)

    return _mk_minimum_viable_signed_beacon_blocks(
        range(head.message.slot + 1, head.message.slot + 1 + number_of_slots),
        lambda slot: chain.get_state_machine(at_slot=slot),
        state,
        head,
        config
    )


@pytest.mark.trio
async def test_beacon_node_can_track_canonical_head_by_highest_slot(
    autojump_clock, beacon_node, genesis_validators, no_op_bls, eth2_config
):
    number_of_slots = 10
    blocks = _build_branch_across_slots(number_of_slots, beacon_node._chain, eth2_config)
    for block in blocks:
        beacon_node.on_block(block)

    head = beacon_node._chain.get_canonical_head()
    assert head.slot == blocks[-1].slot
    assert not beacon_node._block_pool
    assert not beacon_node._slashable_block_pool


@pytest.mark.trio
async def test_beacon_node_can_handle_orphans(
    autojump_clock, beacon_node, genesis_validators, no_op_bls, eth2_config
):
    number_of_slots = 30
    blocks = _build_branch_across_slots(number_of_slots, beacon_node._chain, eth2_config)
    shuffled_blocks = random.sample(blocks, len(blocks))
    had_orphans = False
    for block in shuffled_blocks:
        beacon_node.on_block(block)
        if beacon_node._block_pool:
            had_orphans = True

    # NOTE: will be True with _high_ probability...
    assert had_orphans

    head = beacon_node._chain.get_canonical_head()
    assert head.slot == blocks[-1].slot
    assert not beacon_node._block_pool
    assert not beacon_node._slashable_block_pool


@pytest.mark.trio
async def test_beacon_node_handles_duplicate_import(
    autojump_clock, beacon_node, genesis_validators, no_op_bls, eth2_config
):
    number_of_slots = 10
    blocks = _build_branch_across_slots(number_of_slots, beacon_node._chain, eth2_config)
    for block in blocks:
        beacon_node.on_block(block)

    assert not beacon_node._slashable_block_pool

    duplicate_block = blocks[number_of_slots // 2]
    beacon_node.on_block(duplicate_block)

    assert not beacon_node._slashable_block_pool

    head = beacon_node._chain.get_canonical_head()
    assert head.slot == blocks[-1].slot
    assert not beacon_node._block_pool
    assert not beacon_node._slashable_block_pool


@pytest.mark.trio
async def test_beacon_node_yields_slashable_block(
    autojump_clock, beacon_node, genesis_validators, no_op_bls, eth2_config
):
    number_of_slots = 10
    blocks = _build_branch_across_slots(number_of_slots, beacon_node._chain, eth2_config)
    for block in blocks:
        beacon_node.on_block(block)

    assert not beacon_node._slashable_block_pool

    slashable_block = blocks[number_of_slots // 2].transform(
        ("message", "state_root"), 32 * b"\xaa"
    )
    beacon_node.on_block(slashable_block)

    assert beacon_node._slashable_block_pool.pop() == slashable_block

    head = beacon_node._chain.get_canonical_head()
    assert head.slot == blocks[-1].slot
    assert not beacon_node._block_pool


@pytest.mark.trio
async def test_beacon_node_drops_invalid_blocks(
    autojump_clock, beacon_node, genesis_validators, no_op_bls, eth2_config
):
    number_of_slots = 10
    blocks = _build_branch_across_slots(number_of_slots, beacon_node._chain, eth2_config)
    for block in blocks:
        if block.slot == number_of_slots:
            # corrupt the last block
            block = block.transform(("message", "state_root"), 32 * b"\xaa")
        beacon_node.on_block(block)

    head = beacon_node._chain.get_canonical_head()
    assert head.slot == blocks[-2].slot
    assert not beacon_node._block_pool
    assert not beacon_node._slashable_block_pool
