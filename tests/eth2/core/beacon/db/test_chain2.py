from eth2.beacon.constants import EMPTY_SIGNATURE, GENESIS_SLOT
from eth2.beacon.db.chain2 import BeaconChainDB
from eth2.beacon.types.blocks import BeaconBlock, SignedBeaconBlock
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Slot


def test_chain2_at_genesis(base_db, genesis_state, genesis_block, config):
    genesis_block = genesis_block.message
    chain_db = BeaconChainDB.from_genesis(
        base_db, genesis_state, SignedBeaconBlock, config
    )

    block_at_genesis = chain_db.get_block_by_slot(GENESIS_SLOT, BeaconBlock)
    assert block_at_genesis == genesis_block

    block_at_genesis = chain_db.get_block_by_root(
        genesis_block.hash_tree_root, BeaconBlock
    )
    assert block_at_genesis == genesis_block

    genesis_signature = chain_db.get_block_signature_by_root(
        genesis_block.hash_tree_root
    )
    assert genesis_signature == EMPTY_SIGNATURE

    state_at_genesis = chain_db.get_state_by_slot(GENESIS_SLOT, BeaconState)
    assert state_at_genesis == genesis_state

    state_at_genesis = chain_db.get_state_by_root(
        genesis_state.hash_tree_root, BeaconState
    )
    assert state_at_genesis == genesis_state

    finalized_head = chain_db.get_finalized_head(BeaconBlock)
    assert finalized_head == genesis_block

    some_future_slot = Slot(22)
    assert not chain_db.get_block_by_slot(some_future_slot, BeaconBlock)
    assert not chain_db.get_state_by_slot(some_future_slot, BeaconState)

    block_at_future_slot = SignedBeaconBlock.create(
        message=BeaconBlock.create(slot=some_future_slot)
    )
    chain_db.persist_block(block_at_future_slot)
    future_block = chain_db.get_block_by_root(
        block_at_future_slot.message.hash_tree_root, BeaconBlock
    )
    assert block_at_future_slot.message == future_block

    # NOTE: only finalized blocks are stored by slot in the DB
    # non-finalized but canonical blocks are determined by fork choice, separately from the DB
    assert not chain_db.get_block_by_slot(some_future_slot, BeaconBlock)

    # assume the fork choice did finalize this block...
    chain_db.mark_canonical_block(block_at_future_slot.message)
    assert chain_db.get_block_by_slot(some_future_slot, BeaconBlock) == future_block
