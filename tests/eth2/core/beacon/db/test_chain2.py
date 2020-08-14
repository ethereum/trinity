from typing import Optional

from eth_typing import BLSPubkey, Hash32

from eth2.beacon.constants import EMPTY_SIGNATURE, GENESIS_SLOT
from eth2.beacon.db.chain2 import BeaconChainDB
from eth2.beacon.types.blocks import BeaconBlock, SignedBeaconBlock
from eth2.beacon.types.checkpoints import Checkpoint
from eth2.beacon.types.eth1_data import Eth1Data
from eth2.beacon.types.pending_attestations import PendingAttestation
from eth2.beacon.types.states import BeaconState
from eth2.beacon.types.validators import Validator
from eth2.beacon.typing import Epoch, Root, Slot, ValidatorIndex
from eth2.configs import Eth2Config


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

    state_at_genesis = chain_db.get_state_by_slot(GENESIS_SLOT, BeaconState, config)
    assert state_at_genesis == genesis_state

    state_at_genesis = chain_db.get_state_by_root(
        genesis_state.hash_tree_root, BeaconState, config
    )
    assert state_at_genesis == genesis_state

    finalized_head = chain_db.get_finalized_head(BeaconBlock)
    assert finalized_head == genesis_block


def test_chain2_full(base_db, genesis_state, config):
    chain_db = BeaconChainDB.from_genesis(
        base_db, genesis_state, SignedBeaconBlock, config
    )

    state = genesis_state
    states = [genesis_state]
    blocks = {}

    num_slots = 500
    skip_slots = [5, 64, 100, 300, 301, 302, 401]
    finalized_slots = [8, 24, 32, 72, 152, 160, 328, 336, 344, 352, 400]

    # create a new state at each slot using ``_mini_stf()`` and persist it
    for _ in range(1, num_slots):
        if state.slot not in skip_slots:
            new_block = BeaconBlock.create(
                slot=state.slot, state_root=state.hash_tree_root
            )
            blocks[state.slot] = new_block
            chain_db.persist_block(SignedBeaconBlock.create(message=new_block))
        else:
            new_block = None

        state = _mini_stf(state, new_block, config)
        chain_db.persist_state(state, config)
        states.append(state)

    # test that each state created above equals the state stored at its root
    for state in states:
        # finalize a slot two epochs after processing it
        # this is here to test the reconstruction of ``state.randao_mixes``
        maybe_finalized_slot = state.slot - config.SLOTS_PER_EPOCH * 2
        if maybe_finalized_slot in finalized_slots:
            chain_db.mark_finalized_head(blocks[maybe_finalized_slot])

        retrieved_state = chain_db._read_state(
            state.hash_tree_root, BeaconState, config
        )
        assert retrieved_state == state

    for slot in range(0, num_slots):
        if slot in blocks and slot <= finalized_slots[-1]:
            assert chain_db.get_block_by_slot(Slot(slot), BeaconBlock) == blocks[slot]
        else:
            assert chain_db.get_block_by_slot(Slot(slot), BeaconBlock) is None


def _mini_stf(
    state: BeaconState, block: Optional[BeaconBlock], config: Eth2Config
) -> BeaconState:
    """
    A simplified state transition for testing state storage.

    - updates ``state_roots`` with the previous slot's state root
    - updates ``block_roots`` with the previous slot's block root
    - updates ``randao_mixes`` with an arbitrary mix at the current epoch
    - creates a new ``latest_block_header`` and adds it to the state
    - fills in the rest of the attributes with arbitrary values
    """
    current_slot = state.slot + 1
    current_epoch = current_slot // config.SLOTS_PER_EPOCH

    if block:
        latest_block_header = block.header
    else:
        latest_block_header = state.latest_block_header

    # state changes that depend on the previous state for retrieval
    randao_mix = Root(Hash32(current_slot.to_bytes(32, byteorder="little")))
    state = (
        state.transform(
            ("state_roots", state.slot % config.SLOTS_PER_HISTORICAL_ROOT),
            state.hash_tree_root,
        )
        .transform(
            ("block_roots", state.slot % config.SLOTS_PER_HISTORICAL_ROOT),
            state.latest_block_header.hash_tree_root,
        )
        .transform(
            ("randao_mixes", current_epoch % config.EPOCHS_PER_HISTORICAL_VECTOR),
            randao_mix,
        )
        .mset("slot", current_slot, "latest_block_header", latest_block_header)
    )

    # state changes that do not depend on the previous state for retrieval
    new_validators = [
        Validator.create(pubkey=BLSPubkey(n.to_bytes(48, byteorder="little")))
        for n in range(current_slot, current_slot + 20)
    ]
    new_eth1_data_votes = [
        Eth1Data.create(deposit_root=Root(Hash32(n.to_bytes(32, byteorder="little"))))
        for n in range(current_slot, current_slot + 7)
    ]
    new_previous_epoch_attestations = [
        PendingAttestation.create(proposer_index=ValidatorIndex(n))
        for n in range(current_slot, current_slot + 5)
    ]
    new_current_epoch_attestations = [
        PendingAttestation.create(proposer_index=ValidatorIndex(n))
        for n in range(current_slot + 5, current_slot + 10)
    ]
    state = state.mset(
        "validators",
        new_validators,
        "balances",
        (32,) * len(new_validators),
        "eth1_data_votes",
        new_eth1_data_votes,
        "eth1_data",
        new_eth1_data_votes[0],
        "previous_epoch_attestations",
        new_previous_epoch_attestations,
        "current_epoch_attestations",
        new_current_epoch_attestations,
        "previous_justified_checkpoint",
        Checkpoint.create(epoch=Epoch(current_slot + 42)),
        "current_justified_checkpoint",
        Checkpoint.create(epoch=Epoch(current_slot + 43)),
        "finalized_checkpoint",
        Checkpoint.create(epoch=Epoch(current_slot + 44)),
    )

    return state
