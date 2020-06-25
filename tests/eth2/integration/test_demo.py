import pytest

from eth2._utils.bls import bls
from eth2.beacon.committee_helpers import get_beacon_proposer_index
from eth2.beacon.constants import GENESIS_EPOCH, GENESIS_SLOT
from eth2.beacon.db.chain import BeaconChainDB
from eth2.beacon.fork_choice.higher_slot import HigherSlotScoring
from eth2.beacon.state_machines.forks.serenity.blocks import (
    SerenityBeaconBlock,
    SerenitySignedBeaconBlock,
)
from eth2.beacon.state_machines.forks.skeleton_lake import (
    MINIMAL_SERENITY_CONFIG,
    SkeletonLakeStateMachine,
)
from eth2.beacon.tools.builder.initializer import create_mock_genesis
from eth2.beacon.tools.builder.proposer import (
    create_block_proposal,
    generate_randao_reveal,
    sign_block,
)
from eth2.beacon.tools.builder.validator import create_mock_signed_attestations_at_slot
from eth2.beacon.tools.misc.ssz_vector import override_lengths


@pytest.fixture
def fork_choice_scoring():
    return HigherSlotScoring()


@pytest.mark.parametrize(("validator_count"), ((40),))
def test_demo(base_db, validator_count, keymap, pubkeys, fork_choice_scoring):
    bls.use_noop_backend()
    config = MINIMAL_SERENITY_CONFIG
    override_lengths(config)

    genesis_slot = GENESIS_SLOT
    genesis_epoch = GENESIS_EPOCH

    genesis_state, genesis_block = create_mock_genesis(
        pubkeys=pubkeys[:validator_count],
        config=config,
        keymap=keymap,
        genesis_block_class=SerenityBeaconBlock,
    )

    chaindb = BeaconChainDB.from_genesis(
        base_db, genesis_state, SerenitySignedBeaconBlock, fork_choice_scoring
    )
    fixture_sm = SkeletonLakeStateMachine(chaindb)

    for i in range(validator_count):
        assert genesis_state.validators[i].is_active(genesis_slot)

    state = genesis_state
    block = SerenitySignedBeaconBlock.create(message=genesis_block)

    chain_length = 4 * config.SLOTS_PER_EPOCH
    blocks = (block,)

    attestations_map = {}  # Dict[Slot, Sequence[Attestation]]

    for current_slot in range(genesis_slot + 1, genesis_slot + chain_length + 1):
        if current_slot > genesis_slot + config.MIN_ATTESTATION_INCLUSION_DELAY:
            attestations = attestations_map[
                current_slot - config.MIN_ATTESTATION_INCLUSION_DELAY
            ]
        else:
            attestations = ()

        future_state, _ = fixture_sm.apply_state_transition(
            state, future_slot=current_slot
        )
        proposer_index = get_beacon_proposer_index(future_state, config)
        proposer_pubkey = state.validators[proposer_index].pubkey
        private_key = keymap[proposer_pubkey]
        randao_reveal = generate_randao_reveal(private_key, current_slot, state, config)
        block_proposal = create_block_proposal(
            current_slot,
            block.message.hash_tree_root,
            randao_reveal,
            state.eth1_data,
            attestations,
            state,
            fixture_sm,
        )
        block = sign_block(state, block_proposal, private_key, config.SLOTS_PER_EPOCH)

        state, block = fixture_sm.apply_state_transition(state, block)

        chaindb.persist_state(state)
        chaindb.persist_block(block, SerenitySignedBeaconBlock, fork_choice_scoring)

        blocks += (block,)

        # Mock attestations
        attestation_slot = current_slot
        attestations = create_mock_signed_attestations_at_slot(
            state=state,
            config=config,
            state_machine=fixture_sm,
            attestation_slot=attestation_slot,
            beacon_block_root=block.message.hash_tree_root,
            keymap=keymap,
            voted_attesters_ratio=1.0,
        )
        attestations_map[attestation_slot] = attestations

    assert state.slot == chain_length + genesis_slot

    # Justification assertions
    # NOTE: why are the number `2` or `3` used in the checks below?
    # Answer:
    # "We do not check any justification and finality during epochs 0 or 1. We do check for
    # justification and finality from epoch 2 onward."
    # [epoch 0]------[epoch 1]------>
    #
    # "In epoch 2, we justify the current epoch. This epoch is in fact justified but we do not
    # recognize it in the protocol due to an artifact of the construction of the genesis state
    # (using the `zero` value for `Checkpoint` type)."
    # [epoch 0]------[epoch 1]------[epoch 2]*------>
    # []*: checkpoint justified
    # []**: checkpoint finalized
    #
    # "In epoch 3, we have the previous justified checkpoint at the prior current justified
    # checkpoint (so `GENESIS_EPOCH + 2`) and we justify this current epoch. we check finality here
    # and see that we finalize the prior justified checkpoint at epoch 2."
    # [epoch 0]------[epoch 1]------[epoch 2]**------[epoch 3]*------>
    #
    # "Given the way we handle epoch processing (i.e. process a given epoch at the start of
    # the next epoch), we need to transition through `4 * SLOTS_PER_EPOCH` worth of slots to
    # include the processing of epoch 3."
    #
    # source: https://github.com/ethereum/trinity/pull/1214#issuecomment-546184080
    #
    # epoch | prev_justified_checkpoint | cur_justified_checkpoint | finalized_checkpoint
    # ------|---------------------------|--------------------------|---------------------
    # 0     | 0                         | 0                        | 0
    # 1     | 0                         | 0                        | 0
    # 2     | 0                         | 0                        | 0
    # 3     | 0                         | 2                        | 0
    # 4     | 2                         | 3                        | 2
    assert state.previous_justified_checkpoint.epoch == 2 + genesis_epoch
    assert state.current_justified_checkpoint.epoch == 3 + genesis_epoch
    assert state.finalized_checkpoint.epoch == 2 + genesis_epoch
