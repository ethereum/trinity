import pytest

from eth2.beacon.db.chain import BeaconChainDB
from eth2.beacon.state_machines.forks.serenity.blocks import (
    SerenityBeaconBlock,
)
from eth2.beacon.tools.builder.initializer import (
    mock_genesis,
)


@pytest.mark.parametrize(
    (
        'num_validators,'
        'epoch_length,'
        'min_attestation_inclusion_delay,'
        'target_committee_size,'
        'shard_count'
    ),
    [
        (20, 4, 2, 2, 2)
    ]
)
def test_demo(base_db,
              num_validators,
              config,
              keymap,
              fixture_sm_class,
              create_mock_block,
              create_mock_signed_attestations_at_slot):
    chaindb = BeaconChainDB(base_db)

    genesis_state, genesis_block = mock_genesis(
        num_validators=num_validators,
        config=config,
        keymap=keymap,
        genesis_block_class=SerenityBeaconBlock,
    )
    for i in range(num_validators):
        assert genesis_state.validator_registry[i].is_active(0)

    chaindb.persist_block(genesis_block, SerenityBeaconBlock)
    chaindb.persist_state(genesis_state)

    state = genesis_state

    current_slot = 1
    chain_length = 3 * config.EPOCH_LENGTH
    attestations = ()
    for current_slot in range(chain_length):
        # two epochs
        block = create_mock_block(
            state=state,
            block_class=SerenityBeaconBlock,
            parent_block=genesis_block,
            config=config,
            slot=current_slot,
            attestations=attestations,
        )
        block = block.copy(
            body=block.body.copy(
                attestations=attestations,
            )
        )

        # Get state machine instance
        sm = fixture_sm_class(
            chaindb,
            block,
            parent_block_class=SerenityBeaconBlock,
        )
        state, _ = sm.import_block(block)

        # TODO: move to chain level?
        block = block.copy(
            state_root=state.root,
        )

        chaindb.persist_state(state)
        chaindb.persist_block(block, SerenityBeaconBlock)

        if current_slot > config.MIN_ATTESTATION_INCLUSION_DELAY:
            attestation_slot = current_slot - config.MIN_ATTESTATION_INCLUSION_DELAY
            attestations = create_mock_signed_attestations_at_slot(
                state,
                attestation_slot,
                1.0,
            )
        else:
            attestations = ()

    assert state.slot == chain_length - 1
    assert isinstance(sm.block, SerenityBeaconBlock)
