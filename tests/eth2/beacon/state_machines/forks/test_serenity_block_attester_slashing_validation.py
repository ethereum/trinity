import pytest

from eth_utils import (
    ValidationError,
)

from eth2.beacon.state_machines.forks.serenity.block_validation import (
    validate_attester_slashing,
    validate_attester_slashing_different_data,
)
from eth2.beacon.tools.builder.validator import (
    create_mock_double_voted_attester_slashing,
)


def get_valid_attester_slashing(attesting_state,
                                keymap,
                                config):
    return create_mock_double_voted_attester_slashing(
        attesting_state,
        config,
        keymap,
        attestation_epoch=0,
    )


@pytest.mark.parametrize(
    (
        'num_validators',
        'slots_per_epoch',
        'target_committee_size',
        'shard_count',
    ),
    [
        (40, 2, 2, 2),
    ]
)
def test_validate_proposer_slashing_valid(genesis_state,
                                          keymap,
                                          slots_per_epoch,
                                          max_indices_per_slashable_vote,
                                          config):
    attesting_state = genesis_state.copy(
        slot=genesis_state.slot + slots_per_epoch,
    )
    valid_proposer_slashing = get_valid_attester_slashing(
        attesting_state,
        keymap,
        config,
    )

    state = attesting_state.copy(
        slot=attesting_state.slot + 1,
    )
    validate_attester_slashing(
        state,
        valid_proposer_slashing,
        max_indices_per_slashable_vote,
        slots_per_epoch,
    )


@pytest.mark.parametrize(
    (
        'num_validators',
        'slots_per_epoch',
        'target_committee_size',
        'shard_count',
    ),
    [
        (40, 2, 2, 2),
    ]
)
def test_validate_attester_slashing_different_data(
        genesis_state,
        keymap,
        slots_per_epoch,
        max_indices_per_slashable_vote,
        config):
    attesting_state = genesis_state.copy(
        slot=genesis_state.slot + slots_per_epoch,
    )
    valid_proposer_slashing = get_valid_attester_slashing(
        attesting_state,
        keymap,
        config,
    )

    with pytest.raises(ValidationError):
        validate_attester_slashing_different_data(
            valid_proposer_slashing.slashable_attestation_1,
            valid_proposer_slashing.slashable_attestation_1,  # Put the same SlashableAttestation
        )
