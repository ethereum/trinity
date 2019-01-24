import pytest

from eth2.beacon.state_machines.forks.serenity.epoch_processing import (
    process_final_updates,
)


@pytest.mark.parametrize(
    (
        'num_validators,'
        'state_slot,'
        'epoch_length'
    ),
    [
        (10, 0, 5),
        (10, 5, 5),
        (10, 6, 5),
    ]
)
def test_process_final_updates(genesis_state,
                               state_slot,
                               config):
    state = genesis_state.copy(
        slot=state_slot,
    )

    result_state = process_final_updates(state, config)

    # TODO: test latest_penalized_balances after we add `penalize_validator`

    for attestation in result_state.latest_attestations:
        assert attestation.data.slot >= state_slot - config.EPOCH_LENGTH
