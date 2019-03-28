import pytest

from eth2.beacon.committee_helpers import (
    get_beacon_proposer_index,
)
from eth2.beacon.state_machines.forks.serenity.operation_processing import (
    process_transfer,
)
from eth2.beacon.tools.builder.validator import (
    create_mock_transfer,
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
def test_validate_transfer(genesis_state, config, committee_config, keymap, num_validators):
    slot = genesis_state.slot + config.SLOTS_PER_EPOCH * 1
    state = genesis_state.copy(
        slot=slot,
    )
    proposer_index = get_beacon_proposer_index(
        state,
        slot,
        committee_config,
    )
    # Make sure that sender != proposer
    sender = (proposer_index + 1) % num_validators
    recipient = (proposer_index + 2) % num_validators
    amount = 10
    fee = 2
    sender_validator = state.validator_registry[sender].copy(
        withdrawable_epoch=config.GENESIS_EPOCH,
    )
    state = state.update_validator_registry(
        sender,
        sender_validator,
    )
    transfer = create_mock_transfer(
        state=state,
        config=config,
        keymap=keymap,
        sender=sender,
        recipient=recipient,
        amount=amount,
        fee=fee,
        slot=slot,
    )
    result_state = process_transfer(state, config, transfer)

    assert result_state.validator_balances[sender] == (
        state.validator_balances[sender] - (amount + fee)
    )
    assert result_state.validator_balances[recipient] == (
        state.validator_balances[recipient] + amount
    )
