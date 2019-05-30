import pytest

from eth_utils import (
    ValidationError,
)

from eth2.beacon.state_machines.forks.serenity.block_validation import (
    validate_proposer_slashing,
    validate_proposer_slashing_epoch,
    validate_proposer_slashing_headers,
    validate_proposer_slashing_is_slashed,
    validate_block_header_signature,
)
from eth2.beacon.tools.builder.validator import (
    create_mock_proposer_slashing_at_block,
)


def get_valid_proposer_slashing(state,
                                keymap,
                                config,
                                proposer_index=0):
    return create_mock_proposer_slashing_at_block(
        state,
        config,
        keymap,
        block_root_1=b'\x11' * 32,
        block_root_2=b'\x22' * 32,
        proposer_index=proposer_index,
    )


def test_validate_proposer_slashing_valid(genesis_state,
                                          keymap,
                                          slots_per_epoch,
                                          config):
    state = genesis_state
    valid_proposer_slashing = get_valid_proposer_slashing(
        state,
        keymap,
        config,
    )
    validate_proposer_slashing(state, valid_proposer_slashing, slots_per_epoch)


def test_validate_proposer_slashing_epoch(genesis_state,
                                          keymap,
                                          config):
    state = genesis_state
    valid_proposer_slashing = get_valid_proposer_slashing(
        state,
        keymap,
        config,
    )
    # Valid
    validate_proposer_slashing_epoch(valid_proposer_slashing, config.SLOTS_PER_EPOCH)

    header_1 = valid_proposer_slashing.header_1.copy(
        slot=valid_proposer_slashing.header_2.slot + 2 * config.SLOTS_PER_EPOCH
    )
    invalid_proposer_slashing = valid_proposer_slashing.copy(
        header_1=header_1,
    )

    # Invalid
    with pytest.raises(ValidationError):
        validate_proposer_slashing_epoch(invalid_proposer_slashing, config.SLOTS_PER_EPOCH)


def test_validate_proposer_slashing_headers(genesis_state,
                                            keymap,
                                            config):
    state = genesis_state
    valid_proposer_slashing = get_valid_proposer_slashing(
        state,
        keymap,
        config,
    )

    # Valid
    validate_proposer_slashing_headers(valid_proposer_slashing)

    invalid_proposer_slashing = valid_proposer_slashing.copy(
        header_1=valid_proposer_slashing.header_2,
    )

    # Invalid
    with pytest.raises(ValidationError):
        validate_proposer_slashing_headers(invalid_proposer_slashing)


@pytest.mark.parametrize(
    (
        'slashed', 'success'
    ),
    [
        (False, True),
        (True, False),
    ],
)
def test_validate_proposer_slashing_is_slashed(slashed,
                                               success):
    # Invalid
    if success:
        validate_proposer_slashing_is_slashed(slashed)
    else:
        with pytest.raises(ValidationError):
            validate_proposer_slashing_is_slashed(slashed)


def test_validate_block_header_signature(slots_per_epoch,
                                         genesis_state,
                                         keymap,
                                         config):
    state = genesis_state
    proposer_index = 0
    valid_proposer_slashing = get_valid_proposer_slashing(
        state,
        keymap,
        config,
    )
    proposer = state.validator_registry[proposer_index]

    # Valid
    validate_block_header_signature(
        header=valid_proposer_slashing.header_1,
        pubkey=proposer.pubkey,
        fork=state.fork,
        slots_per_epoch=slots_per_epoch,
    )

    # Invalid
    wrong_proposer_index = proposer_index + 1
    wrong_proposer = state.validator_registry[wrong_proposer_index]
    with pytest.raises(ValidationError):
        validate_block_header_signature(
            header=valid_proposer_slashing.header_1,
            pubkey=wrong_proposer.pubkey,
            fork=state.fork,
            slots_per_epoch=slots_per_epoch,
        )
