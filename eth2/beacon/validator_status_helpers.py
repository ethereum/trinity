from functools import partial

from eth_utils.toolz import curry

from eth2.beacon.committee_helpers import get_beacon_proposer_index
from eth2.beacon.constants import FAR_FUTURE_EPOCH
from eth2.beacon.epoch_processing_helpers import (
    compute_activation_exit_epoch,
    decrease_balance,
    get_validator_churn_limit,
    increase_balance,
)
from eth2.beacon.types.states import BeaconState
from eth2.beacon.types.validators import Validator
from eth2.beacon.typing import Epoch, Gwei, ValidatorIndex
from eth2.configs import CommitteeConfig, Eth2Config


def activate_validator(validator: Validator, activation_epoch: Epoch) -> Validator:
    return validator.mset(
        "activation_eligibility_epoch",
        activation_epoch,
        "activation_epoch",
        activation_epoch,
    )


def _compute_exit_queue_epoch(
    state: BeaconState, churn_limit: int, config: Eth2Config
) -> Epoch:
    slots_per_epoch = config.SLOTS_PER_EPOCH

    exit_epochs = tuple(
        v.exit_epoch for v in state.validators if v.exit_epoch != FAR_FUTURE_EPOCH
    )
    exit_queue_epoch = max(
        exit_epochs
        + (
            compute_activation_exit_epoch(
                state.current_epoch(slots_per_epoch), config.MAX_SEED_LOOKAHEAD
            ),
        )
    )
    exit_queue_churn = len(
        tuple(v for v in state.validators if v.exit_epoch == exit_queue_epoch)
    )
    if exit_queue_churn >= churn_limit:
        exit_queue_epoch += 1
    return Epoch(exit_queue_epoch)


# NOTE: adding ``curry`` here gets mypy to allow use of this elsewhere.
@curry
def initiate_exit_for_validator(
    validator: Validator, state: BeaconState, config: Eth2Config
) -> Validator:
    """
    Performs the mutations to ``validator`` used to initiate an exit.
    More convenient given our immutability patterns compared to ``initiate_validator_exit``.
    """
    if validator.exit_epoch != FAR_FUTURE_EPOCH:
        return validator

    churn_limit = get_validator_churn_limit(state, config)
    exit_queue_epoch = _compute_exit_queue_epoch(state, churn_limit, config)

    return validator.mset(
        "exit_epoch",
        exit_queue_epoch,
        "withdrawable_epoch",
        Epoch(exit_queue_epoch + config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY),
    )


def initiate_validator_exit(
    state: BeaconState, index: ValidatorIndex, config: Eth2Config
) -> BeaconState:
    """
    Initiate exit for the validator with the given ``index``.
    Return the updated state (immutable).
    """
    return state.transform(
        ("validators", index),
        partial(initiate_exit_for_validator, state=state, config=config),
    )


@curry
def _set_validator_slashed(
    v: Validator, current_epoch: Epoch, epochs_per_slashings_vector: int
) -> Validator:
    return v.mset(
        "slashed",
        True,
        "withdrawable_epoch",
        max(v.withdrawable_epoch, Epoch(current_epoch + epochs_per_slashings_vector)),
    )


def slash_validator(
    state: BeaconState,
    index: ValidatorIndex,
    config: Eth2Config,
    whistleblower_index: ValidatorIndex = None,
) -> BeaconState:
    """
    Slash the validator with index ``index``.

    Exit the validator, penalize the validator, and reward the whistleblower.
    """
    # NOTE: remove in phase 1
    assert whistleblower_index is None

    slots_per_epoch = config.SLOTS_PER_EPOCH

    current_epoch = state.current_epoch(slots_per_epoch)

    state = initiate_validator_exit(state, index, config)

    state = state.transform(
        ("validators", index),
        partial(
            _set_validator_slashed,
            current_epoch=current_epoch,
            epochs_per_slashings_vector=config.EPOCHS_PER_SLASHINGS_VECTOR,
        ),
    )

    slashed_balance = state.validators[index].effective_balance
    slashed_epoch = current_epoch % config.EPOCHS_PER_SLASHINGS_VECTOR
    state = state.transform(
        ("slashings", slashed_epoch), lambda balance: Gwei(balance + slashed_balance)
    )
    state = decrease_balance(
        state, index, slashed_balance // config.MIN_SLASHING_PENALTY_QUOTIENT
    )

    proposer_index = get_beacon_proposer_index(state, CommitteeConfig(config))
    if whistleblower_index is None:
        whistleblower_index = proposer_index
    whistleblower_reward = Gwei(slashed_balance // config.WHISTLEBLOWER_REWARD_QUOTIENT)
    proposer_reward = Gwei(whistleblower_reward // config.PROPOSER_REWARD_QUOTIENT)
    state = increase_balance(state, proposer_index, proposer_reward)
    state = increase_balance(
        state, whistleblower_index, Gwei(whistleblower_reward - proposer_reward)
    )

    return state
