from eth_utils import (
    ValidationError,
)

from eth2._utils.tuple import (
    update_tuple_item,
)
from eth2.beacon.committee_helpers import (
    get_beacon_proposer_index,
)
from eth2.beacon.configs import (
    CommitteeConfig,
)
from eth2.beacon.enums import (
    ValidatorStatusFlags,
)
from eth2.beacon.helpers import (
    get_entry_exit_effect_epoch,
    get_effective_balance,
    get_epoch_start_slot,
)
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import (
    Epoch,
    Gwei,
    Slot,
    ValidatorIndex,
)


#
# State update
#
def activate_validator(state: BeaconState,
                       index: ValidatorIndex,
                       is_genesis: bool,
                       genesis_epoch: Epoch,
                       slots_per_epoch: int,
                       activation_exit_delay: int) -> BeaconState:
    """
    Activate the validator with the given ``index``.
    Return the updated state (immutable).
    """
    # Update validator.activation_epoch
    validator = state.validator_registry[index].copy(
        activation_epoch=genesis_epoch if is_genesis else get_entry_exit_effect_epoch(
            state.current_epoch(slots_per_epoch),
            activation_exit_delay,
        )
    )
    state = state.update_validator_registry(index, validator)

    return state


def initiate_validator_exit(state: BeaconState,
                            index: ValidatorIndex) -> BeaconState:
    """
    Initiate exit for the validator with the given ``index``.
    Return the updated state (immutable).
    """
    validator = state.validator_registry[index]
    validator = validator.copy(
        status_flags=validator.status_flags | ValidatorStatusFlags.INITIATED_EXIT
    )
    state = state.update_validator_registry(index, validator)

    return state


def exit_validator(state: BeaconState,
                   index: ValidatorIndex,
                   slots_per_epoch: int,
                   activation_exit_delay: int) -> BeaconState:
    """
    Exit the validator with the given ``index``.
    Return the updated state (immutable).
    """
    validator = state.validator_registry[index]

    entry_exit_effect_epoch = get_entry_exit_effect_epoch(
        state.current_epoch(slots_per_epoch),
        activation_exit_delay,
    )

    # The following updates only occur if not previous exited
    if validator.exit_epoch <= entry_exit_effect_epoch:
        return state

    validator = validator.copy(
        exit_epoch=state.current_epoch(slots_per_epoch) + activation_exit_delay,
    )
    state = state.update_validator_registry(index, validator)

    return state


def _settle_penality_to_validator_and_whistleblower(
        *,
        state: BeaconState,
        validator_index: ValidatorIndex,
        latest_slashed_exit_length: int,
        whistleblower_reward_quotient: int,
        max_deposit_amount: Gwei,
        committee_config: CommitteeConfig) -> BeaconState:
    """
    Apply penality/reward to validator and whistleblower and update the meta data

    More intuitive pseudo-code:
    current_epoch_penalization_index = (state.slot // SLOTS_PER_EPOCH) % LATEST_SLASHED_EXIT_LENGTH
    state.latest_slashed_balances[current_epoch_penalization_index] += (
        get_effective_balance(state, index)
    )
    whistleblower_index = get_beacon_proposer_index(state, state.slot)
    whistleblower_reward = get_effective_balance(state, index) // WHISTLEBLOWER_REWARD_QUOTIENT
    state.validator_balances[whistleblower_index] += whistleblower_reward
    state.validator_balances[index] -= whistleblower_reward
    validator.slashed_epoch = slot_to_epoch(state.slot)
    """
    slots_per_epoch = committee_config.SLOTS_PER_EPOCH

    # Update `state.latest_slashed_balances`
    current_epoch_penalization_index = state.current_epoch(
        slots_per_epoch) % latest_slashed_exit_length
    effective_balance = get_effective_balance(
        state.validator_balances,
        validator_index,
        max_deposit_amount,
    )
    slashed_exit_balance = (
        state.latest_slashed_balances[current_epoch_penalization_index] +
        effective_balance
    )
    latest_slashed_balances = update_tuple_item(
        tuple_data=state.latest_slashed_balances,
        index=current_epoch_penalization_index,
        new_value=slashed_exit_balance,
    )
    state = state.copy(
        latest_slashed_balances=latest_slashed_balances,
    )

    # Update whistleblower's balance
    whistleblower_reward = (
        effective_balance //
        whistleblower_reward_quotient
    )
    whistleblower_index = get_beacon_proposer_index(
        state,
        state.slot,
        committee_config,
    )
    state = state.update_validator_balance(
        whistleblower_index,
        state.validator_balances[whistleblower_index] + whistleblower_reward,
    )

    # Update validator's balance and `slashed_epoch` field
    validator = state.validator_registry[validator_index]
    validator = validator.copy(
        slashed_epoch=state.current_epoch(slots_per_epoch),
    )
    state = state.update_validator(
        validator_index,
        validator,
        state.validator_balances[validator_index] - whistleblower_reward,
    )

    return state


def slash_validator(*,
                    state: BeaconState,
                    index: ValidatorIndex,
                    latest_slashed_exit_length: int,
                    whistleblower_reward_quotient: int,
                    max_deposit_amount: Gwei,
                    committee_config: CommitteeConfig) -> BeaconState:
    """
    Slash the validator with index ``index``.

    Exit the validator, penalize the validator, and reward the whistleblower.
    """
    slots_per_epoch = committee_config.SLOTS_PER_EPOCH
    activation_exit_delay = committee_config.ACTIVATION_EXIT_DELAY

    validator = state.validator_registry[index]

    # [TO BE REMOVED IN PHASE 2]
    _validate_withdrawable_epoch(state.slot, validator.withdrawable_epoch, slots_per_epoch)

    state = exit_validator(state, index, slots_per_epoch, activation_exit_delay)
    state = _settle_penality_to_validator_and_whistleblower(
        state=state,
        validator_index=index,
        latest_slashed_exit_length=latest_slashed_exit_length,
        whistleblower_reward_quotient=whistleblower_reward_quotient,
        max_deposit_amount=max_deposit_amount,
        committee_config=committee_config,
    )

    # Update validator
    current_epoch = state.current_epoch(slots_per_epoch)
    validator = validator.copy(
        slashed_epoch=current_epoch,
        withdrawable_epoch=current_epoch + latest_slashed_exit_length,
    )
    state.update_validator_registry(index, validator)

    return state


def prepare_validator_for_withdrawal(state: BeaconState,
                                     index: ValidatorIndex,
                                     slots_per_epoch: int,
                                     min_validator_withdrawability_delay: int) -> BeaconState:
    """
    Set the validator with the given ``index`` as withdrawable
    ``MIN_VALIDATOR_WITHDRAWABILITY_DELAY`` after the current epoch.
    """
    validator = state.validator_registry[index].copy(
        withdrawable_epoch=(
            state.current_epoch(slots_per_epoch) + min_validator_withdrawability_delay
        )
    )
    state = state.update_validator_registry(index, validator)

    return state


#
# Validation
#
def _validate_withdrawable_epoch(state_slot: Slot,
                                 validator_withdrawable_epoch: Epoch,
                                 slots_per_epoch: int) -> None:
    if state_slot >= get_epoch_start_slot(validator_withdrawable_epoch, slots_per_epoch):
        raise ValidationError(
            f"state.slot ({state_slot}) should be less than "
            f"validator.withdrawable_epoch ({validator_withdrawable_epoch})"
        )
