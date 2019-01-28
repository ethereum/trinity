import math

from typing import (
    Tuple,
)
from eth2._utils.tuple import (
    update_tuple_item,
)
from eth2.beacon.helpers import (
    get_current_epoch_committee_count_per_slot,
    get_randao_mix,
)
from eth2.beacon.types.states import BeaconState
from eth2.beacon.state_machines.configs import BeaconConfig


#
# Validator Registry
#
def _is_power_of_two(value: int) -> bool:
    if value == 0:
        return False
    else:
        return (math.log2(value) + 1).is_integer()


def _check_if_update_validator_registry(state: BeaconState,
                                        config: BeaconConfig) -> Tuple[bool, int]:
    if state.finalized_slot <= state.validator_registry_update_slot:
        return False, 0

    num_shards_in_committees = get_current_epoch_committee_count_per_slot(
        state,
        shard_count=config.SHARD_COUNT,
        epoch_length=config.EPOCH_LENGTH,
        target_committee_size=config.TARGET_COMMITTEE_SIZE,
    ) * config.EPOCH_LENGTH

    # Get every shard in the current committees
    shards = set([
        (state.current_epoch_start_shard + i) % config.SHARD_COUNT
        for i in range(num_shards_in_committees)
    ])
    for shard in shards:
        if state.latest_crosslinks[shard].slot <= state.validator_registry_update_slot:
            return False, 0

    return True, num_shards_in_committees


def update_validator_registry(state: BeaconState) -> BeaconState:
    # TODO
    return state


def process_validator_registry(state: BeaconState,
                               config: BeaconConfig) -> BeaconState:
    state = state.copy(
        previous_epoch_calculation_slot=state.current_epoch_calculation_slot,
        previous_epoch_start_shard=state.current_epoch_start_shard,
        previous_epoch_randao_mix=state.current_epoch_randao_mix,
    )

    need_to_update, num_shards_in_committees = _check_if_update_validator_registry(state, config)

    if need_to_update:
        state = update_validator_registry(state)
        state = state.copy(
            current_epoch_calculation_slot=state.slot,
        )
        state = state.copy(
            current_epoch_start_shard=(
                state.current_epoch_start_shard + num_shards_in_committees
            ) % config.SHARD_COUNT,
        )
        state = state.copy(
            current_epoch_randao_mix=get_randao_mix(
                state,
                state.current_epoch_calculation_slot - config.SEED_LOOKAHEAD,
                config.LATEST_RANDAO_MIXES_LENGTH,
            ),
        )
    else:
        epochs_since_last_registry_change = (
            state.slot - state.validator_registry_update_slot
        ) // config.EPOCH_LENGTH
        if _is_power_of_two(epochs_since_last_registry_change):
            state = state.copy(
                current_epoch_calculation_slot=state.slot,
            )
            state = state.copy(
                current_epoch_randao_mix=get_randao_mix(
                    state,
                    state.current_epoch_calculation_slot - config.SEED_LOOKAHEAD,
                    config.LATEST_RANDAO_MIXES_LENGTH,
                ),
            )

    return state


#
# Final updates
#
def process_final_updates(state: BeaconState,
                          config: BeaconConfig) -> BeaconState:
    epoch = state.slot // config.EPOCH_LENGTH
    current_index = (epoch + 1) % config.LATEST_PENALIZED_EXIT_LENGTH
    previous_index = epoch & config.LATEST_PENALIZED_EXIT_LENGTH

    state = state.copy(
        latest_penalized_balances=update_tuple_item(
            state.latest_penalized_balances,
            current_index,
            state.latest_penalized_balances[previous_index],
        ),
    )
    latest_attestations = tuple(
        filter(
            lambda attestation: attestation.data.slot >= state.slot - config.EPOCH_LENGTH,
            state.latest_attestations
        )
    )
    state = state.copy(
        latest_attestations=latest_attestations,
    )

    return state
