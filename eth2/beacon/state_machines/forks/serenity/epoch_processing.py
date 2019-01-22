from eth2._utils.tuple import (
    update_tuple_item,
)
from eth2.beacon.types.blocks import BaseBeaconBlock
from eth2.beacon.types.states import BeaconState
from eth2.beacon.state_machines.configs import BeaconConfig

# def filter_attestation(latest_attestations: Attestation, state_slot: SlotNumber)


def process_final_updates(state: BeaconState,
                          block: BaseBeaconBlock,
                          config: BeaconConfig) -> BeaconState:
    epoch = state.slot // config.EPOCH_LENGTH
    current_index = (epoch + 1) % config.LATEST_PENALIZED_EXIT_LENGTH
    previous_index = epoch & config.LATEST_PENALIZED_EXIT_LENGTH

    state = state.copy(
        latest_penalized_exit_balances=update_tuple_item(
            state.latest_penalized_exit_balances,
            current_index,
            state.latest_penalized_exit_balances[previous_index],
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
