from eth.constants import ZERO_HASH32
from eth_typing import Hash32
from eth_utils import ValidationError
from ssz.hashable_list import HashableList

from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Slot
from eth2.configs import Eth2Config

from .epoch_processing import process_epoch


def _update_historical_root(
    roots: HashableList[Hash32],
    index: Slot,
    slots_per_historical_root: int,
    new_root: Hash32,
) -> HashableList[Hash32]:
    return roots.set(index % slots_per_historical_root, new_root)


def _process_slot(state: BeaconState, config: Eth2Config) -> BeaconState:
    slots_per_historical_root = config.SLOTS_PER_HISTORICAL_ROOT

    previous_state_root = state.hash_tree_root
    updated_state_roots = _update_historical_root(
        state.state_roots, state.slot, slots_per_historical_root, previous_state_root
    )

    if state.latest_block_header.state_root == ZERO_HASH32:
        state = state.transform(
            ("latest_block_header", "state_root"), previous_state_root
        )

    updated_block_roots = _update_historical_root(
        state.block_roots,
        state.slot,
        slots_per_historical_root,
        state.latest_block_header.hash_tree_root,
    )

    return state.mset(
        "block_roots", updated_block_roots, "state_roots", updated_state_roots
    )


def _increment_slot(state: BeaconState) -> BeaconState:
    return state.set("slot", state.slot + 1)


def process_slots(state: BeaconState, slot: Slot, config: Eth2Config) -> BeaconState:
    if state.slot > slot:
        raise ValidationError(
            f"Requested a slot transition at {slot}, behind the current slot {state.slot}"
        )

    # NOTE: ``while`` is guaranteed to terminate if we do not raise the previous ValidationError
    while state.slot < slot:
        state = _process_slot(state, config)

        if (state.slot + 1) % config.SLOTS_PER_EPOCH == 0:
            state = process_epoch(state, config)

        state = _increment_slot(state)

    return state
