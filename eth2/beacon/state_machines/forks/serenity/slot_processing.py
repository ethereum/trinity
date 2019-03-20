from typing import (
    Sequence,
)

from eth.constants import (
    ZERO_HASH32,
)

from eth_typing import (
    Hash32,
)

from eth2.beacon.configs import (
    BeaconConfig,
)
from eth2.beacon.helpers import (
    get_state_root
)
from eth2.beacon.typing import (
    Slot,
)
from eth2.beacon.types.states import BeaconState


def _update_historical_root(roots: Sequence[Hash32],
                            index: Slot,
                            slots_per_historical_root: int,
                            new_root: Hash32) -> Sequence[Hash32]:
    mutable_roots = list(roots)
    mutable_roots[index % slots_per_historical_root] = new_root
    return tuple(mutable_roots)


def process_slot_transition(state: BeaconState, config: BeaconConfig) -> BeaconState:
    slots_per_historical_root = config.SLOTS_PER_HISTORICAL_ROOT

    # Update state.latest_state_roots
    # TODO ensure this becomes the `hash_tree_root` of the `state`
    latest_state_root = state.root
    updated_latest_state_roots = _update_historical_root(
        state.latest_state_roots,
        state.slot,
        slots_per_historical_root,
        latest_state_root,
    )

    # Update state.slot
    state = state.copy(
        slot=state.slot + 1
    )

    if state.latest_block_header.state_root == ZERO_HASH32:
        latest_block_header = state.latest_block_header
        latest_block_header.state_root = get_state_root(
            state,
            state.slot - 1,
            config.SLOTS_PER_HISTORICAL_ROOT,
        )
        state = state.copy(
            latest_block_header=latest_block_header,
        )

    # Update state.latest_block_roots
    updated_latest_block_roots = _update_historical_root(
        state.latest_block_roots,
        state.slot - 1,
        slots_per_historical_root,
        state.latest_block_header.root,
    )

    state = state.copy(
        latest_block_roots=updated_latest_block_roots,
        latest_state_roots=updated_latest_state_roots,
    )

    return state
