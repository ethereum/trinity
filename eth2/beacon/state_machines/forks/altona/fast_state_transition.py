from eth2.beacon.state_machines.forks.altona.eth2fastspec import process_slots, EpochsContext, process_block
from eth2.beacon.types.blocks import BaseSignedBeaconBlock
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Slot


def apply_fast_state_transition(
    epochs_ctx: EpochsContext,
    state: BeaconState,
    signed_block: BaseSignedBeaconBlock = None,
    future_slot: Slot = None,
    check_proposer_signature: bool = True,
) -> BeaconState:
    """
    Callers should request a transition to some slot past the ``state.slot``.
    This can be done by providing either a ``block`` *or* a ``future_slot``.
    We enforce this invariant with the assertion on ``target_slot``.
    """
    target_slot = signed_block.message.slot if signed_block else future_slot
    assert target_slot is not None

    state = process_slots(epochs_ctx, state, target_slot)

    if signed_block:
        if check_proposer_signature:
            # validate_proposer_signature(state, signed_block)
            pass
        state = process_block(epochs_ctx, state, signed_block.message)

    return state
