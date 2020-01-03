from typing import Any, Dict, Iterable

from eth_utils import ValidationError, encode_hex, to_tuple

from eth2.beacon.committee_helpers import (
    get_beacon_proposer_index,
    iterate_committees_at_epoch,
)
from eth2.beacon.state_machines.base import BaseBeaconStateMachine
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Epoch
from eth2.configs import CommitteeConfig, Eth2Config


@to_tuple
def get_validator_duties(
    state: BeaconState,
    state_machine: BaseBeaconStateMachine,
    epoch: Epoch,
    config: Eth2Config,
) -> Iterable[Dict[str, Any]]:
    current_epoch = state.current_epoch(config.SLOTS_PER_EPOCH)
    next_epoch = state.next_epoch(config.SLOTS_PER_EPOCH)

    if epoch < current_epoch:
        raise ValidationError(
            f"Epoch for committee assignment ({epoch})"
            f" must not be before current epoch {current_epoch}."
        )
    elif epoch > next_epoch:
        raise ValidationError(
            f"Epoch for committee assignment ({epoch}) must not be after next epoch {next_epoch}."
        )

    for committee, committee_index, slot in iterate_committees_at_epoch(
        state, epoch, CommitteeConfig(config)
    ):
        if slot < state.slot:
            continue

        state = state_machine.state_transition.apply_state_transition(
            state, future_slot=slot
        )
        proposer_index = get_beacon_proposer_index(state, CommitteeConfig(config))
        for validator_index in committee:
            pubkey = state.validators[validator_index].pubkey
            proposal_slot = slot if proposer_index == validator_index else None
            yield {
                "validator_pubkey": encode_hex(pubkey),
                "attestation_slot": slot,
                "committee_index": committee_index,
                "block_proposal_slot": proposal_slot,
            }
