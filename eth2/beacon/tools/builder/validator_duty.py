from typing import Any, Dict, Iterable

from eth_utils import ValidationError, encode_hex, to_tuple

from eth2.beacon.committee_helpers import iterate_committees_at_epoch
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import CommitteeIndex, Epoch, Slot
from eth2.configs import CommitteeConfig, Eth2Config


@to_tuple
def get_validator_duties(
    state: BeaconState,
    config: Eth2Config,
    epoch: Epoch,
) -> Iterable[Dict[str, Any]]:
    next_epoch = state.next_epoch(config.SLOTS_PER_EPOCH)
    if epoch > next_epoch:
        raise ValidationError(
            f"Epoch for committee assignment ({epoch}) must not be after next epoch {next_epoch}."
        )

    for committee, committee_index, slot in iterate_committees_at_epoch(
        state, epoch, CommitteeConfig(config)
    ):
        for validator_index in committee:
            pubkey = state.validators[validator_index].pubkey
            yield {
                "validator_pubkey": encode_hex(pubkey),
                "attestation_slot": slot,
                "committee_index": committee_index,
                "block_proposal_slot": slot,
            }
