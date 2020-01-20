from typing import Tuple

from eth_utils import ValidationError

from eth2.beacon.state_machines.forks.serenity.block_validation import (
    validate_attestation_slot,
)
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.typing import CommitteeIndex, Root, Slot
from eth2.configs import Eth2Config

from .pool import OperationPool


def is_valid_slot(
    attestation: Attestation, current_slot: Slot, config: Eth2Config
) -> bool:
    try:
        validate_attestation_slot(
            attestation.data.slot,
            current_slot,
            config.SLOTS_PER_EPOCH,
            config.MIN_ATTESTATION_INCLUSION_DELAY,
        )
    except ValidationError:
        return False
    else:
        return True


class AttestationPool(OperationPool[Attestation]):
    def get_valid_attestation_by_current_slot(
        self, slot: Slot, config: Eth2Config
    ) -> Tuple[Attestation, ...]:
        return tuple(
            filter(
                lambda attestation: is_valid_slot(attestation, slot, config),
                self._pool_storage.values(),
            )
        )

    def get_acceptable_attestations(
        self, slot: Slot, committee_index: CommitteeIndex, beacon_block_root: Root
    ) -> Tuple[Attestation, ...]:
        return tuple(
            filter(
                lambda attestation: (
                    beacon_block_root == attestation.data.beacon_block_root
                    and slot == attestation.data.slot
                    and committee_index == attestation.data.index
                ),
                self._pool_storage.values(),
            )
        )
