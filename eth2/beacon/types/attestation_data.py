from typing import Type, TypeVar

from eth_utils import humanize_hash
from ssz.hashable_container import HashableContainer
from ssz.sedes import bytes32, uint64

from eth2.beacon.constants import ZERO_ROOT
from eth2.beacon.types.checkpoints import Checkpoint, default_checkpoint
from eth2.beacon.types.defaults import default_committee_index, default_slot
from eth2.beacon.typing import CommitteeIndex, Root, Slot

TAttestationData = TypeVar("TAttestationData", bound="AttestationData")


class AttestationData(HashableContainer):

    fields = [
        ("slot", uint64),
        ("index", uint64),
        # LMD GHOST vote
        ("beacon_block_root", bytes32),
        # FFG vote
        ("source", Checkpoint),
        ("target", Checkpoint),
    ]

    @classmethod
    def create(
        cls: Type[TAttestationData],
        slot: Slot = default_slot,
        index: CommitteeIndex = default_committee_index,
        beacon_block_root: Root = ZERO_ROOT,
        source: Checkpoint = default_checkpoint,
        target: Checkpoint = default_checkpoint,
    ) -> TAttestationData:
        return super().create(
            slot=slot,
            index=index,
            beacon_block_root=beacon_block_root,
            source=source,
            target=target,
        )

    def __str__(self) -> str:
        return (
            f"slot={self.slot},"
            f" index={self.index},"
            f" beacon_block_root={humanize_hash(self.beacon_block_root)},"
            f" source=({self.source}),"
            f" target=({self.target}),"
        )


default_attestation_data = AttestationData.create()
