from typing import Type, TypeVar

from ssz.hashable_container import HashableContainer
from ssz.sedes import uint64

from eth2.beacon.typing import ValidatorIndex

from .block_headers import BeaconBlockHeader, default_beacon_block_header
from .defaults import default_validator_index

TProposerSlashing = TypeVar("TProposerSlashing", bound="ProposerSlashing")


class ProposerSlashing(HashableContainer):

    fields = [
        # Proposer index
        ("proposer_index", uint64),
        # First block header
        ("header_1", BeaconBlockHeader),
        # Second block header
        ("header_2", BeaconBlockHeader),
    ]

    @classmethod
    def create(
        cls: Type[TProposerSlashing],
        proposer_index: ValidatorIndex = default_validator_index,
        header_1: BeaconBlockHeader = default_beacon_block_header,
        header_2: BeaconBlockHeader = default_beacon_block_header,
    ) -> TProposerSlashing:
        return super().create(
            proposer_index=proposer_index, header_1=header_1, header_2=header_2
        )

    def __str__(self) -> str:
        return (
            f"proposer_index={self.proposer_index},"
            f" header_1=({self.header_1}),"
            f" header_2=({self.header_2})"
        )
