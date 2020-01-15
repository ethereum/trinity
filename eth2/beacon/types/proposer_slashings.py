from typing import Type, TypeVar

from ssz.hashable_container import HashableContainer
from ssz.sedes import uint64

from eth2.beacon.typing import ValidatorIndex

from .block_headers import SignedBeaconBlockHeader, default_signed_beacon_block_header
from .defaults import default_validator_index

TProposerSlashing = TypeVar("TProposerSlashing", bound="ProposerSlashing")


class ProposerSlashing(HashableContainer):

    fields = [
        # Proposer index
        ("proposer_index", uint64),
        # First block header
        ("signed_header_1", SignedBeaconBlockHeader),
        # Second block header
        ("signed_header_2", SignedBeaconBlockHeader),
    ]

    @classmethod
    def create(
        cls: Type[TProposerSlashing],
        proposer_index: ValidatorIndex = default_validator_index,
        signed_header_1: SignedBeaconBlockHeader = default_signed_beacon_block_header,
        signed_header_2: SignedBeaconBlockHeader = default_signed_beacon_block_header,
    ) -> TProposerSlashing:
        return super().create(
            proposer_index=proposer_index,
            signed_header_1=signed_header_1,
            signed_header_2=signed_header_2,
        )

    def __str__(self) -> str:
        return (
            f"proposer_index={self.proposer_index},"
            f" signed_header_1=({self.signed_header_1}),"
            f" signed_header_2=({self.signed_header_2})"
        )
