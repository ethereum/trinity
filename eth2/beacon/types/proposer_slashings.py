from typing import Type, TypeVar

from ssz.hashable_container import HashableContainer

from .block_headers import SignedBeaconBlockHeader, default_signed_beacon_block_header

TProposerSlashing = TypeVar("TProposerSlashing", bound="ProposerSlashing")


class ProposerSlashing(HashableContainer):

    fields = [
        # First block header
        ("signed_header_1", SignedBeaconBlockHeader),
        # Second block header
        ("signed_header_2", SignedBeaconBlockHeader),
    ]

    @classmethod
    def create(
        cls: Type[TProposerSlashing],
        signed_header_1: SignedBeaconBlockHeader = default_signed_beacon_block_header,
        signed_header_2: SignedBeaconBlockHeader = default_signed_beacon_block_header,
    ) -> TProposerSlashing:
        return super().create(
            signed_header_1=signed_header_1,
            signed_header_2=signed_header_2,
        )

    def __str__(self) -> str:
        return (
            f" signed_header_1=({self.signed_header_1}),"
            f" signed_header_2=({self.signed_header_2})"
        )
