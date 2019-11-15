from typing import Type, TypeVar

from ssz.hashable_container import HashableContainer

from .attestations import IndexedAttestation, default_indexed_attestation

TAttesterSlashing = TypeVar("TAttesterSlashing", bound="AttesterSlashing")


class AttesterSlashing(HashableContainer):

    fields = [
        # First attestation
        ("attestation_1", IndexedAttestation),
        # Second attestation
        ("attestation_2", IndexedAttestation),
    ]

    @classmethod
    def create(
        cls: Type[TAttesterSlashing],
        attestation_1: IndexedAttestation = default_indexed_attestation,
        attestation_2: IndexedAttestation = default_indexed_attestation,
    ) -> TAttesterSlashing:
        return super().create(attestation_1=attestation_1, attestation_2=attestation_2)

    def __str__(self) -> str:
        return (
            f"attestation_1=({self.attestation_1}),"
            f" attestation_2=({self.attestation_2})"
        )
