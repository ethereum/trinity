from typing import Type, TypeVar

from ssz.hashable_container import HashableContainer
from ssz.sedes import Bitlist, uint64

from eth2.beacon.typing import Bitfield, ValidatorIndex

from .attestation_data import AttestationData, default_attestation_data
from .defaults import default_bitfield, default_validator_index

TPendingAttestation = TypeVar("TPendingAttestation", bound="PendingAttestation")


class PendingAttestation(HashableContainer):

    fields = [
        ("aggregation_bits", Bitlist(1)),
        ("data", AttestationData),
        ("inclusion_delay", uint64),
        ("proposer_index", uint64),
    ]

    @classmethod
    def create(
        cls: Type[TPendingAttestation],
        aggregation_bits: Bitfield = default_bitfield,
        data: AttestationData = default_attestation_data,
        inclusion_delay: int = 0,
        proposer_index: ValidatorIndex = default_validator_index,
    ) -> TPendingAttestation:
        return super().create(
            aggregation_bits=aggregation_bits,
            data=data,
            inclusion_delay=inclusion_delay,
            proposer_index=proposer_index,
        )

    def __str__(self) -> str:
        return (
            f"aggregation_bits={self.aggregation_bits},"
            f" data=({self.data}),"
            f" inclusion_delay={self.inclusion_delay},"
            f" proposer_index={self.proposer_index}"
        )
