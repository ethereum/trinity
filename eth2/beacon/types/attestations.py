from typing import Sequence, Type, TypeVar

from eth_typing import BLSSignature
from eth_utils import humanize_hash
from ssz.hashable_container import HashableContainer
from ssz.sedes import Bitlist, List, bytes96, uint64

from eth2.beacon.constants import EMPTY_SIGNATURE
from eth2.beacon.typing import Bitfield, ValidatorIndex

from .attestation_data import AttestationData, default_attestation_data
from .defaults import default_bitfield, default_tuple

TAttestation = TypeVar("TAttestation", bound="Attestation")


class Attestation(HashableContainer):

    fields = [
        ("aggregation_bits", Bitlist(1)),
        ("data", AttestationData),
        ("signature", bytes96),
    ]

    @classmethod
    def create(
        cls: Type[TAttestation],
        aggregation_bits: Bitfield = default_bitfield,
        data: AttestationData = default_attestation_data,
        signature: BLSSignature = EMPTY_SIGNATURE,
    ) -> TAttestation:
        return super().create(
            aggregation_bits=aggregation_bits, data=data, signature=signature
        )

    def __str__(self) -> str:
        return (
            f"aggregation_bits={self.aggregation_bits},"
            f" data=({self.data}),"
            f" signature={humanize_hash(self.signature)}"
        )


default_attestation = Attestation.create()


TIndexedAttestation = TypeVar("TIndexedAttestation", bound="IndexedAttestation")


class IndexedAttestation(HashableContainer):

    fields = [
        # Validator indices
        ("attesting_indices", List(uint64, 1)),
        # Attestation data
        ("data", AttestationData),
        # Aggregate signature
        ("signature", bytes96),
    ]

    @classmethod
    def create(
        cls: Type[TIndexedAttestation],
        attesting_indices: Sequence[ValidatorIndex] = default_tuple,
        data: AttestationData = default_attestation_data,
        signature: BLSSignature = EMPTY_SIGNATURE,
    ) -> TIndexedAttestation:
        return super().create(
            attesting_indices=attesting_indices, data=data, signature=signature
        )

    def __str__(self) -> str:
        return (
            f"attesting_indices={self.attesting_indices},"
            f" data=({self.data}),"
            f" signature={humanize_hash(self.signature)}"
        )


default_indexed_attestation = IndexedAttestation.create()
