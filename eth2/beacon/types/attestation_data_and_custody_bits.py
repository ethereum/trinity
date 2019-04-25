import ssz
from ssz.sedes import (
    boolean,
)
from eth_typing import (
    Hash32,
)

from .attestation_data import (
    AttestationData,
)


class AttestationDataAndCustodyBit(ssz.Serializable):

    fields = [
        # Attestation data
        ('data', AttestationData),
        # Custody bit
        ('custody_bit', boolean),
    ]

    def __init__(self,
                 data: AttestationData,
                 custody_bit: bool)-> None:

        super().__init__(
            data=data,
            custody_bit=custody_bit,
        )

    _root = None

    @property
    def root(self) -> Hash32:
        return super().root
