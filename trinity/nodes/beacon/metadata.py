from typing import Tuple, Type, TypeVar

from ssz.hashable_container import HashableContainer
from ssz.sedes import Bitvector, uint64

ATTESTATION_SUBNET_COUNT = 64

TMetaData = TypeVar("TMetaData", bound="MetaData")

default_attnets_tuple = (False,) * ATTESTATION_SUBNET_COUNT


class SeqNumber(int):
    pass


default_seq_number = SeqNumber(0)


class MetaData(HashableContainer):
    fields = [("seq_number", uint64), ("attnets", Bitvector(ATTESTATION_SUBNET_COUNT))]

    @classmethod
    def create(
        cls: Type[TMetaData],
        *,
        seq_number: SeqNumber = default_seq_number,
        attnets: Tuple[bool, ...] = default_attnets_tuple,
    ) -> TMetaData:
        return super().create(seq_number=seq_number, attnets=attnets)

    def __str__(self) -> str:
        attnets = map(lambda elem: "1" if elem else "0", self.attnets)
        return f"seq_number={self.seq_number}, attnets=0b{''.join(attnets)}"
