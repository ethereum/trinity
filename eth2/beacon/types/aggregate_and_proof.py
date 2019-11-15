from typing import Type, TypeVar

from eth_typing import BLSSignature
from eth_utils import humanize_hash
from ssz.hashable_container import HashableContainer
from ssz.sedes import bytes96, uint64

from eth2.beacon.constants import EMPTY_SIGNATURE
from eth2.beacon.types.attestations import Attestation, default_attestation
from eth2.beacon.types.defaults import default_validator_index
from eth2.beacon.typing import ValidatorIndex

TAggregateAndProof = TypeVar("TAggregateAndProof", bound="AggregateAndProof")


class AggregateAndProof(HashableContainer):

    fields = [
        ("index", uint64),
        ("selection_proof", bytes96),
        ("aggregate", Attestation),
    ]

    @classmethod
    def create(
        cls: Type[TAggregateAndProof],
        index: ValidatorIndex = default_validator_index,
        selection_proof: BLSSignature = EMPTY_SIGNATURE,
        aggregate: Attestation = default_attestation,
    ) -> TAggregateAndProof:
        return super().create(
            index=index, selection_proof=selection_proof, aggregate=aggregate
        )

    def __str__(self) -> str:
        return (
            f"index={self.index},"
            f" selection_proof={humanize_hash(self.selection_proof)},"
            f" aggregate={self.aggregate},"
        )


default_aggregate_and_proof = AggregateAndProof.create()
