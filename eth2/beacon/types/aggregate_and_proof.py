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
        ("aggregator_index", uint64),
        ("aggregate", Attestation),
        ("selection_proof", bytes96),
    ]

    @classmethod
    def create(
        cls: Type[TAggregateAndProof],
        aggregator_index: ValidatorIndex = default_validator_index,
        aggregate: Attestation = default_attestation,
        selection_proof: BLSSignature = EMPTY_SIGNATURE,
    ) -> TAggregateAndProof:
        return super().create(
            aggregator_index=aggregator_index,
            aggregate=aggregate,
            selection_proof=selection_proof,
        )

    def __str__(self) -> str:
        return (
            f"aggregator_index={self.aggregator_index},"
            f" aggregate={self.aggregate},"
            f" selection_proof={humanize_hash(self.selection_proof)},"
        )


default_aggregate_and_proof = AggregateAndProof.create()
