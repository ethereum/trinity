from typing import Any, Type

import ssz

from eth2.beacon.fork_choice.scoring import BaseForkChoiceScoring, BaseScore
from eth2.beacon.types.blocks import BaseBeaconBlock
from eth2.beacon.types.states import BeaconState


def _score_block_by_higher_slot(block: BaseBeaconBlock) -> int:
    return block.slot


class HigherSlotScore(BaseScore):
    _score: int

    def __init__(self, score: int) -> None:
        self._score = score

    def __lt__(self, other: "HigherSlotScore") -> bool:
        return self._score < other._score

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, HigherSlotScore):
            return NotImplemented
        return self._score == other._score

    def serialize(self) -> bytes:
        return ssz.encode(self._score, sedes=ssz.sedes.uint64)

    @classmethod
    def deserialize(cls, data: bytes) -> "HigherSlotScore":
        score = ssz.decode(data, sedes=ssz.sedes.uint64)
        return cls(score)

    @classmethod
    def from_genesis(
        cls, genesis_state: BeaconState, genesis_block: BaseBeaconBlock
    ) -> BaseScore:
        score = _score_block_by_higher_slot(genesis_block)
        return cls(score)


class HigherSlotScoring(BaseForkChoiceScoring):
    @classmethod
    def get_score_class(cls) -> Type[BaseScore]:
        return HigherSlotScore

    def score(self, block: BaseBeaconBlock) -> BaseScore:
        """
        A ``block`` with a higher slot has a higher score.
        """
        score = _score_block_by_higher_slot(block)
        return HigherSlotScore(score)
