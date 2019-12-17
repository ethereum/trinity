from typing import Type

from eth2.beacon.fork_choice.scoring import BaseForkChoiceScoring, BaseScore
from eth2.beacon.types.blocks import BaseBeaconBlock
from eth2.beacon.types.states import BeaconState


class ConstantScoring(BaseForkChoiceScoring):
    """
    ``ConstantScoring`` is a class used to satisfy the API of
    ``score`` with an arbitrary value of ``BaseScore`` provided during
    instantiation of the ``ConstantScoring``.
    """

    def __init__(self, score: BaseScore) -> None:
        self._score = score

    @classmethod
    def get_score_class(cls) -> Type[BaseScore]:
        raise NotImplementedError

    def score(self, block: BaseBeaconBlock) -> BaseScore:
        return self._score

    @classmethod
    def from_genesis(
        cls, genesis_state: BeaconState, genesis_block: BaseBeaconBlock
    ) -> BaseForkChoiceScoring:
        raise NotImplementedError
