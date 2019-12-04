"""
A blockchain has a way to pick a canonical chain from the block tree called a fork choice.
A fork choice works by using a scoring rule to attach a scalar quantity to a particular block
given our local view of the network.

The fork choice provides a "canonical" path through the block tree by recursively selecting the
highest scoring child of a given block until we terminate at the tip of the chain.

This module provides a variety of fork choice rules. Clients can introduce new rules here to
experiment with alternative fork choice methods.
"""

from abc import ABC, abstractmethod
from typing import Type

from eth2._utils.typing import Comparable, Serializeable
from eth2.beacon.types.blocks import BaseBeaconBlock
from eth2.beacon.types.states import BeaconState


class BaseScore(ABC, Comparable, Serializeable):
    @classmethod
    @abstractmethod
    def from_genesis(
        cls, genesis_state: BeaconState, genesis_block: BaseBeaconBlock
    ) -> "BaseScore":
        ...


class BaseForkChoiceScoring(ABC):
    @classmethod
    @abstractmethod
    def get_score_class(cls) -> Type[BaseScore]:
        ...

    @abstractmethod
    def score(self, block: BaseBeaconBlock) -> BaseScore:
        ...
