# from abc import ABC, abstractmethod
from typing import Callable

from eth2.beacon.types.blocks import BaseBeaconBlock
from eth2.beacon.db.chain import BaseBeaconChainDB

BaseForkChoiceRule = Callable[[BaseBeaconBlock, BaseBeaconChainDB], int]

# class BaseForkChoiceRule(ABC):
#     @abstractmethod
#     def score_block(self, block: BaseBeaconBlock, chaindb: BaseBeaconChainDB) -> int:
#         pass
