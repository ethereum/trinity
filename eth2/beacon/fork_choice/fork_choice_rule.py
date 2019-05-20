from collections.abc import ABC, abstractmethod

class BaseForkChoiceRule(ABC):
    @abstractmethod
    def score_block(self, block: BaseBeaconBlock, chaindb: BaseChainDB):
        pass
