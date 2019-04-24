from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    NewType,
)

from eth2.beacon.db.chain import BaseBeaconChainDB
from eth2.beacon.types.blocks import BaseBeaconBlock
from eth2.beacon.types.states import BeaconState

from .store import Store


# AttestationPool is a stub type to be replaced once the proper abstraction is clearer
AttestationPool = NewType("AttestationPool", BaseBeaconChainDB)

class BaseForkChoice(ABC):
    @abstractmethod
    def run(store: Store, block: BaseBeaconBlock) -> BeaconBlock:
        """
        Execute fork
        """
        pass
