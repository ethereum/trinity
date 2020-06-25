from abc import ABC, abstractmethod
import logging
from typing import Tuple, Type

from eth._utils.datatypes import Configurable

from eth2.beacon.db.chain import BaseBeaconChainDB
from eth2.beacon.fork_choice.scoring import BaseForkChoiceScoring
from eth2.beacon.types.blocks import BaseBeaconBlock, BaseSignedBeaconBlock
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Slot
from eth2.configs import Eth2Config

logger = logging.getLogger("trinity.beacon.state_machines")
logger.setLevel(logging.DEBUG)


class BaseBeaconStateMachine(Configurable, ABC):
    # https://gitlab.com/pycqa/flake8/issues/394
    fork: str = None  # noqa: E701
    config: Eth2Config = None

    block_class: Type[BaseBeaconBlock] = None
    signed_block_class: Type[BaseSignedBeaconBlock] = None
    state_class: Type[BeaconState] = None

    @abstractmethod
    def __init__(self, chain_db: BaseBeaconChainDB) -> None:
        """
        NOTE: assumes ``chain_db`` has been initialized with genesis data
        """
        ...

    @abstractmethod
    def get_fork_choice_scoring(self) -> BaseForkChoiceScoring:
        ...

    @abstractmethod
    def apply_state_transition(
        self,
        state: BeaconState,
        signed_block: BaseSignedBeaconBlock = None,
        future_slot: Slot = None,
        check_proposer_signature: bool = True,
    ) -> Tuple[BeaconState, BaseSignedBeaconBlock]:
        ...
