from abc import ABC, abstractmethod
import logging
from typing import Tuple, Type

from eth2.beacon.fork_choice.abc import BaseForkChoice
from eth2.beacon.types.blocks import BaseBeaconBlock, BaseSignedBeaconBlock
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Slot
from eth2.configs import Eth2Config

logger = logging.getLogger("trinity.beacon.state_machines")


class BaseBeaconStateMachine(ABC):
    config: Eth2Config = None

    block_class: Type[BaseBeaconBlock] = None
    signed_block_class: Type[BaseSignedBeaconBlock] = None
    state_class: Type[BeaconState] = None
    fork_choice_class: Type[BaseForkChoice]

    @abstractmethod
    def apply_state_transition(
        self,
        state: BeaconState,
        signed_block: BaseSignedBeaconBlock = None,
        future_slot: Slot = None,
        check_proposer_signature: bool = True,
    ) -> Tuple[BeaconState, BaseSignedBeaconBlock]:
        ...
