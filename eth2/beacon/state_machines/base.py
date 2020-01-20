from abc import ABC, abstractmethod
import logging
from typing import Tuple, Type

from eth._utils.datatypes import Configurable

from eth2.beacon.db.chain import BaseBeaconChainDB
from eth2.beacon.fork_choice.scoring import BaseForkChoiceScoring
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.blocks import BaseBeaconBlock, BaseSignedBeaconBlock
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Timestamp
from eth2.configs import Eth2Config

from .state_transitions import BaseStateTransition

logger = logging.getLogger("trinity.beacon.state_machines")
logger.setLevel(logging.DEBUG)


class BaseBeaconStateMachine(Configurable, ABC):
    # https://gitlab.com/pycqa/flake8/issues/394
    fork: str = None  # noqa: E701
    chaindb: BaseBeaconChainDB = None
    config: Eth2Config = None

    block_class: Type[BaseBeaconBlock] = None
    signed_block_class: Type[BaseSignedBeaconBlock] = None
    state_class: Type[BeaconState] = None
    state_transition_class: Type[BaseStateTransition] = None
    fork_choice_scoring_class: Type[BaseForkChoiceScoring] = None
    fork_choice_scoring: BaseForkChoiceScoring = None

    @abstractmethod
    def __init__(self, chaindb: BaseBeaconChainDB) -> None:
        ...

    @classmethod
    @abstractmethod
    def get_block_class(cls) -> Type[BaseBeaconBlock]:
        ...

    @classmethod
    @abstractmethod
    def get_state_class(cls) -> Type[BeaconState]:
        ...

    @classmethod
    @abstractmethod
    def get_state_transiton_class(cls) -> Type[BaseStateTransition]:
        ...

    @property
    @abstractmethod
    def state_transition(self) -> BaseStateTransition:
        ...

    @classmethod
    @abstractmethod
    def get_fork_choice_scoring_class(cls) -> Type[BaseForkChoiceScoring]:
        ...

    @abstractmethod
    def get_fork_choice_scoring(self) -> BaseForkChoiceScoring:
        ...

    @abstractmethod
    def on_tick(self, time: Timestamp) -> None:
        ...

    @abstractmethod
    def on_block(self, block: BaseBeaconBlock) -> None:
        ...

    @abstractmethod
    def on_attestation(self, attestation: Attestation) -> None:
        ...

    #
    # Import block API
    #
    @abstractmethod
    def import_block(
        self,
        signed_block: BaseSignedBeaconBlock,
        state: BeaconState,
        check_proposer_signature: bool = True,
    ) -> Tuple[BeaconState, BaseSignedBeaconBlock]:
        ...


class BeaconStateMachine(BaseBeaconStateMachine):
    def __init__(self, chaindb: BaseBeaconChainDB) -> None:
        self.chaindb = chaindb

    @classmethod
    def get_block_class(cls) -> Type[BaseBeaconBlock]:
        """
        Return the :class:`~eth2.beacon.types.blocks.BeaconBlock` class that this
        StateMachine uses for blocks.
        """
        if cls.block_class is None:
            raise AttributeError("No `block_class` has been set for this StateMachine")
        else:
            return cls.block_class

    @classmethod
    def get_state_class(cls) -> Type[BeaconState]:
        """
        Return the :class:`~eth2.beacon.types.states.BeaconState` class that this
        StateMachine uses for BeaconState.
        """
        if cls.state_class is None:
            raise AttributeError("No `state_class` has been set for this StateMachine")
        else:
            return cls.state_class

    @classmethod
    def get_state_transiton_class(cls) -> Type[BaseStateTransition]:
        """
        Return the :class:`~eth2.beacon.state_machines.state_transitions.BaseStateTransition`
        class that this StateTransition uses for StateTransition.
        """
        if cls.state_transition_class is None:
            raise AttributeError(
                "No `state_transition_class` has been set for this StateMachine"
            )
        else:
            return cls.state_transition_class

    @property
    def state_transition(self) -> BaseStateTransition:
        return self.get_state_transiton_class()(self.config)

    @classmethod
    def get_fork_choice_scoring_class(cls) -> Type[BaseForkChoiceScoring]:
        return cls.fork_choice_scoring_class

    def on_tick(self, time: Timestamp) -> None:
        pass

    def on_block(self, block: BaseBeaconBlock) -> None:
        pass

    def on_attestation(self, attestation: Attestation) -> None:
        pass

    #
    # Import block API
    #
    def import_block(
        self,
        signed_block: BaseSignedBeaconBlock,
        state: BeaconState,
        check_proposer_signature: bool = True,
    ) -> Tuple[BeaconState, BaseSignedBeaconBlock]:
        state = self.state_transition.apply_state_transition(
            state,
            signed_block=signed_block,
            check_proposer_signature=check_proposer_signature,
        )

        signed_block = signed_block.transform(
            ("message", "state_root"), state.hash_tree_root
        )

        return state, signed_block
