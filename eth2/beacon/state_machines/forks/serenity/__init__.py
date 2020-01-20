from typing import Type  # noqa: F401

from eth2.beacon.db.chain import BaseBeaconChainDB
from eth2.beacon.db.exceptions import MissingForkChoiceContext
from eth2.beacon.fork_choice.lmd_ghost import Context as LMDGHOSTContext
from eth2.beacon.fork_choice.lmd_ghost import LMDGHOSTScoring, Store
from eth2.beacon.fork_choice.scoring import BaseForkChoiceScoring
from eth2.beacon.state_machines.base import BeaconStateMachine
from eth2.beacon.state_machines.state_transitions import (  # noqa: F401
    BaseStateTransition,
)
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.blocks import BaseBeaconBlock  # noqa: F401
from eth2.beacon.types.states import BeaconState  # noqa: F401
from eth2.beacon.typing import Timestamp

from .blocks import SerenityBeaconBlock, SerenitySignedBeaconBlock
from .configs import SERENITY_CONFIG
from .state_transitions import SerenityStateTransition
from .states import SerenityBeaconState


class SerenityStateMachine(BeaconStateMachine):
    # fork name
    fork = "serenity"  # type: str
    config = SERENITY_CONFIG

    # classes
    block_class = SerenityBeaconBlock  # type: Type[BaseBeaconBlock]
    signed_block_class = SerenitySignedBeaconBlock
    state_class = SerenityBeaconState  # type: Type[BeaconState]
    state_transition_class = SerenityStateTransition  # type: Type[BaseStateTransition]
    fork_choice_scoring_class = LMDGHOSTScoring  # type: Type[BaseForkChoiceScoring]

    def __init__(
        self, chaindb: BaseBeaconChainDB, fork_choice_context: LMDGHOSTContext = None
    ) -> None:
        super().__init__(chaindb)
        self._fork_choice_store = Store(
            chaindb,
            self.block_class,
            self.config,
            fork_choice_context or self._get_fork_choice_context(),
        )
        self.fork_choice_scoring = LMDGHOSTScoring(self._fork_choice_store)

    def _get_fork_choice_context(self) -> LMDGHOSTContext:
        try:
            fork_choice_context_data = self.chaindb.get_fork_choice_context_data_for(
                self.fork
            )
        except MissingForkChoiceContext:
            # NOTE: ``MissingForkChoiceContext`` here implies that we are missing the
            # fork choice context for this fork, which happens to be the genesis fork.
            # In this situation (possibly uniquely), we want to build a new
            # fork choice context from the genesis data.
            genesis_root = self.chaindb.get_genesis_block_root()
            genesis_block = self.chaindb.get_block_by_root(
                genesis_root, self.block_class
            )
            genesis_state = self.chaindb.get_state_by_root(
                genesis_block.message.state_root, self.state_class
            )
            return LMDGHOSTContext.from_genesis(genesis_state, genesis_block)
        else:
            return LMDGHOSTContext.from_bytes(fork_choice_context_data)

    # methods
    def _get_justified_head_state(self) -> BeaconState:
        justified_head = self.chaindb.get_justified_head(self.block_class)
        return self.chaindb.get_state_by_root(
            justified_head.state_root, self.state_class
        )

    def get_fork_choice_scoring(self) -> BaseForkChoiceScoring:
        return self.fork_choice_scoring

    def on_tick(self, time: Timestamp) -> None:
        self._fork_choice_store.on_tick(time)

    def on_block(self, block: BaseBeaconBlock) -> None:
        self._fork_choice_store.on_block(block)

    def on_attestation(self, attestation: Attestation) -> None:
        self._fork_choice_store.on_attestation(attestation)
