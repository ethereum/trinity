from typing import Tuple, Type  # noqa: F401

from eth2.beacon.db.chain import BaseBeaconChainDB, MissingForkChoiceContext
from eth2.beacon.fork_choice.lmd_ghost import Context as LMDGHOSTContext
from eth2.beacon.fork_choice.lmd_ghost import LMDGHOSTScoring, Store
from eth2.beacon.fork_choice.scoring import BaseForkChoiceScoring
from eth2.beacon.state_machines.base import BaseBeaconStateMachine
from eth2.beacon.state_machines.forks.serenity.state_transitions import (
    apply_state_transition,
)
from eth2.beacon.types.blocks import BaseBeaconBlock  # noqa: F401
from eth2.beacon.types.blocks import BaseSignedBeaconBlock
from eth2.beacon.types.states import BeaconState  # noqa: F401
from eth2.beacon.typing import Slot

from .blocks import SerenityBeaconBlock, SerenitySignedBeaconBlock
from .configs import SERENITY_CONFIG
from .states import SerenityBeaconState


class SerenityStateMachine(BaseBeaconStateMachine):
    # fork name
    fork = "serenity"  # type: str
    config = SERENITY_CONFIG

    # classes
    block_class = SerenityBeaconBlock  # type: Type[BaseBeaconBlock]
    signed_block_class = SerenitySignedBeaconBlock
    state_class = SerenityBeaconState  # type: Type[BeaconState]

    def __init__(self, chain_db: BaseBeaconChainDB) -> None:
        self.chain_db = chain_db
        self._fork_choice_store = Store(
            chain_db, self.block_class, self.config, self._get_fork_choice_context()
        )
        self.fork_choice_scoring = LMDGHOSTScoring(self._fork_choice_store)

    def get_fork_choice_scoring(self) -> BaseForkChoiceScoring:
        return self.fork_choice_scoring

    def _get_fork_choice_context(self) -> LMDGHOSTContext:
        try:
            fork_choice_context_data = self.chain_db.get_fork_choice_context_data_for(
                self.fork
            )
        except MissingForkChoiceContext:
            # NOTE: ``MissingForkChoiceContext`` here implies that we are missing the
            # fork choice context for this fork, which happens to be the genesis fork.
            # In this situation (possibly uniquely), we want to build a new
            # fork choice context from the genesis data.
            genesis_root = self.chain_db.get_genesis_block_root()
            genesis_block = self.chain_db.get_block_by_root(
                genesis_root, self.block_class
            )
            genesis_state = self.chain_db.get_state_by_root(
                genesis_block.message.state_root, self.state_class
            )
            return LMDGHOSTContext.from_genesis(genesis_state, genesis_block)
        else:
            return LMDGHOSTContext.from_bytes(fork_choice_context_data)

    def _get_justified_head_state(self) -> BeaconState:
        justified_head = self.chain_db.get_justified_head(self.block_class)
        return self.chain_db.get_state_by_root(
            justified_head.state_root, self.state_class
        )

    def apply_state_transition(
        self,
        state: BeaconState,
        signed_block: BaseSignedBeaconBlock = None,
        future_slot: Slot = None,
        check_proposer_signature: bool = True,
    ) -> Tuple[BeaconState, BaseSignedBeaconBlock]:
        state = apply_state_transition(
            self.config, state, signed_block, future_slot, check_proposer_signature
        )

        if signed_block:
            signed_block = signed_block.transform(
                ("message", "state_root"), state.hash_tree_root
            )

        return state, signed_block
