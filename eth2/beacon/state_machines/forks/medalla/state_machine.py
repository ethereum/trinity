from typing import Tuple, Type

from eth2.beacon.fork_choice.abc import BaseForkChoice
from eth2.beacon.fork_choice.lmd_ghost2 import LMDGHOSTForkChoice
from eth2.beacon.state_machines.abc import BaseBeaconStateMachine
from eth2.beacon.state_machines.forks.medalla.configs import MEDALLA_CONFIG
from eth2.beacon.state_machines.forks.medalla.eth2fastspec import EpochsContext
from eth2.beacon.state_machines.forks.medalla.fast_state_transition import (
    apply_fast_state_transition,
)
from eth2.beacon.state_machines.forks.serenity.state_transitions import (
    apply_state_transition,
)
from eth2.beacon.state_machines.forks.skeleton_lake import MINIMAL_SERENITY_CONFIG
from eth2.beacon.types.blocks import (
    BaseBeaconBlock,
    BaseSignedBeaconBlock,
    BeaconBlock,
    SignedBeaconBlock,
)
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Slot


class MedallaStateMachine(BaseBeaconStateMachine):
    config = MEDALLA_CONFIG
    block_class: Type[BaseBeaconBlock] = BeaconBlock
    signed_block_class: Type[BaseSignedBeaconBlock] = SignedBeaconBlock
    state_class: Type[BeaconState] = BeaconState
    fork_choice_class: Type[BaseForkChoice] = LMDGHOSTForkChoice

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


class MedallaStateMachineFast(MedallaStateMachine):
    _epochs_ctx: EpochsContext = None

    def apply_state_transition(
        self,
        state: BeaconState,
        signed_block: BaseSignedBeaconBlock = None,
        future_slot: Slot = None,
        check_proposer_signature: bool = True,
    ) -> Tuple[BeaconState, BaseSignedBeaconBlock]:
        if not self._epochs_ctx:
            self._epochs_ctx = EpochsContext(self.config)
            self._epochs_ctx.load_state(state)

        if self._epochs_ctx.current_shuffling.epoch != state.current_epoch(
            self.config.SLOTS_PER_EPOCH
        ):
            self._epochs_ctx.load_state(state)

        state = apply_fast_state_transition(
            self._epochs_ctx,
            self.config,
            state,
            signed_block,
            future_slot,
            check_proposer_signature,
        )

        if signed_block:
            signed_block = signed_block.transform(
                ("message", "state_root"), state.hash_tree_root
            )

        return state, signed_block


class MedallaStateMachineTest(MedallaStateMachine):
    config = MINIMAL_SERENITY_CONFIG
