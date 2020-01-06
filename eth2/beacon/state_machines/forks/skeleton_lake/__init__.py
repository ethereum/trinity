from eth2.beacon.fork_choice.higher_slot import HigherSlotScoring
from eth2.beacon.state_machines.base import BeaconStateMachine
from eth2.beacon.state_machines.forks.serenity.blocks import (
    SerenityBeaconBlock,
    SerenitySignedBeaconBlock,
)
from eth2.beacon.state_machines.forks.serenity.state_transitions import (
    SerenityStateTransition,
)
from eth2.beacon.state_machines.forks.serenity.states import SerenityBeaconState

from .config import MINIMAL_SERENITY_CONFIG


class SkeletonLakeStateMachine(BeaconStateMachine):
    fork = "skeleton_lake"
    config = MINIMAL_SERENITY_CONFIG

    # classes
    signed_block_class = SerenitySignedBeaconBlock
    block_class = SerenityBeaconBlock
    state_class = SerenityBeaconState
    state_transition_class = SerenityStateTransition
    fork_choice_scoring_class = HigherSlotScoring

    def get_fork_choice_scoring(self) -> HigherSlotScoring:
        return self.fork_choice_scoring_class()
