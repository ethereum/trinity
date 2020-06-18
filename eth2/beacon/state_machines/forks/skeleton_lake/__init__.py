from eth2.beacon.db.chain import BaseBeaconChainDB
from eth2.beacon.fork_choice.higher_slot import HigherSlotScoring
from eth2.beacon.fork_choice.scoring import BaseForkChoiceScoring
from eth2.beacon.state_machines.forks.serenity import SerenityStateMachine

from .configs import MINIMAL_SERENITY_CONFIG


class SkeletonLakeStateMachine(SerenityStateMachine):
    fork = "skeleton_lake"
    config = MINIMAL_SERENITY_CONFIG

    def __init__(self, chain_db: BaseBeaconChainDB) -> None:
        self.chain_db = chain_db

    def get_fork_choice_scoring(self) -> BaseForkChoiceScoring:
        return HigherSlotScoring()
