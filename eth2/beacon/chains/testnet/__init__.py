from eth2.beacon.chains.base import BeaconChain
from eth2.beacon.state_machines.forks.skeleton_lake import SkeletonLakeStateMachine


class SkeletonLakeChain(BeaconChain):
    sm_configuration = (
        (SkeletonLakeStateMachine.config.GENESIS_SLOT, SkeletonLakeStateMachine),
    )


class MinimalChain(SkeletonLakeChain):
    """
    ``MinimalChain`` is an alias for the ``BeaconChain`` using the
    "minimal" configuration profile.
    """

    pass
