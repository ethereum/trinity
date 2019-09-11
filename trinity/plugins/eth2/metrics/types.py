from typing import (
    NamedTuple,
)

from eth2.beacon.types.checkpoints import Checkpoint
from eth2.beacon.typing import (
    Slot,
    SigningRoot,
)


class OnSlotInfo(NamedTuple):
    """
    On slot
    beacon_slot: Latest slot of the beacon chain state
    """
    slot: Slot


class OnBlockInfo(NamedTuple):
    """
    On block transition
    beacon_head_slot: Slot of the head block of the beacon chain
    beacon_head_root: Root of the head block of the beacon chain
    """
    slot: Slot
    root: SigningRoot


class OnEpochInfo(NamedTuple):
    """
    On epoch transition
    beacon_finalized_epoch: Current finalized epoch
    beacon_finalized_root: Current finalized root
    """
    previous_justified_checkpoint: Checkpoint
    current_justified_checkpoint: Checkpoint
    finalized_checkpoint: Checkpoint
