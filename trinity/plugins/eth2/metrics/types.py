from typing import (
    NamedTuple,
)

from eth_typing import (
    BlockNumber,
)

from eth2.beacon.types.checkpoints import Checkpoint
from eth2.beacon.typing import (
    Epoch,
    HashTreeRoot,
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
    # finalized_epoch: Epoch
    # finalized_root: HashTreeRoot
    # current_justified_epoch: Epoch
    # current_justified_root: HashTreeRoot
    # previous_justified_epoch: Epoch
    # previous_justified_root: HashTreeRoot
    finalized_checkpoint: Checkpoint
    current_justified_checkpoint: Checkpoint
    previous_justified_checkpoint: Checkpoint
