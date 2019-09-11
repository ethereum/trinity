from dataclasses import (
    dataclass,
)
from typing import (
    Type,
)

from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
)

from eth2.beacon.typing import Epoch, Slot, SigningRoot
#
# Fork choice rule
#
@dataclass
class HeadSlotResponse(BaseEvent):
    """
    beacon_head_slot: Slot of the head block of the beacon chain
    """
    slot: Slot


class HeadSlotRequest(BaseRequestResponseEvent[HeadSlotResponse]):
    @staticmethod
    def expected_response_type() -> Type[HeadSlotResponse]:
        return HeadSlotResponse


@dataclass
class HeadRootResponse(BaseEvent):
    """
    beacon_head_root: Root of the head block of the beacon chain
    """
    root: SigningRoot


class HeadRootRequest(BaseRequestResponseEvent[HeadRootResponse]):
    @staticmethod
    def expected_response_type() -> Type[HeadRootResponse]:
        return HeadRootResponse


#
# Epoch transition
#
@dataclass
class FinalizedEpochResponse(BaseEvent):
    """
    beacon_finalized_epoch: Current finalized epoch
    """
    epoch: Epoch


class FinalizedEpochRequest(BaseRequestResponseEvent[FinalizedEpochResponse]):
    @staticmethod
    def expected_response_type() -> Type[FinalizedEpochResponse]:
        return FinalizedEpochResponse


@dataclass
class FinalizedRootResponse(BaseEvent):
    """
    beacon_current_justified_root: Current justified root
    """
    root: SigningRoot


class FinalizedRootRequest(BaseRequestResponseEvent[FinalizedRootResponse]):
    @staticmethod
    def expected_response_type() -> Type[FinalizedRootResponse]:
        return FinalizedRootResponse
