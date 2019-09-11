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

from eth2.beacon.typing import Epoch, HashTreeRoot, Slot, SigningRoot


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

# PreviousJustified
@dataclass
class PreviousJustifiedEpochResponse(BaseEvent):
    """
    beacon_finalized_epoch: Current finalized epoch
    """
    epoch: Epoch


class PreviousJustifiedEpochRequest(BaseRequestResponseEvent[PreviousJustifiedEpochResponse]):
    @staticmethod
    def expected_response_type() -> Type[PreviousJustifiedEpochResponse]:
        return PreviousJustifiedEpochResponse


@dataclass
class PreviousJustifizedRootResponse(BaseEvent):
    """
    beacon_current_justified_root: Current justified root
    """
    root: HashTreeRoot


class PreviousJustifizedRootRequest(BaseRequestResponseEvent[PreviousJustifizedRootResponse]):
    @staticmethod
    def expected_response_type() -> Type[PreviousJustifizedRootResponse]:
        return PreviousJustifizedRootResponse


# CurrentJustified
@dataclass
class CurrentJustifiedEpochResponse(BaseEvent):
    """
    beacon_finalized_epoch: Current finalized epoch
    """
    epoch: Epoch


class CurrentJustifiedEpochRequest(BaseRequestResponseEvent[CurrentJustifiedEpochResponse]):
    @staticmethod
    def expected_response_type() -> Type[CurrentJustifiedEpochResponse]:
        return CurrentJustifiedEpochResponse


@dataclass
class CurrentJustifiedRootResponse(BaseEvent):
    """
    beacon_current_justified_root: Current justified root
    """
    root: HashTreeRoot


class CurrentJustifiedRootRequest(BaseRequestResponseEvent[CurrentJustifiedRootResponse]):
    @staticmethod
    def expected_response_type() -> Type[CurrentJustifiedRootResponse]:
        return CurrentJustifiedRootResponse


# Finalized
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
    root: HashTreeRoot


class FinalizedRootRequest(BaseRequestResponseEvent[FinalizedRootResponse]):
    @staticmethod
    def expected_response_type() -> Type[FinalizedRootResponse]:
        return FinalizedRootResponse
