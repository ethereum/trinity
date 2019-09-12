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

from eth2.beacon.typing import Slot


@dataclass
class BeaconSlotResponse(BaseEvent):
    """
    beacon_slot: Latest slot of the beacon chain state
    """
    result: Slot


class BeaconSlotRequest(BaseRequestResponseEvent[BeaconSlotResponse]):
    @staticmethod
    def expected_response_type() -> Type[BeaconSlotResponse]:
        return BeaconSlotResponse


@dataclass
class Libp2pPeersResponse(BaseEvent):
    """
    libp2p_peers: Tracks number of libp2p peers
    """
    result: int


class Libp2pPeersRequest(BaseRequestResponseEvent[Libp2pPeersResponse]):
    @staticmethod
    def expected_response_type() -> Type[Libp2pPeersResponse]:
        return Libp2pPeersResponse
