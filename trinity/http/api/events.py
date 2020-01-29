from dataclasses import (
    dataclass,
)
from typing import (
    Type,
)

from eth_typing import BLSSignature
from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
)

from eth2.beacon.types.blocks import BaseBeaconBlock
from eth2.beacon.typing import Slot
from libp2p.peer.id import ID


@dataclass
class Libp2pPeerIDResponse(BaseEvent):
    """
    libp2p_peers: The peer ID of the beacon node.
    """
    result: ID


class Libp2pPeerIDRequest(BaseRequestResponseEvent[Libp2pPeerIDResponse]):
    @staticmethod
    def expected_response_type() -> Type[Libp2pPeerIDResponse]:
        return Libp2pPeerIDResponse


@dataclass
class GetBeaconBlockResponse(BaseEvent):
    result: BaseBeaconBlock


@dataclass
class GetBeaconBlockRequest(BaseRequestResponseEvent[GetBeaconBlockResponse]):
    slot: Slot
    randao_reveal: BLSSignature

    @staticmethod
    def expected_response_type() -> Type[GetBeaconBlockResponse]:
        return GetBeaconBlockResponse
