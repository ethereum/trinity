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
