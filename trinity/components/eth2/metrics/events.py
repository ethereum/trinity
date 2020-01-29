from dataclasses import (
    dataclass,
)
from typing import (
    Tuple,
    Type,
)

from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
)

from libp2p.peer.id import ID


@dataclass
class Libp2pPeersResponse(BaseEvent):
    """
    libp2p_peers: Handshaked Peer IDs.
    """
    result: Tuple[ID, ...]


class Libp2pPeersRequest(BaseRequestResponseEvent[Libp2pPeersResponse]):
    @staticmethod
    def expected_response_type() -> Type[Libp2pPeersResponse]:
        return Libp2pPeersResponse
