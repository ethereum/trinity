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
