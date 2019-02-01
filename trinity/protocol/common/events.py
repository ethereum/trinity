from typing import (
    Type,
)

from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
)


class ConnectToNodeCommand(BaseEvent):

    def __init__(self, node: str) -> None:
        self.node = node


class PeerCountResponse(BaseEvent):

    def __init__(self, peer_count: int) -> None:
        self.peer_count = peer_count


class PeerCountRequest(BaseRequestResponseEvent[PeerCountResponse]):

    @staticmethod
    def expected_response_type() -> Type[PeerCountResponse]:
        return PeerCountResponse
