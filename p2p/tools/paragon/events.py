from lahja import (
    BaseEvent,
)
from p2p.peer import (
    IdentifiablePeer,
)


class GetSumRequest(BaseEvent):

    def __init__(self, dto_peer: IdentifiablePeer, a: int, b: int) -> None:
        self.dto_peer = dto_peer
        self.a = a
        self.b = b
