from dataclasses import (
    dataclass,
)
from typing import Tuple, Type

from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
)

from p2p.abc import NodeAPI
from p2p.discv5.typing import NodeID


class BaseConnectionTrackerEvent(BaseEvent):
    pass


@dataclass
class BlacklistEvent(BaseConnectionTrackerEvent):

    remote: NodeAPI
    timeout_seconds: int
    reason: str


@dataclass
class GetBlacklistedPeersResponse(BaseConnectionTrackerEvent):

    peers: Tuple[NodeID, ...]


@dataclass
class GetBlacklistedPeersRequest(BaseRequestResponseEvent[GetBlacklistedPeersResponse]):

    @staticmethod
    def expected_response_type() -> Type[GetBlacklistedPeersResponse]:
        return GetBlacklistedPeersResponse
