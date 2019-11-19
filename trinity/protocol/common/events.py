from dataclasses import (
    dataclass,
)
from typing import (
    Any,
    Tuple,
    Type,
)

from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
)

from p2p.abc import CommandAPI, NodeAPI, SessionAPI
from p2p.disconnect import DisconnectReason


@dataclass
class ConnectToNodeCommand(BaseEvent):
    """
    Event that wraps a node URI that the pool should connect to.
    """
    remote: NodeAPI


@dataclass
class PeerCountResponse(BaseEvent):
    """
    Response event that wraps the count of peers connected to the pool.
    """

    peer_count: int


class PeerCountRequest(BaseRequestResponseEvent[PeerCountResponse]):
    """
    Request event to get the count of peers connected to the pool.
    """

    @staticmethod
    def expected_response_type() -> Type[PeerCountResponse]:
        return PeerCountResponse


@dataclass
class DisconnectPeerEvent(BaseEvent):
    """
    Event broadcasted when we want to disconnect from a peer
    """
    session: SessionAPI
    reason: DisconnectReason


@dataclass
class PeerJoinedEvent(BaseEvent):
    """
    Event broadcasted when a new peer joined the pool.
    """
    session: SessionAPI


@dataclass
class PeerLeftEvent(BaseEvent):
    """
    Event broadcasted when a peer left the pool.
    """
    session: SessionAPI


@dataclass
class GetConnectedPeersResponse(BaseEvent):

    sessions: Tuple[SessionAPI, ...]


class GetConnectedPeersRequest(BaseRequestResponseEvent[GetConnectedPeersResponse]):

    @staticmethod
    def expected_response_type() -> Type[GetConnectedPeersResponse]:
        return GetConnectedPeersResponse


@dataclass
class PeerPoolMessageEvent(BaseEvent):
    """
    Base event for all peer messages that are relayed on the event bus. The events are mapped
    to individual subclasses for every different ``cmd`` to allow efficient consumption through
    the event bus.
    """
    session: SessionAPI
    command: CommandAPI[Any]
