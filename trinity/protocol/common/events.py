from dataclasses import (
    dataclass,
)
from typing import (
    Any,
    Tuple,
    Type,
    NamedTuple,
    Dict,
)

from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
)

from p2p.abc import CommandAPI, NodeAPI, SessionAPI
from p2p.disconnect import DisconnectReason
from p2p.peer import BasePeer
from p2p.typing import Capabilities


class PeerInfo(NamedTuple):
    session: SessionAPI
    capabilities: Capabilities
    client_version_string: str
    inbound: bool


@dataclass
class ConnectToNodeCommand(BaseEvent):
    """
    Event that wraps a node URI that the pool should connect to.
    """
    remote: NodeAPI


@dataclass
class DisconnectFromPeerCommand(BaseEvent):
    """
    Command to wrap a node session that the pool should disconnect from.
    """
    peer_info: PeerInfo
    reason: DisconnectReason


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
    """
    The response class to answer a :class:`trinity.protocol.common.events.GetConnectedPeersRequest`
    """
    peers: Tuple[PeerInfo, ...]

    @staticmethod
    def from_connected_nodes(peers: Dict[SessionAPI, BasePeer]) -> 'GetConnectedPeersResponse':
        return GetConnectedPeersResponse(tuple(
            PeerInfo(
                session=session,
                capabilities=peer.connection.remote_capabilities,
                client_version_string=peer.connection.safe_client_version_string,
                inbound=peer.inbound
            ) for session, peer in peers.items()
        ))


class GetConnectedPeersRequest(BaseRequestResponseEvent[GetConnectedPeersResponse]):
    """
    A request class that can be dispatched from any process to be answered from another process
    with a :class:`trinity.protocol.common.events.GetConnectedPeersResponse`.
   """
    @staticmethod
    def expected_response_type() -> Type[GetConnectedPeersResponse]:
        return GetConnectedPeersResponse


@dataclass
class PeerPoolMessageEvent(BaseEvent):
    """
    Base event for all peer messages that are routed through the event bus. The events are mapped
    to individual subclasses for every different ``cmd`` to allow efficient consumption through
    the event bus.
    The event type is used bidirectionally, for peer messages that originate in the peer pool and
    are propagated to any consuming party but also for peer messages that originate elsewhere but
    are propagated toward the peer pool to be dispatched on a peer.
    """
    session: SessionAPI
    command: CommandAPI[Any]


@dataclass
class ProtocolCapabilitiesResponse(BaseEvent):
    """
    The response class to answer a
    :class:`trinity.protocol.common.events.GetProtocolCapabilitiesRequest`
    """
    capabilities: Capabilities


class GetProtocolCapabilitiesRequest(BaseRequestResponseEvent[ProtocolCapabilitiesResponse]):
    """
    A request class that can be dispatched from any process to be answered from another process
    with a :class:`trinity.protocol.common.events.ProtocolCapabilitiesResponse`.
   """
    @staticmethod
    def expected_response_type() -> Type[ProtocolCapabilitiesResponse]:
        return ProtocolCapabilitiesResponse
