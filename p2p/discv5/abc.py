from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    AsyncContextManager,
    AsyncIterable,
    Generic,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

from eth.abc import DatabaseAPI

from p2p.discv5.enr import (
    ENR,
)
from p2p.discv5.channel_services import (
    Endpoint,
    IncomingMessage,
)
from p2p.discv5.identity_schemes import (
    IdentityScheme,
    IdentitySchemeRegistry,
)
from p2p.discv5.messages import (
    BaseMessage,
)
from p2p.discv5.packets import (
    Packet,
)
from p2p.discv5.typing import (
    NodeID,
    HandshakeResult,
    Tag,
)


class HandshakeParticipantAPI(ABC):
    @abstractmethod
    def __init__(self,
                 is_initiator: bool,
                 local_private_key: bytes,
                 local_enr: ENR,
                 remote_node_id: NodeID,
                 ) -> None:
        ...

    @property
    @abstractmethod
    def first_packet_to_send(self) -> Packet:
        """The first packet we have to send the peer."""
        ...

    @abstractmethod
    def is_response_packet(self, packet: Packet) -> bool:
        """Check if the given packet is the response we need to complete the handshake."""
        ...

    @abstractmethod
    def complete_handshake(self, response_packet: Packet) -> HandshakeResult:
        """Complete the handshake using a response packet received from the peer."""
        ...

    @property
    @abstractmethod
    def is_initiator(self) -> bool:
        """`True` if the handshake was initiated by us, `False` if it was initiated by the peer."""
        ...

    @property
    @abstractmethod
    def identity_scheme(self) -> Type[IdentityScheme]:
        """The identity scheme used during the handshake."""
        ...

    @property
    @abstractmethod
    def local_private_key(self) -> bytes:
        """The static node key of this node."""
        ...

    @property
    @abstractmethod
    def local_enr(self) -> ENR:
        """The ENR of this node"""
        ...

    @property
    @abstractmethod
    def local_node_id(self) -> NodeID:
        """The node id of this node."""
        ...

    @property
    @abstractmethod
    def remote_node_id(self) -> NodeID:
        """The peer's node id."""
        ...

    @property
    @abstractmethod
    def tag(self) -> Tag:
        """The tag used for message packets sent by this node to the peer."""
        ...


class NodeDBAPI(ABC):

    @abstractmethod
    def __init__(self, identity_scheme_registry: IdentitySchemeRegistry, db: DatabaseAPI) -> None:
        ...

    @abstractmethod
    def set_enr(self, enr: ENR) -> None:
        ...

    @abstractmethod
    def get_enr(self, node_id: NodeID) -> ENR:
        ...

    @abstractmethod
    def delete_enr(self, node_id: NodeID) -> None:
        ...

    @abstractmethod
    def set_last_pong_time(self, node_id: NodeID, last_pong: int) -> None:
        ...

    @abstractmethod
    def get_last_pong_time(self, node_id: NodeID) -> int:
        ...

    @abstractmethod
    def delete_last_pong_time(self, node_id: NodeID) -> None:
        ...


ChannelContentType = TypeVar("ChannelContentType")
ChannelHandlerAsyncContextManager = AsyncContextManager[
    "ChannelHandlerSubscriptionAPI[ChannelContentType]"
]


class ChannelHandlerSubscriptionAPI(Generic[ChannelContentType],
                                    AsyncIterable[ChannelContentType],
                                    AsyncContextManager[
                                        "ChannelHandlerSubscriptionAPI[ChannelContentType]"],
                                    ):
    @abstractmethod
    def cancel(self) -> None:
        ...

    @abstractmethod
    async def receive(self) -> ChannelContentType:
        ...


class MessageDispatcherAPI(ABC):
    @abstractmethod
    def get_free_request_id(self, node_id: NodeID) -> int:
        """Get a currently unused request id for requests to the given node."""
        ...

    @abstractmethod
    async def request(self,
                      receiver_node_id: NodeID,
                      message: BaseMessage,
                      endpoint: Optional[Endpoint] = None,
                      ) -> IncomingMessage:
        """Send a request to the given peer and return the response.

        This is the primary interface for requesting data from a peer. Internally, it will look up
        the peer's ENR in the database, extract endpoint information from it, add a response
        handler, send the request, wait for the response, and finally remove the handler again.

        This method cannot be used if the response consists of multiple messages.

        If no endpoint is given, it will be queried from the ENR DB, raising a ValueError if it is
        not present.
        """
        ...

    @abstractmethod
    async def request_nodes(self,
                            receiver_node_id: NodeID,
                            message: BaseMessage,
                            endpoint: Optional[Endpoint] = None,
                            ) -> Tuple[IncomingMessage, ...]:
        """Send a request to the given peer and return the collection of Nodes responses.

        Similar to `request`, but waits for all Nodes messages sent in response. If a different
        message type is received or the messages are in some other way invalid, an
        `UnexpectedMessage` error is thrown.
        """
        ...

    @abstractmethod
    def add_request_handler(self,
                            message_class: Type[BaseMessage],
                            ) -> ChannelHandlerSubscriptionAPI[IncomingMessage]:
        """Add a request handler for messages of a given type.

        Only one handler per message type can be added.
        """
        ...

    @abstractmethod
    def add_response_handler(self,
                             remote_node_id: NodeID,
                             request_id: int,
                             ) -> ChannelHandlerSubscriptionAPI[IncomingMessage]:
        """Add a response handler.

        All messages sent by the given peer with the given request id will be send to the returned
        handler's channel.
        """
        ...
