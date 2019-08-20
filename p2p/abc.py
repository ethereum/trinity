from abc import ABC, abstractmethod
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    ClassVar,
    Dict,
    Generic,
    List,
    Tuple,
    Type,
    TYPE_CHECKING,
    TypeVar,
    Union,
)


from rlp import sedes

from cancel_token import CancelToken

from eth_keys import datatypes

from p2p.disconnect import DisconnectReason
from p2p.typing import Capability, Capabilities, Payload, Structure

if TYPE_CHECKING:
    from p2p.p2p_proto import (  # noqa: F401
        BaseP2PProtocol,
    )


TAddress = TypeVar('TAddress', bound='AddressAPI')


class AddressAPI(ABC):
    udp_port: int
    tcp_port: int

    @abstractmethod
    def __init__(self, ip: str, udp_port: int, tcp_port: int = 0) -> None:
        ...

    @property
    @abstractmethod
    def is_loopback(self) -> bool:
        ...

    @property
    @abstractmethod
    def is_unspecified(self) -> bool:
        ...

    @property
    @abstractmethod
    def is_reserved(self) -> bool:
        ...

    @property
    @abstractmethod
    def is_private(self) -> bool:
        ...

    @property
    @abstractmethod
    def ip(self) -> str:
        ...

    @abstractmethod
    def __eq__(self, other: Any) -> bool:
        ...

    @abstractmethod
    def to_endpoint(self) -> List[bytes]:
        ...

    @classmethod
    @abstractmethod
    def from_endpoint(cls: Type[TAddress],
                      ip: str,
                      udp_port: bytes,
                      tcp_port: bytes = b'\x00\x00') -> TAddress:
        ...


TNode = TypeVar('TNode', bound='NodeAPI')


class NodeAPI(ABC):
    pubkey: datatypes.PublicKey
    address: AddressAPI
    id: int

    @abstractmethod
    def __init__(self, pubkey: datatypes.PublicKey, address: AddressAPI) -> None:
        ...

    @classmethod
    @abstractmethod
    def from_uri(cls: Type[TNode], uri: str) -> TNode:
        ...

    @abstractmethod
    def uri(self) -> str:
        ...

    @abstractmethod
    def distance_to(self, id: int) -> int:
        ...

    # mypy doesn't have support for @total_ordering
    # https://github.com/python/mypy/issues/4610
    @abstractmethod
    def __lt__(self, other: Any) -> bool:
        ...

    @abstractmethod
    def __eq__(self, other: Any) -> bool:
        ...

    @abstractmethod
    def __ne__(self, other: Any) -> bool:
        ...

    @abstractmethod
    def __hash__(self) -> int:
        ...


class CommandAPI(ABC):
    structure: Structure

    cmd_id: int
    cmd_id_offset: int
    snappy_support: bool

    @abstractmethod
    def __init__(self, cmd_id_offset: int, snappy_support: bool) -> None:
        ...

    @property
    @abstractmethod
    def is_base_protocol(self) -> bool:
        ...

    @abstractmethod
    def encode_payload(self, data: Union[Payload, sedes.CountableList]) -> bytes:
        ...

    @abstractmethod
    def decode_payload(self, rlp_data: bytes) -> Payload:
        ...

    @abstractmethod
    def encode(self, data: Payload) -> Tuple[bytes, bytes]:
        ...

    @abstractmethod
    def decode(self, data: bytes) -> Payload:
        ...

    @abstractmethod
    def decompress_payload(self, raw_payload: bytes) -> bytes:
        ...

    @abstractmethod
    def compress_payload(self, raw_payload: bytes) -> bytes:
        ...


# A payload to be delivered with a request
TRequestPayload = TypeVar('TRequestPayload', bound=Payload, covariant=True)


class RequestAPI(ABC, Generic[TRequestPayload]):
    """
    Must define command_payload during init. This is the data that will
    be sent to the peer with the request command.
    """
    # Defined at init time, with specific parameters:
    command_payload: TRequestPayload

    # Defined as class attributes in subclasses
    # outbound command type
    cmd_type: Type[CommandAPI]
    # response command type
    response_type: Type[CommandAPI]


class TransportAPI(ABC):
    remote: NodeAPI

    @property
    @abstractmethod
    def is_closing(self) -> bool:
        ...

    @property
    @abstractmethod
    def public_key(self) -> datatypes.PublicKey:
        ...

    @abstractmethod
    async def read(self, n: int, token: CancelToken) -> bytes:
        ...

    @abstractmethod
    def write(self, data: bytes) -> None:
        ...

    @abstractmethod
    async def recv(self, token: CancelToken) -> bytes:
        ...

    @abstractmethod
    def send(self, header: bytes, body: bytes) -> None:
        ...

    @abstractmethod
    def close(self) -> None:
        ...


class ProtocolAPI(ABC):
    transport: TransportAPI
    name: ClassVar[str]
    version: ClassVar[int]

    cmd_length: int

    cmd_id_offset: int

    commands: Tuple[CommandAPI, ...]
    cmd_by_type: Dict[Type[CommandAPI], CommandAPI]
    cmd_by_id: Dict[int, CommandAPI]

    @abstractmethod
    def __init__(self, transport: TransportAPI, cmd_id_offset: int, snappy_support: bool) -> None:
        ...

    @abstractmethod
    def send_request(self, request: RequestAPI[Payload]) -> None:
        ...

    @abstractmethod
    def supports_command(self, cmd_type: Type[CommandAPI]) -> bool:
        ...

    @classmethod
    @abstractmethod
    def as_capability(cls) -> Capability:
        ...


TProtocol = TypeVar('TProtocol', bound=ProtocolAPI)


class MultiplexerAPI(ABC):
    cancel_token: CancelToken

    #
    # Transport API
    #
    @abstractmethod
    def get_transport(self) -> TransportAPI:
        ...

    #
    # Message Counts
    #
    @abstractmethod
    def get_total_msg_count(self) -> int:
        ...

    #
    # Proxy Transport methods
    #
    @property
    @abstractmethod
    def remote(self) -> NodeAPI:
        ...

    @property
    @abstractmethod
    def is_closing(self) -> bool:
        ...

    @abstractmethod
    def close(self) -> None:
        ...

    #
    # Protocol API
    #
    @abstractmethod
    def has_protocol(self, protocol_identifier: Union[ProtocolAPI, Type[ProtocolAPI]]) -> bool:
        ...

    @abstractmethod
    def get_protocol_by_type(self, protocol_class: Type[TProtocol]) -> TProtocol:
        ...

    @abstractmethod
    def get_base_protocol(self) -> 'BaseP2PProtocol':
        ...

    @abstractmethod
    def get_protocols(self) -> Tuple[ProtocolAPI, ...]:
        ...

    #
    # Streaming API
    #
    @abstractmethod
    def stream_protocol_messages(self,
                                 protocol_identifier: Union[ProtocolAPI, Type[ProtocolAPI]],
                                 ) -> AsyncIterator[Tuple[CommandAPI, Payload]]:
        ...

    #
    # Message reading and streaming API
    #
    def multiplex(self) -> AsyncContextManager[None]:
        ...


class HandlerSubscriptionAPI:
    remove_fn: Callable[[], None]


class Connection(ABC):
    disconnect_reason: DisconnectReason = None

    #
    # Primary properties of the connection
    #
    @property
    @abstractmethod
    def is_dial_in(self) -> bool:
        ...

    @property
    @abstractmethod
    def remote(self) -> NodeAPI:
        ...

    #
    # Subscriptions/Handler API
    #
    @abstractmethod
    def add_protocol_handler(self,
                             protocol_name: str,
                             handler_fn: Callable[[CommandAPI, Payload], Awaitable[Any]],
                             ) -> HandlerSubscriptionAPI:
        ...

    @abstractmethod
    def add_command_handler(self,
                            command_type: Type[CommandAPI],
                            handler_fn: Callable[[Payload], Awaitable[Any]],
                            ) -> HandlerSubscriptionAPI:
        ...

    #
    # Access to underlying Multiplexer
    #
    @abstractmethod
    def get_multiplexer(self) -> MultiplexerAPI:
        ...

    #
    # Base Protocol shortcuts
    #
    @property
    @abstractmethod
    def base_protocol(self) -> 'P2PProtocol':
        ...

    @property
    @abstractmethod
    def remote_capabilities(self) -> Capabilities:
        ...

    @property
    @abstractmethod
    def p2p_version(self) -> int:
        ...

    @property
    @abstractmethod
    def client_version_string(self) -> str:
        ...

    @property
    @abstractmethod
    def safe_client_version_string(self) -> str:
        ...

    #
    # Disconnect API
    #
    @abstractmethod
    async def disconnect(self, reason: DisconnectReason) -> None:
        ...
