from __future__ import annotations
from abc import ABC, abstractmethod
import asyncio
import contextlib
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    ClassVar,
    ContextManager,
    Generic,
    Hashable,
    List,
    Optional,
    Tuple,
    Type,
    TYPE_CHECKING,
    TypeVar,
    Union,
)
import uuid

from async_service.abc import ServiceAPI

from eth_typing import NodeID
from eth_utils import ExtendedDebugLogger

from eth_keys import keys
from eth_enr.abc import ENRAPI

from p2p.typing import (
    Capabilities,
    Capability,
    TCommandPayload,
)

if TYPE_CHECKING:
    from p2p.handshake import DevP2PReceipt  # noqa: F401
    from p2p.peer import BasePeer  # noqa: F401
    from p2p.p2p_proto import (  # noqa: F401
        BaseP2PProtocol,
    )


ENR_FieldProvider = Callable[[], Awaitable[Tuple[bytes, Any]]]
TAddress = TypeVar('TAddress', bound='AddressAPI')


class AddressAPI(ABC):
    udp_port: int
    tcp_port: int

    @abstractmethod
    def __init__(self, ip: str, udp_port: int, tcp_port: int) -> None:
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

    @property
    @abstractmethod
    def ip_packed(self) -> str:
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
TPeer = TypeVar('TPeer', bound='BasePeer')


class NodeAPI(ABC):
    """
    A node in the Ethereum P2P network.

    Its state is stored in an ENR instance, which is exposed as a read-only attribute like all
    other attributes that are derived from the ENR.

    When we get a new ENR version for a given NodeID, a new NodeAPI instance must be created.
    """

    @abstractmethod
    def __init__(self, enr: ENRAPI) -> None:
        ...

    @classmethod
    @abstractmethod
    def from_pubkey_and_addr(
            cls: Type[TNode], pubkey: keys.PublicKey, address: AddressAPI) -> TNode:
        ...

    @classmethod
    @abstractmethod
    def from_uri(cls: Type[TNode], uri: str) -> TNode:
        ...

    @classmethod
    @abstractmethod
    def from_enode_uri(cls: Type[TNode], uri: str) -> TNode:
        ...

    @classmethod
    @abstractmethod
    def from_enr_repr(cls: Type[TNode], uri: str) -> TNode:
        ...

    @property
    @abstractmethod
    def pubkey(self) -> keys.PublicKey:
        ...

    @property
    @abstractmethod
    def address(self) -> AddressAPI:
        ...

    @property
    @abstractmethod
    def id(self) -> NodeID:
        ...

    @property
    @abstractmethod
    def enr(self) -> ENRAPI:
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


class SessionAPI(ABC, Hashable):
    id: uuid.UUID
    remote: NodeAPI


class SerializationCodecAPI(ABC, Generic[TCommandPayload]):
    @abstractmethod
    def encode(self, payload: TCommandPayload) -> bytes:
        ...

    @abstractmethod
    def decode(self, data: bytes) -> TCommandPayload:
        ...


class CompressionCodecAPI(ABC):
    @abstractmethod
    def compress(self, data: bytes) -> bytes:
        ...

    @abstractmethod
    def decompress(self, data: bytes) -> bytes:
        ...


class MessageAPI(ABC):
    header: bytes
    body: bytes
    # This is the combined `command_id_offset + protocol_command_id`
    command_id: int
    # This is the `body` with the first byte stripped off
    encoded_payload: bytes


class CommandAPI(ABC, Generic[TCommandPayload]):
    # This is the local `id` for the command within the context of the
    # protocol.
    protocol_command_id: ClassVar[int]
    serialization_codec: SerializationCodecAPI[TCommandPayload]
    compression_codec: CompressionCodecAPI

    payload: TCommandPayload

    @abstractmethod
    def __init__(self, payload: TCommandPayload) -> None:
        ...

    @abstractmethod
    def encode(self, negotiated_command_id: int, snappy_support: bool) -> MessageAPI:
        ...

    @classmethod
    @abstractmethod
    def decode(cls: Type['TCommand'], message: MessageAPI, snappy_support: bool) -> 'TCommand':
        ...


TCommand = TypeVar("TCommand", bound=CommandAPI[Any])


class TransportAPI(ABC):
    session: SessionAPI
    remote: NodeAPI
    logger: ExtendedDebugLogger

    @property
    @abstractmethod
    def is_closing(self) -> bool:
        ...

    @property
    @abstractmethod
    def public_key(self) -> keys.PublicKey:
        ...

    @abstractmethod
    async def read(self, n: int) -> bytes:
        ...

    @abstractmethod
    def write(self, data: bytes) -> None:
        ...

    @abstractmethod
    async def recv(self) -> MessageAPI:
        ...

    @abstractmethod
    def send(self, message: MessageAPI) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...


class ProtocolAPI(ABC):
    name: ClassVar[str]
    version: ClassVar[int]

    # Command classes that this protocol supports.
    commands: ClassVar[Tuple[Type[CommandAPI[Any]], ...]]
    command_length: ClassVar[int]

    command_id_offset: int
    snappy_support: bool
    transport: TransportAPI

    @abstractmethod
    def __init__(self,
                 transport: TransportAPI,
                 negotiated_command_id_offset: int,
                 snappy_support: bool) -> None:
        ...

    @classmethod
    @abstractmethod
    def supports_command(cls, command_type: Type[CommandAPI[Any]]) -> bool:
        ...

    @classmethod
    @abstractmethod
    def as_capability(cls) -> Capability:
        ...

    @abstractmethod
    def get_command_type_for_command_id(self, command_id: int) -> Type[CommandAPI[Any]]:
        ...

    @abstractmethod
    def send(self, command: CommandAPI[Any]) -> None:
        ...


TProtocol = TypeVar('TProtocol', bound=ProtocolAPI)


class MultiplexerAPI(ABC):

    @abstractmethod
    async def stream_in_background(self) -> None:
        """Start streaming of messages in the background.

        Multiplexers are created and start streaming during the auth handshake, but afterwards are
        passed to Connection instances and we need to be running in the background to ensure no
        messages are dropped.
        """
        ...

    @abstractmethod
    async def wait_streaming_finished(self) -> None:
        """Wait until the background streaming finishes or raises any exceptions."""
        ...

    @abstractmethod
    def get_streaming_error(self) -> Optional[BaseException]:
        """
        Return the Exception raised by the streaming task, if any.

        If streaming has not started, raise an Exception.

        If streaming is not finished, return None.

        If streaming was cancelled, return CancelledError.
        """
        ...

    @abstractmethod
    def raise_if_streaming_error(self) -> None:
        ...

    @property
    @abstractmethod
    def is_streaming(self) -> bool:
        ...

    @abstractmethod
    def cancel_streaming(self) -> None:
        """Cancel the background streaming."""
        ...

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

    @property
    @abstractmethod
    def last_msg_time(self) -> float:
        ...

    #
    # Proxy Transport properties and methods
    #
    @property
    @abstractmethod
    def session(self) -> SessionAPI:
        ...

    @property
    @abstractmethod
    def remote(self) -> NodeAPI:
        ...

    @property
    @abstractmethod
    def is_closing(self) -> bool:
        ...

    @abstractmethod
    async def close(self) -> None:
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

    @abstractmethod
    def get_protocol_for_command_type(self, command_type: Type[CommandAPI[Any]]) -> ProtocolAPI:
        ...

    #
    # Streaming API
    #
    @abstractmethod
    def stream_protocol_messages(self,
                                 protocol_identifier: Union[ProtocolAPI, Type[ProtocolAPI]],
                                 ) -> AsyncIterator[CommandAPI[Any]]:
        ...


class ServiceEventsAPI(ABC):
    started: asyncio.Event
    stopped: asyncio.Event
    cleaned_up: asyncio.Event
    cancelled: asyncio.Event
    finished: asyncio.Event


TReturn = TypeVar('TReturn')


class HandshakeCheckAPI(ABC):
    """
    A certain type of check performed during a handshake with another peer.

    e.g. Checking that we're on the same network, ForkID validation, etc.
    """


class HandshakeReceiptAPI(ABC):
    protocol: ProtocolAPI

    @abstractmethod
    def was_check_performed(self, check_type: Type[HandshakeCheckAPI]) -> bool:
        ...


THandshakeReceipt = TypeVar('THandshakeReceipt', bound=HandshakeReceiptAPI)


class HandshakerAPI(ABC, Generic[TProtocol]):

    protocol_class: Type[TProtocol]

    @property
    @abstractmethod
    def logger(self) -> ExtendedDebugLogger:
        ...

    @abstractmethod
    async def do_handshake(self,
                           multiplexer: MultiplexerAPI,
                           protocol: TProtocol) -> HandshakeReceiptAPI:
        """
        Perform the actual handshake for the protocol.
        """
        ...


QualifierFn = Callable[['ConnectionAPI', 'LogicAPI'], bool]


class LogicAPI(ABC):
    @abstractmethod
    def as_behavior(self, qualifier: QualifierFn = None) -> 'BehaviorAPI':
        ...

    @abstractmethod
    @contextlib.asynccontextmanager
    def apply(self, connection: 'ConnectionAPI') -> AsyncIterator[asyncio.Task[Any]]:
        """
        Apply this behavior to the given connection.

        The returned future must be used in callsites to detect early exits (i.e. the behaviour
        exiting before the connection is cancelled) or unexpected errors.
        """
        ...


TLogic = TypeVar('TLogic', bound=LogicAPI)


class BehaviorAPI(ABC):
    qualifier: QualifierFn
    logic: Any

    @abstractmethod
    def should_apply_to(self, connection: 'ConnectionAPI') -> bool:
        ...

    @abstractmethod
    def post_apply(self) -> None:
        """
        Called after all behaviors have been applied to the Connection.
        """
        ...

    @abstractmethod
    @contextlib.asynccontextmanager
    def apply(self, connection: 'ConnectionAPI') -> AsyncIterator[asyncio.Task[Any]]:
        """
        Context manager API used programatically by the `ContextManager` to
        apply the behavior to the connection during the lifecycle of the
        connection.
        """
        ...


TBehavior = TypeVar('TBehavior', bound=BehaviorAPI)


class SubscriptionAPI(ContextManager['SubscriptionAPI']):
    @abstractmethod
    def cancel(self) -> None:
        ...


HandlerFn = Callable[['ConnectionAPI', CommandAPI[Any]], Awaitable[Any]]


class ConnectionAPI(ServiceAPI):
    protocol_receipts: Tuple[HandshakeReceiptAPI, ...]
    logger: ExtendedDebugLogger

    #
    # Primary properties of the connection
    #
    behaviors_applied: asyncio.Event
    is_dial_out: bool

    @property
    @abstractmethod
    def is_dial_in(self) -> bool:
        ...

    @property
    @abstractmethod
    def session(self) -> SessionAPI:
        ...

    @property
    @abstractmethod
    def remote(self) -> NodeAPI:
        ...

    @property
    @abstractmethod
    def is_alive(self) -> bool:
        ...

    @abstractmethod
    async def run_peer(self, peer: 'BasePeer') -> None:
        ...

    #
    # Subscriptions/Handler API
    #
    @abstractmethod
    def start_protocol_streams(self) -> None:
        ...

    @property
    @abstractmethod
    def is_streaming_messages(self) -> bool:
        ...

    @abstractmethod
    def add_protocol_handler(self,
                             protocol_type: Type[ProtocolAPI],
                             handler_fn: HandlerFn,
                             ) -> SubscriptionAPI:
        ...

    @abstractmethod
    def add_command_handler(self,
                            command_type: Type[CommandAPI[Any]],
                            handler_fn: HandlerFn,
                            ) -> SubscriptionAPI:
        """
        Add a handler for messages of the given type.
        """
        ...

    @abstractmethod
    def add_msg_handler(self, handler_fn: HandlerFn) -> SubscriptionAPI:
        """
        Add a handler for messages of any type.
        """
        ...

    #
    # Behavior API
    #
    @abstractmethod
    def add_logic(self, name: str, logic: LogicAPI) -> SubscriptionAPI:
        ...

    @abstractmethod
    def remove_logic(self, name: str) -> None:
        ...

    @abstractmethod
    def has_logic(self, name: str) -> bool:
        ...

    @abstractmethod
    def get_logic(self, name: str, logic_type: Type[TLogic]) -> TLogic:
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
    @abstractmethod
    def get_base_protocol(self) -> 'BaseP2PProtocol':
        ...

    @abstractmethod
    def get_p2p_receipt(self) -> 'DevP2PReceipt':
        ...

    #
    # Protocol APIS
    #
    @abstractmethod
    def has_protocol(self, protocol_identifier: Union[ProtocolAPI, Type[ProtocolAPI]]) -> bool:
        ...

    @abstractmethod
    def get_protocols(self) -> Tuple[ProtocolAPI, ...]:
        ...

    @abstractmethod
    def get_protocol_by_type(self, protocol_type: Type[TProtocol]) -> TProtocol:
        ...

    @abstractmethod
    def get_protocol_for_command_type(self, command_type: Type[CommandAPI[Any]]) -> ProtocolAPI:
        ...

    @abstractmethod
    def get_receipt_by_type(self, receipt_type: Type[THandshakeReceipt]) -> THandshakeReceipt:
        ...

    #
    # Connection Metadata
    #
    @property
    @abstractmethod
    def remote_capabilities(self) -> Capabilities:
        ...

    @property
    @abstractmethod
    def remote_p2p_version(self) -> int:
        ...

    @property
    @abstractmethod
    def client_version_string(self) -> str:
        ...

    @property
    @abstractmethod
    def safe_client_version_string(self) -> str:
        ...
