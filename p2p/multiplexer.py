import asyncio
import collections
import time
from typing import (
    Any,
    AsyncIterator,
    cast,
    DefaultDict,
    Dict,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

from cached_property import cached_property

from eth_utils import ValidationError
from eth_utils.toolz import cons
import rlp

from p2p.abc import (
    CommandAPI,
    MultiplexerAPI,
    NodeAPI,
    ProtocolAPI,
    SessionAPI,
    TransportAPI,
    TProtocol,
)
from p2p.constants import (
    MAX_IN_LOOP_DECODE_SIZE,
)
from p2p.exceptions import (
    MalformedMessage,
    PeerConnectionLost,
    UnknownProtocol,
    UnknownProtocolCommand,
)
from p2p.p2p_proto import BaseP2PProtocol
from p2p._utils import (
    get_logger,
    snappy_CompressedLengthError,
)


async def stream_transport_messages(transport: TransportAPI,
                                    base_protocol: BaseP2PProtocol,
                                    *protocols: ProtocolAPI,
                                    ) -> AsyncIterator[Tuple[ProtocolAPI, CommandAPI[Any]]]:
    """
    Streams 2-tuples of (Protocol, Command) over the provided `Transport`

    Raises a TimeoutError if nothing is received in constants.CONN_IDLE_TIMEOUT seconds.
    """
    # A cache for looking up the proper protocol instance for a given command
    # id.
    command_id_cache: Dict[int, ProtocolAPI] = {}
    loop = asyncio.get_event_loop()

    while not transport.is_closing:
        try:
            msg = await transport.recv()
        except PeerConnectionLost:
            transport.logger.debug(
                "Lost connection to %s, leaving stream_transport_messages()", transport.remote)
            return

        command_id = msg.command_id

        if msg.command_id not in command_id_cache:
            if command_id < base_protocol.command_length:
                command_id_cache[command_id] = base_protocol
            else:
                for protocol in protocols:
                    if command_id < protocol.command_id_offset + protocol.command_length:
                        command_id_cache[command_id] = protocol
                        break
                else:
                    protocol_infos = '  '.join(tuple(
                        (
                            f"{proto.name}@{proto.version}"
                            f"[offset={proto.command_id_offset},"
                            f"command_length={proto.command_length}]"
                        )
                        for proto in cons(base_protocol, protocols)
                    ))
                    raise UnknownProtocolCommand(
                        f"No protocol found for command_id {command_id}: Available "
                        f"protocol/offsets are: {protocol_infos}"
                    )

        msg_proto = command_id_cache[command_id]
        command_type = msg_proto.get_command_type_for_command_id(command_id)

        try:
            if len(msg.body) > MAX_IN_LOOP_DECODE_SIZE:
                cmd = await loop.run_in_executor(
                    None,
                    command_type.decode,
                    msg,
                    msg_proto.snappy_support,
                )
            else:
                cmd = command_type.decode(msg, msg_proto.snappy_support)

        except (rlp.exceptions.DeserializationError, snappy_CompressedLengthError) as err:
            raise MalformedMessage(f"Failed to decode {msg} for {command_type}") from err

        yield msg_proto, cmd

        # yield to the event loop for a moment to allow `transport.is_closing`
        # a chance to update.
        await asyncio.sleep(0)


class Multiplexer(MultiplexerAPI):

    _transport: TransportAPI
    _msg_counts: DefaultDict[Type[CommandAPI[Any]], int]
    _last_msg_time: float
    _stream_idle_timeout: int = 5

    _protocol_locks: Dict[Type[ProtocolAPI], asyncio.Lock]
    _protocol_queues: Dict[Type[ProtocolAPI], 'asyncio.Queue[CommandAPI[Any]]']

    def __init__(self,
                 transport: TransportAPI,
                 base_protocol: BaseP2PProtocol,
                 protocols: Sequence[ProtocolAPI],
                 max_queue_size: int = 4096) -> None:
        self.logger = get_logger('p2p.multiplexer.Multiplexer')
        self._transport = transport
        # the base `p2p` protocol instance.
        self._base_protocol = base_protocol

        # the sub-protocol instances
        self._protocols = protocols

        self._streaming_task: asyncio.Future[None] = None

        # Lock management on a per-protocol basis to ensure we only have one
        # stream consumer for each protocol.
        self._protocol_locks = {
            type(protocol): asyncio.Lock()
            for protocol
            in self.get_protocols()
        }

        # Each protocol gets a queue where messages for the individual protocol
        # are placed when streamed from the transport
        self._protocol_queues = {
            type(protocol): asyncio.Queue(max_queue_size)
            for protocol
            in self.get_protocols()
        }

        self._msg_counts = collections.defaultdict(int)
        self._last_msg_time = 0
        self._started_streaming = asyncio.Event()

    def __str__(self) -> str:
        protocol_infos = ','.join(tuple(
            f"{proto.name}:{proto.version}"
            for proto
            in self.get_protocols()
        ))
        return (
            f"Multiplexer[{self.remote.address.ip}/{protocol_infos}/streaming={self.is_streaming}]")

    def __repr__(self) -> str:
        return f"<{self}>"

    #
    # Transport API
    #
    def get_transport(self) -> TransportAPI:
        return self._transport

    #
    # Message Counts
    #
    def get_total_msg_count(self) -> int:
        return sum(self._msg_counts.values())

    @property
    def last_msg_time(self) -> float:
        return self._last_msg_time

    #
    # Proxy Transport methods
    #
    @cached_property
    def remote(self) -> NodeAPI:
        return self._transport.remote

    @cached_property
    def session(self) -> SessionAPI:
        return self._transport.session

    @property
    def is_closing(self) -> bool:
        return self._transport.is_closing

    async def close(self) -> None:
        await self._transport.close()

    #
    # Protocol API
    #
    def has_protocol(self, protocol_identifier: Union[ProtocolAPI, Type[ProtocolAPI]]) -> bool:
        try:
            if isinstance(protocol_identifier, ProtocolAPI):
                self.get_protocol_by_type(type(protocol_identifier))
                return True
            elif isinstance(protocol_identifier, type):
                self.get_protocol_by_type(protocol_identifier)
                return True
            else:
                raise TypeError(
                    f"Unsupported protocol value: {protocol_identifier} of type "
                    f"{type(protocol_identifier)}"
                )
        except UnknownProtocol:
            return False

    def get_protocol_by_type(self, protocol_class: Type[TProtocol]) -> TProtocol:
        if issubclass(protocol_class, BaseP2PProtocol):
            return cast(TProtocol, self._base_protocol)

        for protocol in self._protocols:
            if type(protocol) is protocol_class:
                return cast(TProtocol, protocol)
        raise UnknownProtocol(f"No protocol found with type {protocol_class}")

    def get_base_protocol(self) -> BaseP2PProtocol:
        return self._base_protocol

    def get_protocols(self) -> Tuple[ProtocolAPI, ...]:
        return tuple(cons(self._base_protocol, self._protocols))

    def get_protocol_for_command_type(self, command_type: Type[CommandAPI[Any]]) -> ProtocolAPI:
        supported_protocols = tuple(
            protocol
            for protocol in self.get_protocols()
            if protocol.supports_command(command_type)
        )

        if len(supported_protocols) == 1:
            return supported_protocols[0]
        elif not supported_protocols:
            raise UnknownProtocol(
                f"Connection does not have any protocols that support the "
                f"request command: {command_type}"
            )
        elif len(supported_protocols) > 1:
            raise ValidationError(
                f"Could not determine appropriate protocol for command: "
                f"{command_type}.  Command was found in the "
                f"protocols {supported_protocols}"
            )
        else:
            raise Exception("This code path should be unreachable")

    #
    # Streaming API
    #
    async def stream_protocol_messages(self,
                                       protocol_identifier: Union[ProtocolAPI, Type[ProtocolAPI]],
                                       ) -> AsyncIterator[CommandAPI[Any]]:
        """
        Stream the messages for the specified protocol.
        """
        if isinstance(protocol_identifier, ProtocolAPI):
            protocol_class = type(protocol_identifier)
        elif isinstance(protocol_identifier, type) and issubclass(protocol_identifier, ProtocolAPI):
            protocol_class = protocol_identifier
        else:
            raise TypeError(f"Unknown protocol identifier: {protocol_identifier}")

        if not self.has_protocol(protocol_class):
            raise UnknownProtocol(f"Unknown protocol '{protocol_class}'")

        if self._protocol_locks[protocol_class].locked():
            raise Exception(f"Streaming lock for {protocol_class} is not free.")

        async with self._protocol_locks[protocol_class]:
            self.raise_if_streaming_error()
            msg_queue = self._protocol_queues[protocol_class]
            while self.is_streaming:
                if not msg_queue.empty():
                    # We use an optimistic strategy here of using
                    # `get_nowait()` to reduce the number of times we yield to
                    # the event loop.
                    yield msg_queue.get_nowait()

                    # Manually release the event loop to prevent blocking it for too long in case
                    # the queue has lots of items.
                    await asyncio.sleep(0)
                else:
                    # Must run this with a timeout so that we return in case the multiplexer
                    # stops streaming, althouth there's no real need to have this wake up so
                    # frequently in case there are no messages, so use a long one.
                    try:
                        yield await asyncio.wait_for(
                            msg_queue.get(), timeout=self._stream_idle_timeout)
                    except asyncio.TimeoutError:
                        pass

    #
    # Message reading and streaming API
    #
    async def stream_in_background(self) -> None:
        self._streaming_task = asyncio.ensure_future(self._do_multiplexing())
        await self._started_streaming.wait()

    @property
    def is_streaming(self) -> bool:
        return self._streaming_task is not None and not self._streaming_task.done()

    async def wait_streaming_finished(self) -> None:
        """
        Wait for our streaming task to finish, propagating any errors from it.

        The streaming must have been started via stream_in_background().

        Upon returning, the multiplexer and transport will be closed.
        """
        if self._streaming_task is None:
            raise Exception("Multiplexer has not started streaming")
        await self._streaming_task

    def get_streaming_error(self) -> Optional[BaseException]:
        if self._streaming_task is None:
            raise Exception("Multiplexer has not started streaming")
        elif self._streaming_task.cancelled():
            return asyncio.CancelledError()
        elif not self._streaming_task.done():
            return None
        return self._streaming_task.exception()

    def raise_if_streaming_error(self) -> None:
        err = self.get_streaming_error()
        if err:
            raise err

    async def _do_multiplexing(self) -> None:
        """
        Read messages from the transport and feeds them into individual queues for each of the
        protocols.

        Will close ourselves before returning.
        """
        self._started_streaming.set()
        msg_stream = stream_transport_messages(
            self._transport,
            self._base_protocol,
            *self._protocols,
        )
        try:
            await self._handle_commands(msg_stream)
        except asyncio.TimeoutError as exc:
            self.logger.warning("Timed out waiting for command from %s, exiting...", self.remote)
            self.logger.debug("Timeout %r: %s", self, exc, exc_info=True)
        finally:
            await self.close()

    async def _handle_commands(
            self,
            msg_stream: AsyncIterator[Tuple[ProtocolAPI, CommandAPI[Any]]]) -> None:

        async for protocol, cmd in msg_stream:
            self._last_msg_time = time.monotonic()
            # track total number of messages received for each command type.
            self._msg_counts[type(cmd)] += 1

            queue = self._protocol_queues[type(protocol)]
            try:
                # We must use `put_nowait` here to ensure that in the event
                # that a single protocol queue is full that we don't block
                # other protocol messages getting through.
                queue.put_nowait(cmd)
            except asyncio.QueueFull:
                self.logger.error(
                    (
                        "Multiplexing queue for protocol '%s' full. "
                        "discarding message: %s"
                    ),
                    protocol,
                    cmd,
                )

    def cancel_streaming(self) -> None:
        if self._streaming_task is not None and not self._streaming_task.done():
            self._streaming_task.cancel()

    async def stop_streaming(self) -> None:
        self.cancel_streaming()
        try:
            await self._streaming_task
        except asyncio.CancelledError:
            pass
