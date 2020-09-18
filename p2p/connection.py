import asyncio
import collections
import contextlib
import functools
from typing import (
    Any,
    DefaultDict,
    Dict,
    List,
    Sequence,
    Set,
    Tuple,
    Type,
    TYPE_CHECKING,
    Union,
)

from async_service import Service
from async_service.asyncio import cleanup_tasks

from cached_property import cached_property

from eth_keys import keys

from p2p.abc import (
    BehaviorAPI,
    CommandAPI,
    ConnectionAPI,
    HandlerFn,
    HandshakeReceiptAPI,
    LogicAPI,
    MultiplexerAPI,
    NodeAPI,
    ProtocolAPI,
    SessionAPI,
    SubscriptionAPI,
    THandshakeReceipt,
    TLogic,
    TProtocol,
)
from p2p.constants import PEER_READY_TIMEOUT
from p2p.disconnect import DisconnectReason
from p2p.exceptions import (
    DuplicateAPI,
    MalformedMessage,
    PeerConnectionLost,
    ReceiptNotFound,
    UnknownAPI,
    UnknownProtocol,
    UnknownProtocolCommand,
)
from p2p.asyncio_utils import create_task, wait_first
from p2p.subscription import Subscription
from p2p.p2p_proto import BaseP2PProtocol, DevP2PReceipt, Disconnect
from p2p.typing import Capabilities
from p2p._utils import get_logger

if TYPE_CHECKING:
    from p2p.peer import BasePeer  # noqa: F401


class Connection(ConnectionAPI, Service):
    _protocol_handlers: DefaultDict[
        Type[ProtocolAPI],
        Set[HandlerFn]
    ]
    _msg_handlers: Set[HandlerFn]
    _command_handlers: DefaultDict[
        Type[CommandAPI[Any]],
        Set[HandlerFn]
    ]
    _logics: Dict[str, LogicAPI]

    def __init__(self,
                 multiplexer: MultiplexerAPI,
                 devp2p_receipt: DevP2PReceipt,
                 protocol_receipts: Sequence[HandshakeReceiptAPI],
                 is_dial_out: bool) -> None:
        self.logger = get_logger('p2p.connection.Connection')
        # The multiplexer passed to us will have been started when performing the handshake, so it
        # is already reading messages from the transport and storing them in per-protocol queues.
        self._multiplexer = multiplexer
        # Stop early in case the multiplexer is no longer streaming.
        self._multiplexer.raise_if_streaming_error()
        self._devp2p_receipt = devp2p_receipt
        self.protocol_receipts = tuple(protocol_receipts)
        self.is_dial_out = is_dial_out

        self._protocol_handlers = collections.defaultdict(set)
        self._command_handlers = collections.defaultdict(set)
        self._msg_handlers = set()

        # An event that controls when the connection will start reading from
        # the individual multiplexed protocol streams and feeding handlers.
        # This ensures that the connection does not start consuming messages
        # before all necessary handlers have been added
        self._handlers_ready = asyncio.Event()

        self.behaviors_applied = asyncio.Event()

        self._logics = {}

    def __str__(self) -> str:
        return f"Connection-{self.session}"

    def __repr__(self) -> str:
        return f"<Connection {self.session!r} {self._multiplexer!r} dial_out={self.is_dial_out}>"

    @property
    def is_streaming_messages(self) -> bool:
        return self._handlers_ready.is_set()

    def start_protocol_streams(self) -> None:
        self._handlers_ready.set()

    async def run_behaviors(self, behaviors: Tuple[BehaviorAPI, ...]) -> None:
        async with contextlib.AsyncExitStack() as stack:
            futures: List[asyncio.Task[Any]] = [
                create_task(self.manager.wait_finished(), 'Connection/run_behaviors/wait_finished')]
            for behavior in behaviors:
                if behavior.should_apply_to(self):
                    behavior_exit = await stack.enter_async_context(behavior.apply(self))
                    futures.append(behavior_exit)

            self.behaviors_applied.set()
            # If wait_first() is called, cleanup_tasks() will be a no-op, but if any post_apply()
            # calls raise an exception, it will ensure we don't leak pending tasks that would
            # cause asyncio to complain.
            async with cleanup_tasks(*futures):
                try:
                    for behavior in behaviors:
                        behavior.post_apply()
                    await wait_first(futures, max_wait_after_cancellation=2)
                except PeerConnectionLost:
                    # Any of our behaviors may propagate a PeerConnectionLost, which is to be
                    # expected as many Connection APIs used by them can raise that. To avoid a
                    # DaemonTaskExit since we're returning silently, ensure we're cancelled.
                    self.manager.cancel()

    async def run_peer(self, peer: 'BasePeer') -> None:
        """
        Run the peer as a child service.

        A peer must always run as a child of the connection so that it has an open connection
        until it finishes its cleanup.
        """
        self.manager.run_daemon_task(self.run_behaviors, peer.get_behaviors())
        await self.behaviors_applied.wait()

        self.manager.run_daemon_child_service(peer)
        await asyncio.wait_for(peer.manager.wait_started(), timeout=PEER_READY_TIMEOUT)
        await asyncio.wait_for(peer.ready.wait(), timeout=PEER_READY_TIMEOUT)

    #
    # Primary properties of the connection
    #
    @cached_property
    def is_dial_in(self) -> bool:
        return not self.is_dial_out

    @cached_property
    def remote(self) -> NodeAPI:
        return self._multiplexer.remote

    @cached_property
    def session(self) -> SessionAPI:
        return self._multiplexer.session

    @property
    def is_alive(self) -> bool:
        return self.manager.is_running and not self._multiplexer.is_closing

    def __del__(self) -> None:
        # This is necessary because the multiplexer passed to our constructor will be streaming,
        # and if for some reason our run() method is not called, we'd leave the multiplexer
        # streaming indefinitely. We might still get asyncio warnings (about a task being destroyed
        # while still pending) if that happens, but this is the best we can do.
        self._multiplexer.cancel_streaming()

    async def run(self) -> None:
        # Our multiplexer will already be streaming in the background (as it was used during
        # handshake), so we do this to ensure we only start if it is still running.
        self._multiplexer.raise_if_streaming_error()

        for protocol in self._multiplexer.get_protocols():
            self.manager.run_daemon_task(self._feed_protocol_handlers, protocol)

        try:
            await self._multiplexer.wait_streaming_finished()
        except PeerConnectionLost:
            pass
        except MalformedMessage as err:
            self.logger.debug(
                "Disconnecting peer %s for sending MalformedMessage: %s",
                self.remote,
                err,
                exc_info=True,
            )
            try:
                self.get_base_protocol().send(Disconnect(DisconnectReason.BAD_PROTOCOL))
            except PeerConnectionLost:
                self.logger.debug(
                    "%s went away while trying to disconnect for MalformedMessage",
                    self,
                )
        finally:
            self.manager.cancel()

    #
    # Subscriptions/Handler API
    #
    async def _feed_protocol_handlers(self, protocol: ProtocolAPI) -> None:
        # do not start consuming from the protocol stream until
        # `start_protocol_streams` has been called and the multiplexer is
        # active.
        try:
            await asyncio.wait_for(self._handlers_ready.wait(), timeout=10)
        except asyncio.TimeoutError as err:
            self.logger.warning('Timedout waiting for handler ready signal')
            raise asyncio.TimeoutError(
                "The handlers ready event was never set.  Ensure that "
                "`Connection.start_protocol_streams()` is being called"
            ) from err

        async for cmd in self._multiplexer.stream_protocol_messages(protocol):
            self.logger.debug2('Handling command: %s', type(cmd))
            # local copy to prevent multation while iterating
            protocol_handlers = set(self._protocol_handlers[type(protocol)])
            for proto_handler_fn in protocol_handlers:
                self.logger.debug2(
                    'Running protocol handler %s for protocol=%s command=%s',
                    proto_handler_fn,
                    protocol,
                    type(cmd),
                )
                self.manager.run_task(proto_handler_fn, self, cmd)
            command_handlers = set(self._command_handlers[type(cmd)])
            command_handlers.update(self._msg_handlers)
            for cmd_handler_fn in command_handlers:
                self.logger.debug2(
                    'Running command handler %s for protocol=%s command=%s',
                    cmd_handler_fn,
                    protocol,
                    type(cmd),
                )
                self.manager.run_task(cmd_handler_fn, self, cmd)

        # XXX: This ugliness is needed because Multiplexer.stream_protocol_messages() stops as
        # soon as the transport is closed, and that may happen immediately after we received a
        # Disconnect+EOF from a remote, but before we've had a chance to process the disconnect,
        # which would cause a DaemonTaskExit error
        # (https://github.com/ethereum/trinity/issues/1733).
        if self._multiplexer.is_closing and not self.manager.is_cancelled:
            try:
                await asyncio.wait_for(self.manager.wait_finished(), timeout=2)
            except asyncio.TimeoutError:
                if not self.manager.is_cancelled:
                    self.logger.error(
                        "stream_protocol_messages() terminated but %s was never cancelled, "
                        "this will cause the Connection to crash with a DaemonTaskExit", self)

    def add_protocol_handler(self,
                             protocol_class: Type[ProtocolAPI],
                             handler_fn: HandlerFn,
                             ) -> SubscriptionAPI:
        if not self._multiplexer.has_protocol(protocol_class):
            raise UnknownProtocol(
                f"Protocol {protocol_class} was not found int he connected "
                f"protocols: {self._multiplexer.get_protocols()}"
            )
        self._protocol_handlers[protocol_class].add(handler_fn)
        cancel_fn = functools.partial(
            self._protocol_handlers[protocol_class].remove,
            handler_fn,
        )
        return Subscription(cancel_fn)

    def add_msg_handler(self, handler_fn: HandlerFn) -> SubscriptionAPI:
        self._msg_handlers.add(handler_fn)
        cancel_fn = functools.partial(self._msg_handlers.remove, handler_fn)
        return Subscription(cancel_fn)

    def add_command_handler(self,
                            command_type: Type[CommandAPI[Any]],
                            handler_fn: HandlerFn,
                            ) -> SubscriptionAPI:
        for protocol in self._multiplexer.get_protocols():
            if protocol.supports_command(command_type):
                self._command_handlers[command_type].add(handler_fn)
                cancel_fn = functools.partial(
                    self._command_handlers[command_type].remove,
                    handler_fn,
                )
                return Subscription(cancel_fn)
        else:
            raise UnknownProtocolCommand(
                f"Command {command_type} was not found in the connected "
                f"protocols: {self._multiplexer.get_protocols()}"
            )

    #
    # API extension
    #
    def add_logic(self, name: str, logic: LogicAPI) -> SubscriptionAPI:
        if name in self._logics:
            raise DuplicateAPI(
                f"There is already an API registered under the name '{name}': "
                f"{self._logics[name]}"
            )
        self.logger.debug("Adding '%s' logic to %s", name, self)
        self._logics[name] = logic
        cancel_fn = functools.partial(self.remove_logic, name)
        return Subscription(cancel_fn)

    def remove_logic(self, name: str) -> None:
        self.logger.debug("Removing '%s' logic from %s", name, self)
        self._logics.pop(name)

    def has_logic(self, name: str) -> bool:
        if not self.is_alive:
            # This is a safety net, really, as the Peer should never call this if it is no longer
            # alive.
            raise PeerConnectionLost("Cannot look up subprotocol when connection is not alive")
        return name in self._logics

    def get_logic(self, name: str, logic_type: Type[TLogic]) -> TLogic:
        if not self.has_logic(name):
            raise UnknownAPI(
                f"No '{name}' logic registered on {self}. "
                f"Registered ones are: {self._logics.keys()} ")
        logic = self._logics[name]
        if isinstance(logic, logic_type):
            return logic
        else:
            raise TypeError(
                f"Wrong logic type.  expected: {logic_type}  got: {type(logic)}"
            )

    #
    # Access to underlying Multiplexer
    #
    def get_multiplexer(self) -> MultiplexerAPI:
        return self._multiplexer

    #
    # Base Protocol shortcuts
    #
    def get_base_protocol(self) -> BaseP2PProtocol:
        return self._multiplexer.get_base_protocol()

    def get_p2p_receipt(self) -> DevP2PReceipt:
        return self._devp2p_receipt

    #
    # Protocol APIS
    #
    def has_protocol(self, protocol_identifier: Union[ProtocolAPI, Type[ProtocolAPI]]) -> bool:
        return self._multiplexer.has_protocol(protocol_identifier)

    def get_protocols(self) -> Tuple[ProtocolAPI, ...]:
        return self._multiplexer.get_protocols()

    def get_protocol_by_type(self, protocol_type: Type[TProtocol]) -> TProtocol:
        return self._multiplexer.get_protocol_by_type(protocol_type)

    def get_protocol_for_command_type(self, command_type: Type[CommandAPI[Any]]) -> ProtocolAPI:
        return self._multiplexer.get_protocol_for_command_type(command_type)

    def get_receipt_by_type(self, receipt_type: Type[THandshakeReceipt]) -> THandshakeReceipt:
        for receipt in self.protocol_receipts:
            if isinstance(receipt, receipt_type):
                return receipt
        else:
            raise ReceiptNotFound(f"Receipt not found: {receipt_type}")

    #
    # Connection Metadata
    #
    @cached_property
    def remote_capabilities(self) -> Capabilities:
        return self._devp2p_receipt.capabilities

    @cached_property
    def remote_p2p_version(self) -> int:
        return self._devp2p_receipt.version

    @cached_property
    def negotiated_p2p_version(self) -> int:
        return self.get_base_protocol().version

    @cached_property
    def remote_public_key(self) -> keys.PublicKey:
        return keys.PublicKey(self._devp2p_receipt.remote_public_key)

    @cached_property
    def client_version_string(self) -> str:
        return self._devp2p_receipt.client_version_string

    @cached_property
    def safe_client_version_string(self) -> str:
        # limit number of chars to be displayed, and try to keep printable ones only
        # MAGIC 256: arbitrary, "should be enough for everybody"
        if len(self.client_version_string) <= 256:
            return self.client_version_string

        truncated_client_version_string = self.client_version_string[:253] + '...'
        if truncated_client_version_string.isprintable():
            return truncated_client_version_string
        else:
            return repr(truncated_client_version_string)
