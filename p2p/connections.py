import asyncio
import collections
import functools
from typing import Any, Awaitable, Callable, DefaultDict, Set, Tuple, TypeVar, Type

from p2p.abc import MultiplexerAPI
from p2p.exceptions import (
    UnknownProtocolCommand,
)
from p2p.handshake import (
    DevP2PReceipt,
    HandshakeReceipt,
)
from p2p.kademlia import Node
from p2p.service import BaseService
from p2p.p2p_proto import (
    DisconnectReason,
    P2PProtocol,
)
from p2p.protocol import (
    Command,
    Capabilities,
    Payload,
    Protocol,
)


TProtocol = TypeVar('TProtocol', bound=Protocol)
TReceipt = TypeVar('TReceipt', bound=HandshakeReceipt)


class HandlerSubscription:
    def __init__(self, remove_fn: Callable[[], Any]) -> None:
        self._remove_fn = remove_fn

    def cancel(self) -> None:
        self._remove_fn()


class Connection(BaseService):
    disconnect_reason: DisconnectReason = None

    _protocol_handlers: DefaultDict[str, Set[Callable[[Command, Payload], Awaitable[Any]]]]
    _command_handlers: DefaultDict[Type[Command], Set[Callable[[Payload], Awaitable[Any]]]]

    def __init__(self,
                 multiplexer: MultiplexerAPI,
                 devp2p_receipt: DevP2PReceipt,
                 protocol_receipts: Tuple[HandshakeReceipt, ...],
                 is_dial_out: bool) -> None:
        super().__init__(token=multiplexer.cancel_token, loop=multiplexer.cancel_token.loop)
        self._multiplexer = multiplexer
        self._devp2p_receipt = devp2p_receipt
        self._protocol_receipts = protocol_receipts
        self.is_dial_out = is_dial_out

        self._protocol_handlers = collections.defaultdict(set)
        self._command_handlers = collections.defaultdict(set)

        # An event that controls when the connection will start reading from
        # the individual multiplexed protocol streams and feeding handlers.
        # This ensures that the connection does not start consuming messages
        # before all necessary handlers have been added
        self._handlers_ready = asyncio.Event()

    def start_protocol_streams(self):
        self._handlers_ready.set()

    #
    # Primary properties of the connection
    #
    @property
    def is_dial_in(self) -> bool:
        return not self.is_dial_out

    @property
    def remote(self) -> Node:
        return self._multiplexer.remote

    async def _run(self) -> None:
        async with self._multiplexer.multiplex():
            for protocol in self._multiplexer.get_protocols():
                self.run_daemon_task(self._feed_protocol_handlers(protocol))

            await self.cancellation()

    #
    # Subscriptions/Handler API
    #
    async def _feed_protocol_handlers(self, protocol: Protocol) -> None:
        # do not start consuming from the protocol stream until the h
        try:
            await asyncio.wait_for(self._handlers_ready.wait(), timeout=10)
        except TimeoutError:
            raise TimeoutError(
                "The handlers ready event was never set.  Ensure that "
                "`Connection.start_protocol_streams()` is being called"
            )

        # we don't need to use wait_iter here because the multiplexer does it
        # for us.
        async for cmd, msg in self._multiplexer.stream_protocol_messages(protocol):
            self.logger.debug('Handling command: %s', type(cmd))
            # local copy to prevent multation while iterating
            protocol_handlers = set(self._protocol_handlers[protocol.name])
            for proto_handler_fn in protocol_handlers:
                self.logger.debug(
                    'Running protocol handler %s for protocol=%s command=%s',
                    proto_handler_fn,
                    protocol,
                    type(cmd),
                )
                self.run_task(proto_handler_fn(cmd, msg))
            command_handlers = set(self._command_handlers[type(cmd)])
            for cmd_handler_fn in command_handlers:
                self.logger.debug(
                    'Running command handler %s for protocol=%s command=%s',
                    cmd_handler_fn,
                    protocol,
                    type(cmd),
                )
                self.run_task(cmd_handler_fn(msg))

    def add_protocol_handler(self,
                             protocol_name: str,
                             handler_fn: Callable[[Command, Payload], Awaitable[Any]],
                             ) -> HandlerSubscription:
        protocol = self._multiplexer.get_protocol_by_name(protocol_name)
        self._protocol_handlers[protocol.name].add(handler_fn)
        remove_fn = functools.partial(
            self._protocol_handlers[protocol.name].remove,
            handler_fn,
        )
        return HandlerSubscription(remove_fn)

    def add_command_handler(self,
                            command_type: Type[Command],
                            handler_fn: Callable[[Payload], Awaitable[Any]],
                            ) -> HandlerSubscription:
        for protocol in self._multiplexer.get_protocols():
            if protocol.supports_command(command_type):
                self._command_handlers[command_type].add(handler_fn)
                remove_fn = functools.partial(
                    self._command_handlers[command_type].remove,
                    handler_fn,
                )
                return HandlerSubscription(remove_fn)
        else:
            raise UnknownProtocolCommand(
                f"Command {command_type} was not found in the connected "
                f"protocols: {self._multiplexer.get_protocols()}"
            )

    #
    # Access to underlying Multiplexer
    #
    def get_multiplexer(self) -> MultiplexerAPI:
        return self._multiplexer

    #
    # Base Protocol shortcuts
    #
    @property
    def base_protocol(self) -> P2PProtocol:
        return self._multiplexer.get_base_protocol()

    @property
    def remote_capabilities(self) -> Capabilities:
        return self._devp2p_receipt.capabilities

    @property
    def p2p_version(self) -> int:
        return self._devp2p_receipt.version

    @property
    def client_version_string(self) -> str:
        return self._devp2p_receipt.client_version_string

    @property
    def safe_client_version_string(self) -> str:
        # limit number of chars to be displayed, and try to keep printable ones only
        # MAGIC 256: arbitrary, "should be enough for everybody"
        if len(self.client_version_string) <= 256:
            return self.client_version_string

        truncated_client_version_string = self.client_version_string[:256] + '...'
        if truncated_client_version_string.isprintable():
            return truncated_client_version_string
        else:
            return repr(truncated_client_version_string)

    async def disconnect(self, reason: DisconnectReason) -> None:
        if not isinstance(reason, DisconnectReason):
            raise ValueError(
                f"Reason must be an item of DisconnectReason, got {reason}"
            )

        self.logger.debug("Disconnecting from remote peer %s; reason: %s", self.remote, reason.name)
        self.base_protocol.send_disconnect(reason.value)
        self.disconnect_reason = reason
        self._multiplexer.close()
        await self.cancel()
