import asyncio
import collections
import contextlib
import functools
import logging
import operator
from abc import (
    ABC,
    abstractmethod
)
from typing import (
    Any,
    cast,
    Dict,
    Iterator,
    List,
    NamedTuple,
    FrozenSet,
    Tuple,
    Type,
    TYPE_CHECKING,
)

from cached_property import cached_property

import rlp

from eth_utils import (
    to_tuple,
)

from eth_keys import datatypes

from cancel_token import CancelToken

from p2p import protocol
from p2p.kademlia import (
    Node,
)
from p2p.exceptions import (
    DecryptionError,
    HandshakeFailure,
    MalformedMessage,
    NoMatchingPeerCapabilities,
    PeerConnectionLost,
    RemoteDisconnected,
    TooManyPeersFailure,
    UnexpectedMessage,
    UnknownProtocolCommand,
)
from p2p.service import BaseService
from p2p._utils import (
    get_devp2p_cmd_id,
)
from p2p.p2p_proto import (
    Disconnect,
    DisconnectReason,
    Hello,
    P2PProtocol,
    Ping,
    Pong,
)
from p2p.transport import (
    Transport,
)

from .constants import (
    CONN_IDLE_TIMEOUT,
    SNAPPY_PROTOCOL_VERSION,
)

if TYPE_CHECKING:
    from p2p.peer_pool import BasePeerPool  # noqa: F401


async def handshake(remote: Node, factory: 'BasePeerFactory') -> 'BasePeer':
    """Perform the auth and P2P handshakes with the given remote.

    Return an instance of the given peer_class (must be a subclass of
    BasePeer) connected to that remote in case both handshakes are
    successful and at least one of the sub-protocols supported by
    peer_class is also supported by the remote.

    Raises UnreachablePeer if we cannot connect to the peer or
    HandshakeFailure if the remote disconnects before completing the
    handshake or if none of the sub-protocols supported by us is also
    supported by the remote.
    """
    transport = await Transport.open_connection(remote, factory.privkey)

    peer = factory.create_peer(transport, inbound=False)

    try:
        await peer.do_p2p_handshake()
        await peer.do_sub_proto_handshake()
    except Exception:
        # Note: This is one of two places where we manually handle closing the
        # reader/writer connection pair in the event of an error during the
        # peer connection and handshake process.
        # See `p2p.auth.handshake` for the other.
        transport.close()
        await asyncio.sleep(0)
        raise

    return peer


class BasePeerBootManager(BaseService):
    """
    The default boot manager does nothing, simply serving as a hook for other
    protocols which need to perform more complex boot check.
    """
    def __init__(self, peer: 'BasePeer') -> None:
        super().__init__(peer.cancel_token)
        self.peer = peer

    async def _run(self) -> None:
        pass


class BasePeerContext:
    pass


class BasePeer(BaseService):
    # Must be defined in subclasses. All items here must be Protocol classes representing
    # different versions of the same P2P sub-protocol (e.g. ETH, LES, etc).
    supported_sub_protocols: List[Type[protocol.Protocol]] = []
    # FIXME: Must be configurable.
    listen_port = 30303
    # Will be set upon the successful completion of a P2P handshake.
    sub_proto: protocol.Protocol = None
    disconnect_reason: DisconnectReason = None

    def __init__(self,
                 transport: Transport,
                 context: BasePeerContext,
                 inbound: bool = False,
                 token: CancelToken = None,
                 ) -> None:
        super().__init__(token)

        # Transport instance for network communications
        self.transport = transport

        # Any contextual information the peer may need.
        self.context = context

        # The self-identifying string that the remote names itself.
        self.client_version_string = ''

        # Initially while doing the handshake, the base protocol shouldn't support
        # snappy compression
        self.base_protocol = P2PProtocol(
            transport=self.transport,
            snappy_support=False,
            capabilities=self.capabilities,
            listen_port=self.listen_port,
        )

        # Flag indicating whether the connection this peer represents was
        # established from a dial-out or dial-in (True: dial-in, False:
        # dial-out)
        # TODO: rename to `dial_in` and have a computed property for `dial_out`
        self.inbound = inbound
        self._subscribers: List[PeerSubscriber] = []

        # A counter of the number of messages this peer has received for each
        # message type.
        self.received_msgs: Dict[protocol.Command, int] = collections.defaultdict(int)

        # Manages the boot process
        self.boot_manager = self.get_boot_manager()

    def __str__(self) -> str:
        return f"{self.__class__.__name__} {self.remote}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} {self.remote!r}"

    def __hash__(self) -> int:
        return hash(self.remote)

    #
    # Proxy Transport attributes
    #
    @cached_property
    def remote(self) -> Node:
        return self.transport.remote

    @property
    def is_closing(self) -> bool:
        return self.transport.is_closing

    def get_extra_stats(self) -> List[str]:
        return []

    @property
    def boot_manager_class(self) -> Type[BasePeerBootManager]:
        return BasePeerBootManager

    def get_boot_manager(self) -> BasePeerBootManager:
        return self.boot_manager_class(self)

    @abstractmethod
    async def send_sub_proto_handshake(self) -> None:
        pass

    @abstractmethod
    async def process_sub_proto_handshake(
            self, cmd: protocol.Command, msg: protocol.PayloadType) -> None:
        pass

    @contextlib.contextmanager
    def collect_sub_proto_messages(self) -> Iterator['MsgBuffer']:
        """
        Can be used to gather up all messages that are sent to the peer.
        """
        if not self.is_running:
            raise RuntimeError("Cannot collect messages if peer is not running")
        msg_buffer = MsgBuffer()

        with msg_buffer.subscribe_peer(self):
            yield msg_buffer

    @property
    def received_msgs_count(self) -> int:
        return sum(self.received_msgs.values())

    def add_subscriber(self, subscriber: 'PeerSubscriber') -> None:
        self._subscribers.append(subscriber)

    def remove_subscriber(self, subscriber: 'PeerSubscriber') -> None:
        if subscriber in self._subscribers:
            self._subscribers.remove(subscriber)

    async def do_sub_proto_handshake(self) -> None:
        """Perform the handshake for the sub-protocol agreed with the remote peer.

        Raises HandshakeFailure if the handshake is not successful.
        """
        await self.send_sub_proto_handshake()
        cmd, msg = await self.read_msg()
        if isinstance(cmd, Ping):
            # Parity sends a Ping before the sub-proto handshake, so respond to that and read the
            # next one, which hopefully will be the actual handshake.
            self.base_protocol.send_pong()
            cmd, msg = await self.read_msg()
        if isinstance(cmd, Disconnect):
            msg = cast(Dict[str, Any], msg)
            # Peers sometimes send a disconnect msg before they send the sub-proto handshake.
            if msg['reason'] == DisconnectReason.too_many_peers.value:
                raise TooManyPeersFailure(f'{self} disconnected from us before handshake')
            raise HandshakeFailure(
                f"{self} disconnected before completing sub-proto handshake: {msg['reason_name']}"
            )
        await self.process_sub_proto_handshake(cmd, msg)
        self.logger.debug("Finished %s handshake with %s", self.sub_proto, self.remote)

    async def do_p2p_handshake(self) -> None:
        """Perform the handshake for the P2P base protocol.

        Raises HandshakeFailure if the handshake is not successful.
        """
        self.base_protocol.send_handshake()

        try:
            cmd, msg = await self.read_msg()
        except rlp.DecodingError:
            raise HandshakeFailure("Got invalid rlp data during handshake")
        except MalformedMessage as e:
            raise HandshakeFailure("Got malformed message during handshake") from e

        if isinstance(cmd, Disconnect):
            msg = cast(Dict[str, Any], msg)
            # Peers sometimes send a disconnect msg before they send the initial P2P handshake.
            if msg['reason'] == DisconnectReason.too_many_peers.value:
                raise TooManyPeersFailure(f'{self} disconnected from us before handshake')
            raise HandshakeFailure(
                f"{self} disconnected before completing sub-proto handshake: {msg['reason_name']}"
            )
        await self.process_p2p_handshake(cmd, msg)

    @property
    def capabilities(self) -> Tuple[Tuple[str, int], ...]:
        return tuple((klass.name, klass.version) for klass in self.supported_sub_protocols)

    def get_protocol_command_for(self, msg: bytes) -> protocol.Command:
        """Return the Command corresponding to the cmd_id encoded in the given msg."""
        cmd_id = get_devp2p_cmd_id(msg)
        self.logger.debug2("Got msg with cmd_id: %s", cmd_id)
        if cmd_id < self.base_protocol.cmd_length:
            return self.base_protocol.cmd_by_id[cmd_id]
        elif cmd_id < self.sub_proto.cmd_id_offset + self.sub_proto.cmd_length:
            return self.sub_proto.cmd_by_id[cmd_id]
        else:
            raise UnknownProtocolCommand(f"No protocol found for cmd_id {cmd_id}")

    async def _cleanup(self) -> None:
        self.transport.close()

    async def _run(self) -> None:
        # The `boot` process is run in the background to allow the `run` loop
        # to continue so that all of the Peer APIs can be used within the
        # `boot` task.
        self.run_child_service(self.boot_manager)
        while self.is_operational:
            try:
                cmd, msg = await self.read_msg()
            except (PeerConnectionLost, TimeoutError) as err:
                self.logger.debug(
                    "%s stopped responding (%r), disconnecting", self.remote, err)
                return
            except DecryptionError as err:
                self.logger.warning(
                    "Unable to decrypt message from %s, disconnecting: %r",
                    self, err,
                    exc_info=True,
                )
                return
            except MalformedMessage as err:
                await self.disconnect(DisconnectReason.bad_protocol)
                return

            try:
                self.process_msg(cmd, msg)
            except RemoteDisconnected as e:
                self.logger.debug("%r disconnected: %s", self, e)
                return

    async def read_msg(self) -> Tuple[protocol.Command, protocol.PayloadType]:
        msg = await self.wait(self.transport.recv(), timeout=CONN_IDLE_TIMEOUT)
        cmd = self.get_protocol_command_for(msg)
        # NOTE: This used to be a bottleneck but it doesn't seem to be so anymore. If we notice
        # too much time is being spent on this again, we need to consider running this in a
        # ProcessPoolExecutor(). Need to make sure we don't use all CPUs in the machine for that,
        # though, otherwise asyncio's event loop can't run and we can't keep up with other peers.
        try:
            decoded_msg = cast(Dict[str, Any], cmd.decode(msg))
        except MalformedMessage as err:
            self.logger.debug(
                "Malformed message from peer %s: CMD:%s Error: %r",
                self, type(cmd).__name__, err,
            )
            raise
        else:
            self.logger.debug2("Successfully decoded %s msg: %s", cmd, decoded_msg)
            self.received_msgs[cmd] += 1
            return cmd, decoded_msg

    def handle_p2p_msg(self, cmd: protocol.Command, msg: protocol.PayloadType) -> None:
        """Handle the base protocol (P2P) messages."""
        if isinstance(cmd, Disconnect):
            msg = cast(Dict[str, Any], msg)
            raise RemoteDisconnected(msg['reason_name'])
        elif isinstance(cmd, Ping):
            self.base_protocol.send_pong()
        elif isinstance(cmd, Pong):
            # Currently we don't do anything when we get a pong, but eventually we should
            # update the last time we heard from a peer in our DB (which doesn't exist yet).
            pass
        else:
            raise UnexpectedMessage(f"Unexpected msg: {cmd} ({msg})")

    def handle_sub_proto_msg(self, cmd: protocol.Command, msg: protocol.PayloadType) -> None:
        cmd_type = type(cmd)

        if self._subscribers:
            was_added = tuple(
                subscriber.add_msg(PeerMessage(self, cmd, msg))
                for subscriber
                in self._subscribers
            )
            if not any(was_added):
                self.logger.warning(
                    "Peer %s has no subscribers for msg type %s",
                    self,
                    cmd_type.__name__,
                )
        else:
            self.logger.warning("Peer %s has no subscribers, discarding %s msg", self, cmd)

    def process_msg(self, cmd: protocol.Command, msg: protocol.PayloadType) -> None:
        if cmd.is_base_protocol:
            self.handle_p2p_msg(cmd, msg)
        else:
            self.handle_sub_proto_msg(cmd, msg)

    async def process_p2p_handshake(
            self, cmd: protocol.Command, msg: protocol.PayloadType) -> None:
        msg = cast(Dict[str, Any], msg)
        if not isinstance(cmd, Hello):
            await self.disconnect(DisconnectReason.bad_protocol)
            raise HandshakeFailure(f"Expected a Hello msg, got {cmd}, disconnecting")

        # limit number of chars to be displayed, and try to keep printable ones only
        # MAGIC 256: arbitrary, "should be enough for everybody"
        original_version = msg['client_version_string']
        client_version_string = original_version[:256] + ('...' if original_version[256:] else '')
        if client_version_string.isprintable():
            self.client_version_string = client_version_string.strip()
        else:
            self.client_version_string = repr(client_version_string)

        # Check whether to support Snappy Compression or not
        # based on other peer's p2p protocol version
        snappy_support = msg['version'] >= SNAPPY_PROTOCOL_VERSION

        if snappy_support:
            # Now update the base protocol to support snappy compression
            # This is needed so that Trinity is compatible with parity since
            # parity sends Ping immediately after Handshake
                self.base_protocol = P2PProtocol(
                    self.transport,
                    snappy_support=snappy_support,
                    capabilities=self.capabilities,
                    listen_port=self.listen_port,
                )

        remote_capabilities = msg['capabilities']
        try:
            self.sub_proto = self.select_sub_protocol(remote_capabilities, snappy_support)
        except NoMatchingPeerCapabilities:
            await self.disconnect(DisconnectReason.useless_peer)
            raise HandshakeFailure(
                f"No matching capabilities between us ({self.capabilities}) and {self.remote} "
                f"({remote_capabilities}), disconnecting"
            )

        self.logger.debug(
            "Finished P2P handshake with %s, using sub-protocol %s",
            self.remote, self.sub_proto)

    def _disconnect(self, reason: DisconnectReason) -> None:
        if not isinstance(reason, DisconnectReason):
            raise ValueError(
                f"Reason must be an item of DisconnectReason, got {reason}"
            )
        self.logger.debug("Disconnecting from remote peer %s; reason: %s", self.remote, reason.name)
        self.base_protocol.send_disconnect(reason.value)
        self.disconnect_reason = reason
        self.transport.close()

    async def disconnect(self, reason: DisconnectReason) -> None:
        """Send a disconnect msg to the remote node and stop this Peer.

        Also awaits for self.cancel() to ensure any pending tasks are cleaned up.

        :param reason: An item from the DisconnectReason enum.
        """
        self._disconnect(reason)
        if self.is_operational:
            await self.cancel()

    def disconnect_nowait(self, reason: DisconnectReason) -> None:
        """
        Non-coroutine version of `disconnect`
        """
        self._disconnect(reason)
        if self.is_operational:
            self.cancel_nowait()

    def select_sub_protocol(self,
                            remote_capabilities: List[Tuple[bytes, int]],
                            snappy_support: bool) -> protocol.Protocol:
        """Select the sub-protocol to use when talking to the remote.

        Find the highest version of our supported sub-protocols that is also supported by the
        remote and stores an instance of it (with the appropriate cmd_id offset) in
        self.sub_proto.

        Raises NoMatchingPeerCapabilities if none of our supported protocols match one of the
        remote's protocols.
        """
        matching_capabilities = set(self.capabilities).intersection(remote_capabilities)
        if not matching_capabilities:
            raise NoMatchingPeerCapabilities()
        _, highest_matching_version = max(matching_capabilities, key=operator.itemgetter(1))
        offset = self.base_protocol.cmd_length
        for proto_class in self.supported_sub_protocols:
            if proto_class.version == highest_matching_version:
                return proto_class(self.transport, offset, snappy_support)
        raise NoMatchingPeerCapabilities()


class PeerMessage(NamedTuple):
    peer: BasePeer
    command: protocol.Command
    payload: protocol.PayloadType


class PeerSubscriber(ABC):
    """
    Use the :class:`~p2p.peer.PeerSubscriber` class to subscribe to messages from all or specific
    peers.
    """
    _msg_queue: 'asyncio.Queue[PeerMessage]' = None

    @property
    @abstractmethod
    def subscription_msg_types(self) -> FrozenSet[Type[protocol.Command]]:
        """
        The :class:`p2p.protocol.Command` types that this class subscribes to. Any
        command which is not in this set will not be passed to this subscriber.

        The base command class :class:`p2p.protocol.Command` can be used to enable
        **all** command types.

        .. note: This API only applies to sub-protocol commands. Base protocol
        commands are handled exclusively at the peer level and cannot be
        consumed with this API.
        """
        pass

    @functools.lru_cache(maxsize=64)
    def is_subscription_command(self, cmd_type: Type[protocol.Command]) -> bool:
        return bool(self.subscription_msg_types.intersection(
            {cmd_type, protocol.Command}
        ))

    @property
    @abstractmethod
    def msg_queue_maxsize(self) -> int:
        """
        The max size of messages the underlying :meth:`msg_queue` can keep before it starts
        discarding new messages. Implementers need to overwrite this to specify the maximum size.
        """
        pass

    def register_peer(self, peer: BasePeer) -> None:
        """
        Notify about each registered peer in the :class:`~p2p.peer_pool.BasePeerPool`. Is called
        upon subscription for each :class:`~p2p.peer.BasePeer` that exists in the pool at that time
        and then for each :class:`~p2p.peer.BasePeer` that joins the pool later on.

        A :class:`~p2p.peer.PeerSubscriber` that wants to act upon peer registration needs to
        overwrite this method to provide an implementation.
        """
        pass

    def deregister_peer(self, peer: BasePeer) -> None:
        """
        Notify about each :class:`~p2p.peer.BasePeer` that is removed from the
        :class:`~p2p.peer_pool.BasePeerPool`.

        A :class:`~p2p.peer.PeerSubscriber` that wants to act upon peer deregistration needs to
        overwrite this method to provide an implementation.
        """
        pass

    @property
    def msg_queue(self) -> 'asyncio.Queue[PeerMessage]':
        """
        Return the ``asyncio.Queue[PeerMessage]`` that this subscriber uses to receive messages.
        """
        if self._msg_queue is None:
            self._msg_queue = asyncio.Queue(maxsize=self.msg_queue_maxsize)
        return self._msg_queue

    @property
    def queue_size(self) -> int:
        """
        Return the size of the :meth:`msg_queue`.
        """
        return self.msg_queue.qsize()

    def add_msg(self, msg: PeerMessage) -> bool:
        """
        Add a :class:`~p2p.peer.PeerMessage` to the subscriber.
        """
        peer, cmd, _ = msg

        if not self.is_subscription_command(type(cmd)):
            if hasattr(self, 'logger'):
                self.logger.debug2(  # type: ignore
                    "Discarding %s msg from %s; not subscribed to msg type; "
                    "subscriptions: %s",
                    cmd, peer, self.subscription_msg_types,
                )
            return False

        try:
            if hasattr(self, 'logger'):
                self.logger.debug2(  # type: ignore
                    "Adding %s msg from %s to queue; queue_size=%d", cmd, peer, self.queue_size)
            self.msg_queue.put_nowait(msg)
            return True
        except asyncio.queues.QueueFull:
            if hasattr(self, 'logger'):
                self.logger.warning(  # type: ignore
                    "%s msg queue is full; discarding %s msg from %s",
                    self.__class__.__name__, cmd, peer)
            return False

    @contextlib.contextmanager
    def subscribe(self, peer_pool: 'BasePeerPool') -> Iterator[None]:
        """
        Subscribe to all messages of the given :class:`~p2p.peer_pool.BasePeerPool`.
        Implementors need to call this API to start receiving messages from the pool.

        ::
            async def _run(self) -> None:
                with self.subscribe(self._peer_pool):
                    await self.cancellation()

        Once subscribed, messages can be consumed from the :meth:`msg_queue`.
        """

        peer_pool.subscribe(self)
        try:
            yield
        finally:
            peer_pool.unsubscribe(self)

    @contextlib.contextmanager
    def subscribe_peer(self, peer: BasePeer) -> Iterator[None]:
        """
        Subscribe to all messages of the given :class:`~p2p.peer.BasePeer`.
        Implementors need to call this API to start receiving messages from the peer.

        This API is similar to the :meth:`msg_queue` except that it only subscribes to the messages
        of a single peer.

        Once subscribed, messages can be consumed from the :meth:`msg_queue`.
        """
        peer.add_subscriber(self)
        try:
            yield
        finally:
            peer.remove_subscriber(self)


class MsgBuffer(PeerSubscriber):
    logger = logging.getLogger('p2p.peer.MsgBuffer')
    msg_queue_maxsize = 500
    subscription_msg_types = frozenset({protocol.Command})

    @to_tuple
    def get_messages(self) -> Iterator[PeerMessage]:
        while not self.msg_queue.empty():
            yield self.msg_queue.get_nowait()


class BasePeerFactory(ABC):
    @property
    @abstractmethod
    def peer_class(self) -> Type[BasePeer]:
        pass

    def __init__(self,
                 privkey: datatypes.PrivateKey,
                 context: BasePeerContext,
                 token: CancelToken) -> None:
        self.privkey = privkey
        self.context = context
        self.cancel_token = token

    def create_peer(self,
                    transport: Transport,
                    inbound: bool = False) -> BasePeer:
        return self.peer_class(
            transport=transport,
            context=self.context,
            inbound=inbound,
            token=self.cancel_token,
        )
