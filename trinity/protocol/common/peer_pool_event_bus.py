from abc import (
    abstractmethod,
)
import asyncio
from typing import (
    Any,
    AsyncIterator,
    Callable,
    cast,
    Dict,
    FrozenSet,
    Generic,
    Tuple,
    Type,
    TypeVar,
)

from async_service import Service

from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
    BroadcastConfig,
    EndpointAPI,
)

from p2p.abc import CommandAPI, SessionAPI
from p2p.exceptions import PeerConnectionLost
from p2p.peer import (
    BasePeer,
    PeerSubscriber,
)
from p2p.peer_pool import BasePeerPool

from trinity.constants import FIRE_AND_FORGET_BROADCASTING
from trinity._utils.logging import get_logger
from trinity._utils.decorators import async_suppress_exceptions

from .events import (
    ConnectToNodeCommand,
    DisconnectPeerEvent,
    GetConnectedPeersRequest,
    GetConnectedPeersResponse,
    GetProtocolCapabilitiesRequest,
    PeerCountRequest,
    PeerCountResponse,
    PeerJoinedEvent,
    PeerLeftEvent,
    ProtocolCapabilitiesResponse,
    PeerPoolMessageEvent,
)
from .peer import BaseProxyPeer


TPeer = TypeVar('TPeer', bound=BasePeer)
TEvent = TypeVar('TEvent', bound=BaseEvent)
TRequest = TypeVar('TRequest', bound=BaseRequestResponseEvent[Any])


async_fire_and_forget = async_suppress_exceptions(PeerConnectionLost, asyncio.TimeoutError)


class PeerPoolEventServer(Service, PeerSubscriber, Generic[TPeer]):
    """
    Base class to create a bridge between the ``PeerPool`` and the event bus so that peer
    messages become available to external processes (e.g. isolated components). In the opposite
    direction, other processes can also retrieve information or execute actions on the peer pool by
    sending specific events through the event bus that the ``PeerPoolEventServer`` answers.

    This class bridges all common APIs but protocol specific communication can be enabled through
    subclasses that add more handlers.
    """
    logger = get_logger('trinity.protocol.common.PeerPoolEventServer')

    msg_queue_maxsize: int = 2000

    subscription_msg_types: FrozenSet[Type[CommandAPI[Any]]] = frozenset({})

    def __init__(self,
                 event_bus: EndpointAPI,
                 peer_pool: BasePeerPool) -> None:
        self.peer_pool = peer_pool
        self.event_bus = event_bus

    async def run(self) -> None:
        self.run_daemon_event(
            DisconnectPeerEvent,
            lambda event: self.try_with_session(
                event.session,
                lambda peer: peer.disconnect_nowait(event.reason)
            )
        )

        self.manager.run_daemon_task(self.handle_peer_count_requests)
        self.manager.run_daemon_task(self.handle_connect_to_node_requests)
        self.manager.run_daemon_task(self.handle_native_peer_messages)
        self.manager.run_daemon_task(self.handle_get_connected_peers_requests)
        self.manager.run_daemon_task(self.handle_protocol_capabilities_requests)

        await self.manager.wait_finished()

    @async_fire_and_forget
    async def handle_send_command(self, event: PeerPoolMessageEvent) -> None:
        """
        Process any :class:`trinity.protocol.common.events.PeerPoolMessageEvent` by
        sending the wrapped command through the protocol of the corresponding session.
        """
        await self.try_with_session(
            event.session,
            lambda peer: peer.sub_proto.send(event.command)
        )

    def run_daemon_event(self,
                         event_type: Type[TEvent],
                         event_handler_fn: Callable[[TEvent], Any]) -> None:
        """
        Register a handler to be run every time that an event of type ``event_type`` appears.
        """
        self.manager.run_daemon_task(self.handle_stream, event_type, event_handler_fn)

    def run_daemon_request(
            self,
            event_type: Type[TRequest],
            event_handler_fn: Callable[[TRequest], Any]) -> None:
        """
        Register a handler to be run every time that an request of type ``event_type`` appears.
        """
        self.manager.run_daemon_task(self.handle_request_stream, event_type, event_handler_fn)

    @abstractmethod
    async def handle_native_peer_message(self,
                                         session: SessionAPI,
                                         cmd: CommandAPI[Any]) -> None:
        """
        Process every native peer message. Subclasses should overwrite this to forward specific
        peer messages on the event bus. The handler is called for every message that is defined in
        ``self.subscription_msg_types``.
        """
        ...

    def get_peer(self, session: SessionAPI) -> TPeer:
        """
        Look up and return a peer from the ``PeerPool`` that matches the given node.
        Raise ``PeerConnectionLost`` if the peer is no longer in the pool or is winding down.
        """
        try:
            peer = self.peer_pool.connected_nodes[session]
        except KeyError:
            self.logger.debug(
                "Peer with session %s does not exist in the pool anymore",
                session,
            )
            raise PeerConnectionLost(f"Peer at {session} is gone from the pool")
        else:
            if not peer.manager.is_running:
                self.logger.debug("Peer %s is not operational when selecting from pool", peer)
                raise PeerConnectionLost(f"{peer} is no longer operational")
            else:
                return cast(TPeer, peer)

    async def handle_connect_to_node_requests(self) -> None:
        async for command in self.event_bus.stream(ConnectToNodeCommand):
            self.logger.debug('Received request to connect to %s', command.remote)
            self.manager.run_task(self.peer_pool.connect_to_node, command.remote)

    async def handle_peer_count_requests(self) -> None:
        async for req in self.event_bus.stream(PeerCountRequest):
            await self.event_bus.broadcast(
                PeerCountResponse(len(self.peer_pool)),
                req.broadcast_config()
            )

    async def handle_get_connected_peers_requests(self) -> None:
        async for req in self.event_bus.stream(GetConnectedPeersRequest):
            await self.event_bus.broadcast(
                GetConnectedPeersResponse.from_connected_nodes(self.peer_pool.connected_nodes),
                req.broadcast_config()
            )

    async def handle_protocol_capabilities_requests(self) -> None:
        async for req in self.event_bus.stream(GetProtocolCapabilitiesRequest):
            protocols = await self.peer_pool.get_protocol_capabilities()
            await self.event_bus.broadcast(
                ProtocolCapabilitiesResponse(protocols),
                req.broadcast_config()
            )

    async def handle_stream(self,
                            event_type: Type[TEvent],
                            event_handler_fn: Callable[[TEvent], Any]) -> None:
        """
        Event handlers must not raise exceptions, they should all be handled internally,
        because there is no obvious place to forward the exception.
        """

        async for event in self.event_bus.stream(event_type):
            try:
                await event_handler_fn(event)
            except asyncio.CancelledError:
                # We need to catch and re-raise asyncio.CancelledError here because up until py37
                # it would be suppressed below.
                raise
            except Exception as exc:
                self.logger.exception(
                    "Suppressed uncaught exception to continue handling events: %s",
                    exc,
                )

    async def handle_request_stream(
            self,
            event_type: Type[TRequest],
            event_handler_fn: Callable[[TRequest], Any]) -> None:
        """
        Handlers may raise exceptions, which will be wrapped and sent in response.
        """

        async for event in self.event_bus.stream(event_type):
            try:
                self.logger.debug2("Replaying %s request on actual peer", event_type)
                val = await event_handler_fn(event)
            except Exception as e:
                await self.event_bus.broadcast(
                    event.expected_response_type()(None, e),
                    event.broadcast_config()
                )
            else:
                self.logger.debug2(
                    "Forwarding response to %s from peer to its proxy peer",
                    event_type,
                )
                await self.event_bus.broadcast(
                    event.expected_response_type()(val, None),
                    event.broadcast_config()
                )

    async def try_with_session(self, session: SessionAPI, fn: Callable[[TPeer], Any]) -> None:
        try:
            peer = self.get_peer(session)
        except PeerConnectionLost:
            pass
        else:
            fn(peer)

    async def with_node_and_timeout(self,
                                    session: SessionAPI,
                                    timeout: float,
                                    fn: Callable[[TPeer], Any]) -> Any:
        peer = self.get_peer(session)
        return await asyncio.wait_for(fn(peer), timeout=timeout)

    async def handle_native_peer_messages(self) -> None:
        with self.subscribe(self.peer_pool):
            while self.manager.is_running:
                peer, cmd = await self.msg_queue.get()
                await self.handle_native_peer_message(peer.session, cmd)

    def register_peer(self, peer: BasePeer) -> None:
        self.logger.debug2("Broadcasting PeerJoinedEvent for %s", peer)
        self.event_bus.broadcast_nowait(PeerJoinedEvent(peer.session), FIRE_AND_FORGET_BROADCASTING)

    def deregister_peer(self, peer: BasePeer) -> None:
        self.logger.debug2("Broadcasting PeerLeftEvent for %s", peer)
        self.event_bus.broadcast_nowait(PeerLeftEvent(peer.session), FIRE_AND_FORGET_BROADCASTING)


class DefaultPeerPoolEventServer(PeerPoolEventServer[BasePeer]):

    async def handle_native_peer_message(self,
                                         session: SessionAPI,
                                         cmd: CommandAPI[Any]) -> None:
        pass


TProxyPeer = TypeVar('TProxyPeer', bound=BaseProxyPeer)


class BaseProxyPeerPool(Service, Generic[TProxyPeer]):
    """
    Base class for peer pools that can be used from any process instead of the actual peer pool
    that runs in another process. Eventually, every process that needs to interact with the peer
    pool should be able to use a proxy peer pool for all peer pool interactions.
    """

    def __init__(self, event_bus: EndpointAPI, broadcast_config: BroadcastConfig) -> None:
        self.logger = get_logger('trinity.protocol.common.BaseProxyPeerPool')
        self.event_bus = event_bus
        self.broadcast_config = broadcast_config
        self.connected_peers: Dict[SessionAPI, TProxyPeer] = dict()

    async def stream_existing_and_joining_peers(self) -> AsyncIterator[TProxyPeer]:
        for proxy_peer in await self.get_peers():
            yield proxy_peer

        async for proxy_peer in self.stream_peers_joining():
            yield proxy_peer

    # TODO: PeerJoinedEvent/PeerLeftEvent should probably include a session id
    async def stream_peers_joining(self) -> AsyncIterator[TProxyPeer]:
        async for ev in self.event_bus.stream(PeerJoinedEvent):
            yield await self.ensure_proxy_peer(ev.session)

    async def handle_joining_peers(self) -> None:
        async for peer in self.stream_peers_joining():
            # We just want to consume the AsyncIterator
            self.logger.debug("New Proxy Peer joined %s", peer)

    async def handle_leaving_peers(self) -> None:
        async for ev in self.event_bus.stream(PeerLeftEvent):
            if ev.session not in self.connected_peers:
                self.logger.warning("Wanted to remove peer but it is missing %s", ev.session)
            else:
                proxy_peer = self.connected_peers.pop(ev.session)
                # TODO: Double check based on some session id if we are indeed
                # removing the right peer
                await proxy_peer.manager.stop()
                self.logger.debug("Removed proxy peer from proxy pool %s", ev.session)

    async def fetch_initial_peers(self) -> Tuple[TProxyPeer, ...]:
        response = await self.event_bus.request(GetConnectedPeersRequest(), self.broadcast_config)

        return tuple([
            await self.ensure_proxy_peer(peer_info.session)
            for peer_info
            in response.peers
        ])

    async def get_peers(self) -> Tuple[TProxyPeer, ...]:
        """
        Return proxy peer objects for all connected peers in the actual pool.
        """

        # The ProxyPeerPool could be started at any point in time after the actual peer pool.
        # Based on this assumption, if we don't have any proxy peers yet, sync with the actual pool
        # first. From that point on, the proxy peer pool will maintain the set of proxies based on
        # the events of incoming / leaving peers.
        if not any(self.connected_peers):
            await self.fetch_initial_peers()
        return tuple(self.connected_peers.values())

    @abstractmethod
    def convert_session_to_proxy_peer(self,
                                      session: SessionAPI,
                                      event_bus: EndpointAPI,
                                      broadcast_config: BroadcastConfig) -> TProxyPeer:
        ...

    async def ensure_proxy_peer(self, session: SessionAPI) -> TProxyPeer:

        if session not in self.connected_peers:
            proxy_peer = self.convert_session_to_proxy_peer(
                session,
                self.event_bus,
                self.broadcast_config
            )
            self.connected_peers[session] = proxy_peer
            self.manager.run_child_service(proxy_peer)
            await proxy_peer.manager.wait_started()

        return self.connected_peers[session]

    async def run(self) -> None:
        self.manager.run_daemon_task(self.handle_joining_peers)
        self.manager.run_daemon_task(self.handle_leaving_peers)
        await self.manager.wait_finished()
