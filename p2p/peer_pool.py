from abc import (
    abstractmethod,
)
import asyncio
import operator
from typing import (
    Any,
    AsyncIterator,
    AsyncIterable,
    Awaitable,
    BehaviorAPI,
    Callable,
    cast,
    Dict,
    Iterator,
    List,
    NamedTuple,
    Tuple,
    Type,
)

from cancel_token import (
    CancelToken,
    OperationCancelled,
)
from eth_utils import (
    clamp,
    humanize_seconds,
)
from eth_utils.toolz import (
    groupby,
    take,
)
from lahja import (
    BroadcastConfig,
    EndpointAPI,
)

from p2p.abc import (
    AsyncioServiceAPI,
    CandidateFilterFn,
    OnConnectFn,
    OnDisconnectFn,
    PeerProviderFn,
    PoolManagerAPI,
    NodeAPI,
)
from p2p.constants import (
    DEFAULT_MAX_PEERS,
    DEFAULT_PEER_BOOT_TIMEOUT,
    DISCOVERY_EVENTBUS_ENDPOINT,
    MAX_CONCURRENT_CONNECTION_ATTEMPTS,
    MAX_SEQUENTIAL_PEER_CONNECT,
    PEER_CONNECT_INTERVAL,
    REQUEST_PEER_CANDIDATE_TIMEOUT,
)
from p2p.exceptions import (
    BaseP2PError,
    IneligiblePeer,
    BadAckMessage,
    HandshakeFailure,
    MalformedMessage,
    NoMatchingPeerCapabilities,
    PeerConnectionLost,
    UnreachablePeer,
)
from p2p.peer import (
    BasePeer,
    BasePeerFactory,
    BasePeerContext,
    PeerMessage,
    PeerSubscriber,
)
from p2p.peer_backend import (
    BasePeerBackend,
    DiscoveryPeerBackend,
    BootnodesPeerBackend,
)
from p2p.disconnect import (
    DisconnectReason,
)
from p2p.resource_lock import (
    ResourceLock,
)
from p2p.service import (
    BaseService,
)
from p2p.token_bucket import TokenBucket
from p2p.tracking.connection import (
    BaseConnectionTracker,
    NoopConnectionTracker,
)


TO_DISCOVERY_BROADCAST_CONFIG = BroadcastConfig(filter_endpoint=DISCOVERY_EVENTBUS_ENDPOINT)


COMMON_PEER_CONNECTION_EXCEPTIONS = cast(Tuple[Type[BaseP2PError], ...], (
    NoMatchingPeerCapabilities,
    PeerConnectionLost,
    TimeoutError,
    UnreachablePeer,
))

# This should contain all exceptions that should not propogate during a
# standard attempt to connect to a peer.
ALLOWED_PEER_CONNECTION_EXCEPTIONS = cast(Tuple[Type[BaseP2PError], ...], (
    IneligiblePeer,
    BadAckMessage,
    MalformedMessage,
    HandshakeFailure,
)) + COMMON_PEER_CONNECTION_EXCEPTIONS


class ManagerLogic(NamedTuple):
    behaviors: Tuple[BehaviorAPI, ...]


class InboundConfig(NamedTuple):
    host: str
    port: int


class OutboundConfig(NamedTuple):
    providers: Tuple[PeerProviderFn, ...]
    candidate_filters: Tuple[CandidateFilterFn, ...]
    sleep_fn: Callable[['PoolManager'], Awaitable[Any]]


class PeerPool(BaseService, AsyncIterable[BasePeer]):
    """
    PeerPool maintains connections to up-to max_peers on a given network.
    """
    _report_interval = 60
    _peer_boot_timeout = DEFAULT_PEER_BOOT_TIMEOUT
    _event_bus: EndpointAPI = None

    def __init__(self,
                 manager: PoolManagerAPI,
                 behaviors: Sequence[BehaviorAPI] = (),
                 inbound_config: Optional[InboundConfig] = None,
                 outbound_config: Optional[OutboundConfig] = None,
                 event_bus: EndpointAPI = None,
                 ) -> None:
        super().__init__(manager.cancel_token)

        self._manager = manager
        self._behaviors = behaviors
        self._inbound_config = inbound_config
        self._outbound_config = outbound_config

        self._subscribers: List[PeerSubscriber] = []
        self._event_bus = event_bus

    @property
    def has_event_bus(self) -> bool:
        return self._event_bus is not None

    def get_event_bus(self) -> EndpointAPI:
        if self._event_bus is None:
            raise AttributeError("No event bus configured for this peer pool")
        return self._event_bus

    def __len__(self) -> int:
        return len(self.manager.pool)

    @property
    @abstractmethod
    def peer_factory_class(self) -> Type[BasePeerFactory]:
        ...

    def get_peer_factory(self) -> BasePeerFactory:
        return self.peer_factory_class(
            privkey=self.privkey,
            context=self.context,
            event_bus=self._event_bus,
            token=self.cancel_token,
        )

    @property
    def is_full(self) -> bool:
        return len(self) >= self.max_peers

    def subscribe(self, subscriber: PeerSubscriber) -> None:
        self._subscribers.append(subscriber)
        for peer in self.connected_nodes.values():
            subscriber.register_peer(peer)
            peer.add_subscriber(subscriber)

    def unsubscribe(self, subscriber: PeerSubscriber) -> None:
        if subscriber in self._subscribers:
            self._subscribers.remove(subscriber)
        for peer in self.connected_nodes.values():
            peer.remove_subscriber(subscriber)

    async def _run(self) -> None:
        self.run_daemon_task(self._do_listen)
        self.run_daemon_task(self._do_seek_connections)

        # TODO: hook into on_connect to start `Peer`
        # TODO: hook into on_disconnect to stop `Peer`

        # TODO: Move to behavior....
        self.run_daemon_task(self._periodically_report_stats())
        try:
            await self.cancellation()
        finally:
            await self.stop_all_connections()

    async def _do_listen(self) -> None:
        if self.inbound_config is not None:
            async with self._manager.listent(self.inbound_config.host, self.inbound_config.port) as me:
                awaite self.cancellation()

    async def _do_seek_connections(self):
        if self.outbound_config is not None:
            await self._manager.seek_connections(
                providers=outbound_config.providers,
                candidate_filters=outbound_config.candidate_filters,
                sleep_fn=outbound_config.sleep_fn,
            )
            async with self._manager.listent(self.inbound_config.host, self.inbound_config.port) as me:
                awaite self.cancellation()

    async def stop_all_connections(self) -> None:
        self.logger.info("Stopping all peers ...")
        peers = self._manager.pool
        disconnections = (
            connection.get_base_protocol().send_disconnect(DisconnectReason.client_quitting)
            for connection in self._manager.pool
        )
        await asyncio.gather(*disconnections)

    def _peer_finished(self, peer: AsyncioServiceAPI) -> None:
        """
        Remove the given peer from our list of connected nodes.
        This is passed as a callback to be called when a peer finishes.
        """
        peer = cast(BasePeer, peer)
        if peer.remote in self.connected_nodes:
            self.logger.info("%s finished[%s], removing from pool", peer, peer.disconnect_reason)
            self.connected_nodes.pop(peer.remote)
        else:
            self.logger.warning(
                "%s finished but was not found in connected_nodes (%s)",
                peer,
                tuple(self.connected_nodes.values()),
            )

        for subscriber in self._subscribers:
            subscriber.deregister_peer(peer)

    async def __aiter__(self) -> AsyncIterator[BasePeer]:
        for connection in tuple(self.manager.pool):
        for peer in tuple(self.connected_nodes.values()):
            # Yield control to ensure we process any disconnection requests from peers. Otherwise
            # we could return peers that should have been disconnected already.
            await asyncio.sleep(0)
            if peer.is_operational and not peer.is_closing:
                yield peer

    async def _periodically_report_stats(self) -> None:
        while self.is_operational:
            inbound_peers = len(
                [peer for peer in self.connected_nodes.values() if peer.inbound])
            self.logger.info("Connected peers: %d inbound, %d outbound",
                             inbound_peers, (len(self.connected_nodes) - inbound_peers))
            subscribers = len(self._subscribers)
            if subscribers:
                longest_queue = max(
                    self._subscribers, key=operator.attrgetter('queue_size'))
                self.logger.debug(
                    "Peer subscribers: %d, longest queue: %s(%d)",
                    subscribers, longest_queue.__class__.__name__, longest_queue.queue_size)

            self.logger.debug("== Peer details == ")
            # make a copy, because we might edit the original during iteration
            peers = tuple(self.connected_nodes.values())
            for peer in peers:
                if not peer.is_running:
                    self.logger.warning(
                        "%s is no longer alive but had not been removed from pool", peer)
                    self._peer_finished(peer)
                    continue
                self.logger.debug(
                    "%s: uptime=%s, received_msgs=%d",
                    peer,
                    humanize_seconds(peer.uptime),
                    peer.received_msgs_count,
                )
                self.logger.debug("client_version_string='%s'", peer.client_version_string)
                for line in peer.get_extra_stats():
                    self.logger.debug("    %s", line)
            self.logger.debug("== End peer details == ")
            await self.sleep(self._report_interval)
