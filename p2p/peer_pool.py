from abc import abstractmethod
import asyncio
import functools
import operator
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    AsyncIterable,
    Callable,
    cast,
    Dict,
    List,
    Sequence,
    Tuple,
    Type,
)

from async_service import Service

from cached_property import cached_property

from eth_keys import keys
from eth_typing import NodeID
from eth_utils import (
    clamp,
    humanize_seconds,
)
from eth_utils.toolz import (
    groupby,
    take,
)
from lahja import (
    EndpointAPI,
)
from pyformance import MetricsRegistry

from p2p.abc import NodeAPI, SessionAPI
from p2p.asyncio_utils import create_task
from p2p.constants import (
    DEFAULT_MAX_PEERS,
    DEFAULT_PEER_BOOT_TIMEOUT,
    HANDSHAKE_TIMEOUT,
    MAX_CONCURRENT_CONNECTION_ATTEMPTS,
    PEER_READY_TIMEOUT,
    QUIET_PEER_POOL_SIZE,
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
    UnknownAPI,
    UnreachablePeer,
)
from p2p.metrics import PeerReporterRegistry
from p2p.peer import (
    BasePeer,
    BasePeerFactory,
    BasePeerContext,
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
from p2p.tracking.connection import (
    BaseConnectionTracker,
    NoopConnectionTracker,
)
from p2p._utils import get_logger

DIAL_IN_OUT_RATIO = 0.75
COMMON_PEER_CONNECTION_EXCEPTIONS = cast(Tuple[Type[BaseP2PError], ...], (
    NoMatchingPeerCapabilities,
    PeerConnectionLost,
    asyncio.TimeoutError,
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


OVER_PROVISION_MISSING_PEERS = 4


class BasePeerPool(Service, AsyncIterable[BasePeer]):
    """
    PeerPool maintains connections to up-to max_peers on a given network.
    """
    _report_stats_interval = 60
    _report_metrics_interval = 15  # for influxdb/grafana reporting
    _peer_boot_timeout = DEFAULT_PEER_BOOT_TIMEOUT
    _event_bus: EndpointAPI = None
    # If set to True, exceptions raised when connecting to a remote peer will be logged (DEBUG if
    # it's in ALLOWED_PEER_CONNECTION_EXCEPTIONS or ERROR if not) and suppressed. Should only be
    # set to False in tests if we want to ensure a connection is successful.
    _suppress_connection_exceptions: bool = True

    _handshake_locks: ResourceLock[NodeAPI]
    peer_reporter_registry_class: Type[PeerReporterRegistry[Any]] = PeerReporterRegistry[BasePeer]

    def __init__(self,
                 privkey: keys.PrivateKey,
                 context: BasePeerContext,
                 max_peers: int = DEFAULT_MAX_PEERS,
                 event_bus: EndpointAPI = None,
                 metrics_registry: MetricsRegistry = None,
                 ) -> None:
        self.logger = get_logger(self.__module__ + '.' + self.__class__.__name__)

        self.privkey = privkey
        self.max_peers = max_peers
        self.context = context

        self.connected_nodes: Dict[SessionAPI, BasePeer] = {}

        self._subscribers: List[PeerSubscriber] = []
        self._event_bus = event_bus

        if metrics_registry is None:
            # Initialize with a MetricsRegistry from pyformance as p2p can not depend on Trinity
            # This is so that we don't need to pass a MetricsRegistry in tests and mocked pools.
            metrics_registry = MetricsRegistry()
        self.metrics_registry = metrics_registry
        self._active_peer_counter = metrics_registry.counter('trinity.p2p/peers.counter')
        self._peer_reporter_registry = self.get_peer_reporter_registry(metrics_registry)

        # Restricts the number of concurrent connection attempts can be made
        self._connection_attempt_lock = asyncio.BoundedSemaphore(MAX_CONCURRENT_CONNECTION_ATTEMPTS)

        # Ensure we can only have a single concurrent handshake in flight per remote
        self._handshake_locks = ResourceLock()

        self.peer_backends = self.setup_peer_backends()
        self.connection_tracker = self.setup_connection_tracker()

    @cached_property
    def has_event_bus(self) -> bool:
        return self._event_bus is not None

    def get_event_bus(self) -> EndpointAPI:
        if self._event_bus is None:
            raise AttributeError("No event bus configured for this peer pool")
        return self._event_bus

    def setup_connection_tracker(self) -> BaseConnectionTracker:
        """
        Return an instance of `p2p.tracking.connection.BaseConnectionTracker`
        which will be used to track peer connection failures.
        """
        return NoopConnectionTracker()

    def setup_peer_backends(self) -> Tuple[BasePeerBackend, ...]:
        if self.has_event_bus:
            return (
                DiscoveryPeerBackend(self.get_event_bus()),
                BootnodesPeerBackend(self.get_event_bus()),
            )
        else:
            self.logger.warning("No event bus configured for peer pool.")
            return ()

    @property
    def available_slots(self) -> int:
        return self.max_peers - len(self)

    async def _add_peers_from_backend(
            self,
            backend: BasePeerBackend,
            should_skip_fn: Callable[[Tuple[NodeID, ...], NodeAPI], bool]
    ) -> int:

        connected_node_ids = {peer.remote.id for peer in self.connected_nodes.values()}
        # Only ask for random bootnodes if we're not connected to any peers.
        if isinstance(backend, BootnodesPeerBackend) and len(connected_node_ids) > 0:
            return 0

        try:
            blacklisted_node_ids = await asyncio.wait_for(
                self.connection_tracker.get_blacklisted(),
                timeout=1)
        except asyncio.TimeoutError:
            self.logger.warning(
                "Timed out getting blacklisted peers from connection tracker, pausing peer "
                "addition until we can get that.")
            return 0

        skip_list = connected_node_ids.union(blacklisted_node_ids)
        should_skip_fn = functools.partial(should_skip_fn, skip_list)
        # Request a large batch on every iteration as that will effectively push DiscoveryService
        # to trigger new peer lookups in order to find enough compatible peers to fulfill our
        # request. There's probably some room for experimentation here in order to find an optimal
        # value.
        max_candidates = self.available_slots * OVER_PROVISION_MISSING_PEERS
        try:
            candidates = await asyncio.wait_for(
                backend.get_peer_candidates(
                    max_candidates=max_candidates,
                    should_skip_fn=should_skip_fn,
                ),
                timeout=REQUEST_PEER_CANDIDATE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            self.logger.warning("PeerCandidateRequest timed out to backend %s", backend)
            return 0
        else:
            self.logger.debug("Got %d peer candidates from backend %s", len(candidates), backend)
            if candidates:
                await self.connect_to_nodes(candidates)
            return len(candidates)

    def __len__(self) -> int:
        return len(self.connected_nodes)

    @property
    @abstractmethod
    def peer_factory_class(self) -> Type[BasePeerFactory]:
        ...

    def get_peer_factory(self) -> BasePeerFactory:
        return self.peer_factory_class(
            privkey=self.privkey,
            context=self.context,
            event_bus=self._event_bus,
        )

    def get_peer_reporter_registry(
        self, metrics_registry: MetricsRegistry
    ) -> PeerReporterRegistry[BasePeer]:
        return self.peer_reporter_registry_class(metrics_registry)

    async def get_protocol_capabilities(self) -> Tuple[Tuple[str, int], ...]:
        return tuple(
            (handshaker.protocol_class.name, handshaker.protocol_class.version)
            for handshaker in await self.get_peer_factory().get_handshakers()
        )

    @property
    def is_full(self) -> bool:
        return len(self) >= self.max_peers

    def is_valid_connection_candidate(self, candidate: NodeAPI) -> bool:
        # connect to no more then 2 nodes with the same IP
        nodes_by_ip = groupby(
            operator.attrgetter('remote.address.ip'),
            self.connected_nodes.keys(),
        )
        matching_ip_nodes = nodes_by_ip.get(candidate.address.ip, [])
        return len(matching_ip_nodes) <= 2

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

    async def _start_peer(self, peer: BasePeer) -> None:
        self.manager.run_child_service(peer.connection)
        await asyncio.wait_for(
            peer.connection.get_manager().wait_started(), timeout=PEER_READY_TIMEOUT)
        await peer.connection.run_peer(peer)

    async def disconnect_from_peer(
        self, peer_session: SessionAPI, reason: DisconnectReason
    ) -> None:
        if peer_session in self.connected_nodes:
            peer = self.connected_nodes[peer_session]
            self.logger.info("Disconnecting from %s.", peer)
            await peer.disconnect(reason)
        else:
            self.logger.info("Unable to disconnect from %s, since peer is not connected to.", peer)

    async def add_inbound_peer(self, peer: BasePeer) -> None:
        try:
            await self._start_peer(peer)
        except asyncio.TimeoutError as err:
            self.logger.debug('Timeout waiting for %s to start: %s', peer, err)
            return

        if self.is_connected_to_node(peer.remote):
            self.logger.debug("Aborting inbound connection attempt by %s. Already connected!", peer)
            await peer.disconnect(DisconnectReason.ALREADY_CONNECTED)
            return

        if self.is_full:
            self.logger.debug("Aborting inbound connection attempt by %s. PeerPool is full", peer)
            await peer.disconnect(DisconnectReason.TOO_MANY_PEERS)
            return
        elif not self.is_valid_connection_candidate(peer.remote):
            self.logger.debug(
                "Aborting inbound connection attempt by %s. Not a valid candidate", peer)
            # XXX: Currently, is_valid_connection_candidate() only checks that we're connected
            # to 2 or less nodes with the same IP, so TOO_MANY_PEERS is what makes the most sense
            # here.
            await peer.disconnect(DisconnectReason.TOO_MANY_PEERS)
            return

        total_peers = len(self)
        inbound_peer_count = len(
            tuple(peer for peer in self.connected_nodes.values() if peer.inbound))
        if total_peers > 1 and inbound_peer_count / total_peers > DIAL_IN_OUT_RATIO:
            self.logger.debug(
                "Aborting inbound connection attempt by %s. Too many inbound peers", peer)
            await peer.disconnect(DisconnectReason.TOO_MANY_PEERS)
            return

        await self._add_peer_and_bootstrap(peer)

    async def add_outbound_peer(self, peer: BasePeer) -> None:
        try:
            await self._start_peer(peer)
        except asyncio.TimeoutError as err:
            self.logger.debug('Timeout waiting for %s to start: %s', peer, err)
            return

        # Check again to see if we have *become* full since the previous check.
        if self.is_full:
            self.logger.debug(
                "Successfully connected to %s but peer pool is full.  Disconnecting.",
                peer,
            )
            await peer.disconnect(DisconnectReason.TOO_MANY_PEERS)
            return
        elif not self.manager.is_running:
            self.logger.debug(
                "Successfully connected to %s but peer pool is closing.  Disconnecting.",
                peer,
            )
            await peer.disconnect(DisconnectReason.CLIENT_QUITTING)
            return

        await self._add_peer_and_bootstrap(peer)

    async def _add_peer_and_bootstrap(self, peer: BasePeer) -> None:
        # Add the peer to ourselves, ensuring it has subscribers before we start the protocol
        # streams.
        self._add_peer(peer)
        peer.start_protocol_streams()

        try:
            await asyncio.wait_for(
                peer.boot_manager.get_manager().wait_finished(),
                timeout=self._peer_boot_timeout
            )
        except asyncio.TimeoutError as err:
            self.logger.debug('Timout waiting for peer to boot: %s', err)
            await peer.disconnect(DisconnectReason.TIMEOUT)
            return

    def record_metrics_for_added_peer(self, peer: BasePeer) -> None:
        self._active_peer_counter.inc()

    def _add_peer(self, peer: BasePeer) -> None:
        """Add the given peer to the pool.

        Add the peer to our list of connected nodes and add each of our subscribers
        to the peer.
        """
        if len(self) < QUIET_PEER_POOL_SIZE:
            logger = self.logger.info
        else:
            logger = self.logger.debug
        logger("Adding %s to pool", peer)
        self.connected_nodes[peer.session] = peer
        self.record_metrics_for_added_peer(peer)
        self._peer_reporter_registry.assign_peer_reporter(peer)
        peer.add_finished_callback(self._peer_finished)
        for subscriber in self._subscribers:
            subscriber.register_peer(peer)
            peer.add_subscriber(subscriber)

    @abstractmethod
    async def maybe_connect_more_peers(self) -> None:
        ...

    async def run(self) -> None:
        if self.has_event_bus:
            self.manager.run_daemon_task(self.maybe_connect_more_peers)

        self.manager.run_daemon_task(self._periodically_report_stats)
        self.manager.run_daemon_task(self._periodically_report_metrics)
        await self.manager.wait_finished()

    async def connect(self, remote: NodeAPI) -> BasePeer:
        """
        Connect to the given remote and return a Peer instance when successful.
        Returns None if the remote is unreachable, times out or is useless.
        """
        if not self._handshake_locks.is_locked(remote):
            self.logger.warning("Tried to connect to %s without acquiring lock first!", remote)

        if any(peer.remote == remote for peer in self.connected_nodes.values()):
            self.logger.warning(
                "Attempted to connect to peer we are already connected to: %s", remote)
            raise IneligiblePeer(f"Already connected to {remote}")

        try:
            self.logger.debug("Connecting to %s...", remote)
            task_name = f'PeerPool/Handshake/{remote}'
            task = create_task(self.get_peer_factory().handshake(remote), task_name)
            return await asyncio.wait_for(task, timeout=HANDSHAKE_TIMEOUT)
        except BadAckMessage:
            # This is kept separate from the
            # `COMMON_PEER_CONNECTION_EXCEPTIONS` to be sure that we aren't
            # silencing an error in our authentication code.
            self.logger.error('Got bad auth ack from %r', remote)
            # dump the full stacktrace in the debug logs
            self.logger.debug('Got bad auth ack from %r', remote, exc_info=True)
            raise
        except MalformedMessage as e:
            # This is kept separate from the
            # `COMMON_PEER_CONNECTION_EXCEPTIONS` to be sure that we aren't
            # silencing an error in how we decode messages during handshake.
            self.logger.error('Got malformed response from %r during handshake', remote)
            # dump the full stacktrace in the debug logs
            self.logger.debug('Got malformed response from %r', remote, exc_info=True)
            self.connection_tracker.record_failure(remote, e)
            raise
        except HandshakeFailure as e:
            self.logger.debug("Could not complete handshake with %r: %s", remote, repr(e))
            self.connection_tracker.record_failure(remote, e)
            raise
        except COMMON_PEER_CONNECTION_EXCEPTIONS as e:
            self.logger.debug("Could not complete handshake with %r: %s", remote, repr(e))
            self.connection_tracker.record_failure(remote, e)
            raise
        finally:
            # XXX: We sometimes get an exception here but the task is finished and with
            # an exception as well. No idea how that happens but if we don't consume the
            # task's exception, asyncio complains.
            if not task.done():
                task.cancel()
            try:
                await task
            except (Exception, asyncio.CancelledError):
                pass

    async def connect_to_nodes(self, nodes: Sequence[NodeAPI]) -> None:
        # create an generator for the nodes
        nodes_iter = iter(nodes)

        while True:
            if self.is_full or not self.manager.is_running:
                return

            # only attempt to connect to up to the maximum number of available
            # peer slots that are open.
            batch_size = clamp(1, 10, self.available_slots)
            batch = tuple(take(batch_size, nodes_iter))

            # There are no more *known* nodes to connect to.
            if not batch:
                return

            self.logger.debug(
                'Initiating %d peer connection attempts with %d open peer slots',
                len(batch),
                self.available_slots,
            )
            # Try to connect to the peers concurrently.
            await asyncio.gather(*(self.connect_to_node(node) for node in batch))

    def lock_node_for_handshake(self, node: NodeAPI) -> AsyncContextManager[None]:
        return self._handshake_locks.lock(node)

    def is_connected_to_node(self, node: NodeAPI) -> bool:
        return any(
            node == session.remote
            for session in self.connected_nodes.keys()
        )

    async def connect_to_node(self, node: NodeAPI) -> None:
        """
        Connect to a single node quietly aborting if the peer pool is full or
        shutting down, or one of the expected peer level exceptions is raised
        while connecting.
        """
        if self.is_full or not self.manager.is_running:
            self.logger.warning("Asked to connect to node when either full or not operational")
            return

        if self._handshake_locks.is_locked(node):
            self.logger.info(
                "Asked to connect to %s when handshake lock is already locked, will wait", node)

        async with self.lock_node_for_handshake(node):
            if self.is_connected_to_node(node):
                self.logger.debug(
                    "Aborting outbound connection attempt to %s. Already connected!",
                    node,
                )
                return

            try:
                async with self._connection_attempt_lock:
                    peer = await self.connect(node)
            except ALLOWED_PEER_CONNECTION_EXCEPTIONS:
                if self._suppress_connection_exceptions:
                    # These are all logged in self.connect(), so we simply return.
                    return
                else:
                    raise
            except Exception:
                self.logger.exception("Unexpected error connecting to %s", node)
                if self._suppress_connection_exceptions:
                    return
                else:
                    raise

            await self.add_outbound_peer(peer)

    def record_metrics_for_removed_peer(self, peer: BasePeer) -> None:
        self._active_peer_counter.dec()

    def _peer_finished(self, peer: BasePeer) -> None:
        """
        Remove the given peer from our list of connected nodes.
        This is passed as a callback to be called when a peer finishes.
        """
        if peer.session in self.connected_nodes:
            self.logger.debug(
                "Removing %s from pool: local_reason=%s remote_reason=%s",
                peer,
                peer.local_disconnect_reason,
                peer.remote_disconnect_reason,
            )
            self.connected_nodes.pop(peer.session)
        else:
            self.logger.warning(
                "%s finished but was not found in connected_nodes (%s)",
                peer,
                tuple(self.connected_nodes.values()),
            )

        for subscriber in self._subscribers:
            subscriber.deregister_peer(peer)

        self.record_metrics_for_removed_peer(peer)
        self._peer_reporter_registry.unassign_peer_reporter(peer)

    async def __aiter__(self) -> AsyncIterator[BasePeer]:
        for peer in tuple(self.connected_nodes.values()):
            # Yield control to ensure we process any disconnection requests from peers. Otherwise
            # we could return peers that should have been disconnected already.
            await asyncio.sleep(0)
            if peer.is_alive:
                yield peer

    async def _periodically_report_metrics(self) -> None:
        while self.manager.is_running:
            self._peer_reporter_registry.trigger_peer_reports()
            await asyncio.sleep(self._report_metrics_interval)

    def log_extra_stats(self) -> None:
        pass

    async def _periodically_report_stats(self) -> None:
        while self.manager.is_running:
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
                if not peer.get_manager().is_running:
                    self.logger.debug(
                        "%s is no longer alive but had not been removed from pool", peer)
                    continue
                self.logger.debug(
                    "%s: uptime=%s, received_msgs=%d",
                    peer,
                    humanize_seconds(peer.uptime),
                    peer.received_msgs_count,
                )
                self.logger.debug(
                    "client_version_string='%s'",
                    peer.safe_client_version_string,
                )
                try:
                    for line in peer.get_extra_stats():
                        self.logger.debug("    %s", line)
                except (UnknownAPI, PeerConnectionLost) as exc:
                    self.logger.debug("    Failure during stats lookup: %r", exc)

            self.log_extra_stats()
            self.logger.debug("== End peer details == ")
            await asyncio.sleep(self._report_stats_interval)
