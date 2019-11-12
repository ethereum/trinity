from abc import abstractmethod
import asyncio
import operator
from typing import (
    AsyncIterator,
    AsyncIterable,
    cast,
    Dict,
    Iterator,
    List,
    Tuple,
    Type,
)

from cached_property import cached_property

from eth_keys import (
    datatypes,
)
from eth_utils import (
    clamp,
    humanize_seconds,
    get_extended_debug_logger,
)
from eth_utils.toolz import (
    groupby,
    take,
)
from lahja import (
    BroadcastConfig,
    EndpointAPI,
)

from p2p.abc import NodeAPI, SessionAPI
from p2p.constants import (
    DEFAULT_MAX_PEERS,
    DEFAULT_PEER_BOOT_TIMEOUT,
    DISCOVERY_EVENTBUS_ENDPOINT,
    HANDSHAKE_TIMEOUT,
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
from p2p.service import Service, ServiceAPI
from p2p.token_bucket import TokenBucket
from p2p.tracking.connection import (
    BaseConnectionTracker,
    NoopConnectionTracker,
)


TO_DISCOVERY_BROADCAST_CONFIG = BroadcastConfig(filter_endpoint=DISCOVERY_EVENTBUS_ENDPOINT)


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


class BasePeerPool(Service, AsyncIterable[BasePeer]):
    """
    PeerPool maintains connections to up-to max_peers on a given network.
    """
    _report_interval = 60
    _peer_boot_timeout = DEFAULT_PEER_BOOT_TIMEOUT
    _event_bus: EndpointAPI = None

    logger = get_extended_debug_logger('p2p.peer_pool.PeerPool')

    def __init__(self,
                 privkey: datatypes.PrivateKey,
                 context: BasePeerContext,
                 max_peers: int = DEFAULT_MAX_PEERS,
                 event_bus: EndpointAPI = None,
                 ) -> None:
        self.privkey = privkey
        self.max_peers = max_peers
        self.context = context

        self.connected_nodes: Dict[SessionAPI, BasePeer] = {}

        self._subscribers: List[PeerSubscriber] = []
        self._event_bus = event_bus

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

    async def _add_peers_from_backend(self, backend: BasePeerBackend) -> None:
        available_slots = self.max_peers - len(self)

        try:
            connected_remotes = {
                peer.remote for peer in self.connected_nodes.values()
            }
            candidates = await asyncio.wait_for(
                backend.get_peer_candidates(
                    num_requested=available_slots,
                    connected_remotes=connected_remotes,
                ),
                timeout=REQUEST_PEER_CANDIDATE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            self.logger.warning("PeerCandidateRequest timed out to backend %s", backend)
            return
        else:
            self.logger.debug2(
                "Got candidates from backend %s (%s)",
                backend,
                candidates,
            )
            if candidates:
                await self.connect_to_nodes(iter(candidates))

    async def maybe_connect_more_peers(self) -> None:
        rate_limiter = TokenBucket(
            rate=1 / PEER_CONNECT_INTERVAL,
            capacity=MAX_SEQUENTIAL_PEER_CONNECT,
        )

        while self.manager.is_running:
            if self.is_full:
                await asyncio.sleep(PEER_CONNECT_INTERVAL)
                continue

            await rate_limiter.take()

            try:
                await asyncio.gather(*(
                    self._add_peers_from_backend(backend)
                    for backend in self.peer_backends
                ))
            except asyncio.CancelledError:
                # no need to log this exception, this is expected
                raise
            except Exception:
                self.logger.exception("unexpected error during peer connection")
                # Continue trying to connect to peers, even if there was a
                # surprising failure during one of the attempts.
                continue

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

    async def start_peer(self, peer: BasePeer) -> None:
        self.manager.run_child_service(peer.connection)
        await asyncio.wait_for(peer.connection.manager.wait_started(), timeout=1)

        self.manager.run_child_service(peer)
        await asyncio.wait_for(peer.manager.wait_started(), timeout=1)
        await asyncio.wait_for(peer.ready.wait(), timeout=1)
        if peer.manager.is_running:
            self._add_peer(peer, ())
        else:
            self.logger.debug("%s was cancelled immediately, not adding to pool", peer)

        try:
            await asyncio.wait_for(
                peer.boot_manager.manager.wait_stopped(),
                timeout=self._peer_boot_timeout
            )
        except asyncio.TimeoutError as err:
            self.logger.debug('Timout waiting for peer to boot: %s', err)
            await peer.disconnect(DisconnectReason.TIMEOUT)
            return
        except HandshakeFailure as err:
            self.connection_tracker.record_failure(peer.remote, err)
            raise
        else:
            if not peer.manager.is_running:
                self.logger.debug('%s disconnected during boot-up, dropped from pool', peer)

    def _add_peer(self,
                  peer: BasePeer,
                  msgs: Tuple[PeerMessage, ...]) -> None:
        """Add the given peer to the pool.

        Appart from adding it to our list of connected nodes and adding each of our subscriber's
        to the peer, we also add the given messages to our subscriber's queues.
        """
        self.logger.info('Adding %s to pool', peer)
        self.connected_nodes[peer.session] = peer
        self.manager.run_task(self._run_peer, peer)
        for subscriber in self._subscribers:
            subscriber.register_peer(peer)
            peer.add_subscriber(subscriber)
            for msg in msgs:
                subscriber.add_msg(msg)

    async def _run_peer(self, peer: BasePeer) -> None:
        try:
            await peer.manager.wait_stopped()
        finally:
            self._peer_finished(peer)

    async def run(self) -> None:
        if self.has_event_bus:
            self.manager.run_daemon_task(self.maybe_connect_more_peers)

        self.manager.run_daemon_task(self._periodically_report_stats)
        try:
            await self.manager.wait_forever()
        finally:
            await self.stop_all_peers()

    async def stop_all_peers(self) -> None:
        self.logger.info("Stopping all peers ...")
        peers = self.connected_nodes.values()
        disconnections = (
            peer.disconnect(DisconnectReason.CLIENT_QUITTING)
            for peer in peers
            if peer.manager.is_running
        )
        await asyncio.gather(*disconnections)

    async def connect(self, remote: NodeAPI) -> BasePeer:
        """
        Connect to the given remote and return a Peer instance when successful.
        Returns None if the remote is unreachable, times out or is useless.
        """
        if self._handshake_locks.is_locked(remote):
            self.logger.debug2("Skipping %s; already shaking hands", remote)
            raise IneligiblePeer(f"Already shaking hands with {remote}")

        async with self._handshake_locks.lock(remote):

            if any(peer.remote == remote for peer in self.connected_nodes.values()):
                self.logger.debug2("Skipping %s; already connected to it", remote)
                raise IneligiblePeer(f"Already connected to {remote}")

            try:
                should_connect = await asyncio.wait_for(
                    self.connection_tracker.should_connect_to(remote),
                    timeout=1,
                )
            except asyncio.TimeoutError:
                self.logger.warning("ConnectionTracker.should_connect_to request timed out.")
                raise

            if not should_connect:
                raise IneligiblePeer(f"Peer database rejected peer candidate: {remote}")

            try:
                self.logger.debug2("Connecting to %s...", remote)
                return await asyncio.wait_for(
                    self.get_peer_factory().handshake(remote),
                    timeout=HANDSHAKE_TIMEOUT,
                )
            except BadAckMessage:
                # This is kept separate from the
                # `COMMON_PEER_CONNECTION_EXCEPTIONS` to be sure that we aren't
                # silencing an error in our authentication code.
                self.logger.error('Got bad auth ack from %r', remote)
                # dump the full stacktrace in the debug logs
                self.logger.debug('Got bad auth ack from %r', remote, exc_info=True)
                raise
            except MalformedMessage:
                # This is kept separate from the
                # `COMMON_PEER_CONNECTION_EXCEPTIONS` to be sure that we aren't
                # silencing an error in how we decode messages during handshake.
                self.logger.error('Got malformed response from %r during handshake', remote)
                # dump the full stacktrace in the debug logs
                self.logger.debug('Got malformed response from %r', remote, exc_info=True)
                raise
            except HandshakeFailure as e:
                self.logger.debug("Could not complete handshake with %r: %s", remote, repr(e))
                self.connection_tracker.record_failure(remote, e)
                raise
            except COMMON_PEER_CONNECTION_EXCEPTIONS as e:
                self.logger.debug("Could not complete handshake with %r: %s", remote, repr(e))
                raise
            except asyncio.CancelledError:
                # no need to log this exception, this is expected
                raise
            except Exception:
                self.logger.exception("Unexpected error during auth/p2p handshake with %r", remote)
                raise

    async def connect_to_nodes(self, nodes: Iterator[NodeAPI]) -> None:
        # create an generator for the nodes
        nodes_iter = iter(nodes)

        while True:
            if self.is_full:
                return

            # only attempt to connect to up to the maximum number of available
            # peer slots that are open.
            available_peer_slots = self.max_peers - len(self)
            batch_size = clamp(1, 10, available_peer_slots)
            batch = tuple(take(batch_size, nodes_iter))

            # There are no more *known* nodes to connect to.
            if not batch:
                return

            self.logger.debug(
                'Initiating %d peer connection attempts with %d open peer slots',
                len(batch),
                available_peer_slots,
            )
            # Try to connect to the peers concurrently.
            await asyncio.gather(
                *(self.connect_to_node(node) for node in batch),
            )

    async def connect_to_node(self, node: NodeAPI) -> None:
        """
        Connect to a single node quietly aborting if the peer pool is full or
        shutting down, or one of the expected peer level exceptions is raised
        while connecting.
        """
        if self.is_full:
            return

        try:
            async with self._connection_attempt_lock:
                peer = await self.connect(node)
        except ALLOWED_PEER_CONNECTION_EXCEPTIONS:
            return

        # Check again to see if we have *become* full since the previous
        # check.
        if self.is_full:
            self.logger.debug(
                "Successfully connected to %s but peer pool is full.  Disconnecting.",
                peer,
            )
            await peer.disconnect(DisconnectReason.TOO_MANY_PEERS)
            return
        else:
            await self.start_peer(peer)

    def _peer_finished(self, peer: ServiceAPI) -> None:
        """
        Remove the given peer from our list of connected nodes.
        This is passed as a callback to be called when a peer finishes.
        """
        peer = cast(BasePeer, peer)
        if peer.session in self.connected_nodes:
            self.logger.info(
                "Removing %s from pool: local_reason=%s remote_reason=%s",
                peer,
                peer.p2p_api.local_disconnect_reason,
                peer.p2p_api.remote_disconnect_reason,
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

    async def __aiter__(self) -> AsyncIterator[BasePeer]:
        for peer in tuple(self.connected_nodes.values()):
            # Yield control to ensure we process any disconnection requests from peers. Otherwise
            # we could return peers that should have been disconnected already.
            await asyncio.sleep(0)
            if peer.manager.is_running and not peer.is_closing:
                yield peer

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
                if not peer.manager.is_running:
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
                    peer.p2p_api.safe_client_version_string,
                )
                for line in peer.get_extra_stats():
                    self.logger.debug("    %s", line)
            self.logger.debug("== End peer details == ")
            await asyncio.sleep(self._report_interval)
