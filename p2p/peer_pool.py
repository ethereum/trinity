from abc import (
    abstractmethod,
)
import asyncio
import operator
from pathlib import Path
import sqlite3
import time
import toml
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

from cancel_token import (
    CancelToken,
    OperationCancelled,
)
from eth_keys import (
    datatypes,
)
from eth_utils.toolz import (
    groupby,
)
from lahja import (
    Endpoint,
)

from eth.tools.logging import ExtendedDebugLogger

from p2p.constants import (
    DEFAULT_MAX_PEERS,
    DEFAULT_PEER_BOOT_TIMEOUT,
    DISOVERY_INTERVAL,
    REQUEST_PEER_CANDIDATE_TIMEOUT,
)
from p2p.events import (
    ConnectToNodeCommand,
    PeerCandidatesRequest,
    PeerCountRequest,
    PeerCountResponse,
    RandomBootnodeRequest,
)
from p2p.exceptions import (
    BadAckMessage,
    HandshakeFailure,
    MalformedMessage,
    PeerConnectionLost,
    UnreachablePeer,
    WrongNetworkFailure,
    WrongGenesisFailure,
)
from p2p.kademlia import (
    from_uris,
    Node,
)
from p2p.peer import (
    BasePeer,
    BasePeerFactory,
    BasePeerContext,
    handshake,
    PeerMessage,
    PeerSubscriber,
)
from p2p.p2p_proto import (
    DisconnectReason,
)
from p2p.service import (
    BaseService,
)


class PeerInfoPersistence:
    def __init__(self, path: Path, logger: ExtendedDebugLogger):
        # TODO: implement some kind of exponential backoff?
        self.path = path
        self.logger = logger.getChild('PeerInfo')
        self.logger.debug('Reading from %s', path)

        # TODO: aiosqlite
        self.db = sqlite3.connect(self.path)
        self.db.row_factory = sqlite3.Row
        self._setup_schema()

    def _setup_schema(self) -> None:
        # TODO: add some scripts to scripts/ to make inspecting this db easier
        cur = self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        if len(cur.fetchall()):
            self.logger.debug('database already existed')
            return
        self.logger.debug('creating tables')
        with self.db:
            self.db.execute('create table bad_nodes (enode, until, reason, error_count, kwargs)')
            self.db.execute('create table good_nodes (enode)')
            self.db.execute('create table events (enode, event, kwargs)')

    def too_many_peers(self, remote: Node) -> None:
        self._fail(
            remote,
            timeout=60,  # try again in a minute
            reason='too_many_peers',
        )

    def unreachable_peer(self, remote: Node) -> None:
        self._fail(
            remote,
            timeout=60 * 10,  # try again in 10 minutes
            reason='UnreachablePeer',
        )

    def wrong_network_failure(self, remote: Node, failure: WrongNetworkFailure):
        self._fail(
            remote,
            timeout=60 * 60 * 24,  # re-attempt once a day
            reason='WrongNetwork',
            network=failure.network,
        )

    def wrong_genesis_failure(self, remote: Node, failure: WrongGenesisFailure):
        self._fail(
            remote,
            timeout=60 * 60 * 24,  # re-attempt once a day
            reason='WrongGenesis',
            genesis_hash=failure.genesis,
        )

    def handshake_failure(self, remote: Node, failure: HandshakeFailure):
        self._fail(
            remote,
            timeout=60 * 10,  # try again in 10 minutes
            reason='HandshakeFailure',
            messsage=str(failure),
        )

    def _fail(self, remote: Node, *, timeout: int, reason: str, **kwargs):
        self.logger.info("Recording %s for %s", reason, remote)
        enode = remote.uri()
        cursor = self.db.execute('SELECT * from bad_nodes WHERE enode = ?', (enode,))
        row = cursor.fetchone()
        if row:
            error_count = row['error_count'] + 1
            self.db.execute(
                'UPDATE bad_nodes SET until = ?, reason = ?, error_count = ? WHERE enode = ?',
                (int(time.time()) + timeout*error_count, reason, error_count, enode),
            )
            self.db.commit()
            self.logger.info(
                "%s failed again with reason: %s", remote, reason
            )
            return
        with self.db:
            self.db.execute(
                'INSERT INTO bad_nodes VALUES (?, ?, ?, ?, ?)',
                (enode, int(time.time()) + timeout, reason, 1, ""),
            )

    def can_connect_to(self, remote: Node) -> bool:
        enode = remote.uri()
        cursor = self.db.execute('SELECT * from bad_nodes WHERE enode = ?', (enode,))
        row = cursor.fetchone()

        if not row:
            self.logger.debug("Can connect to %s", remote)
            return True

        if time.time() < row['until']:
            self.logger.info("Cannot connect to %s because: %s", remote, row['reason'])
            return False

        self.logger.info("Reattempting failed node: %s", remote)
        return True

    def connect_success(self, remote: Node) -> None:
        "Record a successful connection to this node"
        pass
        contents = self._load()
        key = remote.uri()
        if key not in contents:
            self.logger.info("Successful connection with %s", remote)
            contents[key] = {
                'status': 'preferred',
                'when': int(time.time()),
                'reason': 'successful-connection',
            }
            self._write(contents)
            return

        self.logger.info("Successful connection with %s: %s", remote, contents[key])
        contents[key].update({
            'status': 'preferred',
            'consecutive-error-count': 0,
            'when': int(time.time()),
            'reason': 'successful-connection',
        })
        self._write(contents)

    def get_preferred_peers(self) -> Iterator[Node]:
        rows = self.db.execute('SELECT enode from good_nodes').fetchall()
        return [Node.from_uri(row['enode']) for row in rows]


class BasePeerPool(BaseService, AsyncIterable[BasePeer]):
    """
    PeerPool maintains connections to up-to max_peers on a given network.
    """
    _report_interval = 60
    _peer_boot_timeout = DEFAULT_PEER_BOOT_TIMEOUT

    def __init__(self,
                 privkey: datatypes.PrivateKey,
                 context: BasePeerContext,
                 max_peers: int = DEFAULT_MAX_PEERS,
                 nodedb_path: Path = None,
                 token: CancelToken = None,
                 event_bus: Endpoint = None
                 ) -> None:
        super().__init__(token)

        self.privkey = privkey
        self.max_peers = max_peers
        self.context = context

        self.peer_info = PeerInfoPersistence(nodedb_path, self.logger)

        self.connected_nodes: Dict[Node, BasePeer] = {}
        self._subscribers: List[PeerSubscriber] = []
        self.event_bus = event_bus

    async def accept_connect_commands(self) -> None:
        async for command in self.wait_iter(self.event_bus.stream(ConnectToNodeCommand)):
            self.logger.debug('Received request to connect to %s', command.node)
            self.run_task(self.connect_to_nodes(from_uris([command.node])))

    async def handle_peer_count_requests(self) -> None:
        async for req in self.wait_iter(self.event_bus.stream(PeerCountRequest)):
                # We are listening for all `PeerCountRequest` events but we ensure to only send a
                # `PeerCountResponse` to the callsite that made the request.  We do that by
                # retrieving a `BroadcastConfig` from the request via the
                # `event.broadcast_config()` API.
                self.event_bus.broadcast(PeerCountResponse(len(self)), req.broadcast_config())

    async def maybe_connect_more_peers(self) -> None:
        preferred_nodes = self.peer_info.get_preferred_peers()
        if len(preferred_nodes):
            self.logger.debug("Connecting to known-good nodes (%s)", preferred_nodes)
            await self.connect_to_nodes(preferred_nodes)

        while self.is_operational:
            await self.sleep(DISOVERY_INTERVAL)

            available_peer_slots = self.max_peers - len(self)
            if available_peer_slots > 0:
                try:
                    response = await self.wait(
                        # TODO: This should use a BroadcastConfig to send the request to discovery
                        # only as soon as we have cut a new Lahja release.
                        self.event_bus.request(PeerCandidatesRequest(available_peer_slots)),
                        timeout=REQUEST_PEER_CANDIDATE_TIMEOUT
                    )
                except TimeoutError:
                    self.logger.warning("Discovery did not answer PeerCandidateRequest in time")
                    continue

                # In some cases (e.g ROPSTEN or private testnets), the discovery table might be
                # full of bad peers so if we can't connect to any peers we try a random bootstrap
                # node as well.
                if not len(self):
                    try:
                        response = await self.wait(
                            # TODO: This should use a BroadcastConfig to send the request to
                            # discovery only as soon as we have cut a new Lahja release.
                            self.event_bus.request(RandomBootnodeRequest()),
                            timeout=REQUEST_PEER_CANDIDATE_TIMEOUT
                        )
                    except TimeoutError:
                        self.logger.warning(
                            "Discovery did not answer RandomBootnodeRequest in time"
                        )
                        continue

                self.logger.debug("Received candidates to connect to (%s)", response.candidates)
                await self.connect_to_nodes(from_uris(response.candidates))

    def __len__(self) -> int:
        return len(self.connected_nodes)

    @property
    @abstractmethod
    def peer_factory_class(self) -> Type[BasePeerFactory]:
        pass

    def get_peer_factory(self) -> BasePeerFactory:
        return self.peer_factory_class(
            privkey=self.privkey,
            context=self.context,
            token=self.cancel_token,
        )

    @property
    def is_full(self) -> bool:
        return len(self) >= self.max_peers

    def is_valid_connection_candidate(self, candidate: Node) -> bool:
        # connect to no more then 2 nodes with the same IP
        nodes_by_ip = groupby(
            operator.attrgetter('remote.address.ip'),
            self.connected_nodes.values(),
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
        self.run_child_service(peer)
        await self.wait(peer.events.started.wait(), timeout=1)
        try:
            with peer.collect_sub_proto_messages() as buffer:
                await self.wait(
                    peer.boot_manager.events.finished.wait(),
                    timeout=self._peer_boot_timeout
                )
        except TimeoutError as err:
            self.logger.debug('Timout waiting for peer to boot: %s', err)
            await peer.disconnect(DisconnectReason.timeout)
            return
        else:
            if peer.is_operational:
                self._add_peer(peer, buffer.get_messages())
            else:
                self.logger.debug('%s disconnected during boot-up, not adding to pool', peer)

    def _add_peer(self,
                  peer: BasePeer,
                  msgs: Tuple[PeerMessage, ...]) -> None:
        """Add the given peer to the pool.

        Appart from adding it to our list of connected nodes and adding each of our subscriber's
        to the peer, we also add the given messages to our subscriber's queues.
        """
        self.connected_nodes[peer.remote] = peer
        self.logger.warning(
            'Adding %s to pool. Have %s peers', peer, len(self.connected_nodes)
        )
        # TODO: I think peer_info should only be called if this was an outgoing connection
        self.peer_info.connect_success(peer.remote)
        peer.add_finished_callback(self._peer_finished)
        for subscriber in self._subscribers:
            subscriber.register_peer(peer)
            peer.add_subscriber(subscriber)
            for msg in msgs:
                subscriber.add_msg(msg)

    async def _run(self) -> None:
        # FIXME: PeerPool should probably no longer be a BaseService, but for now we're keeping it
        # so in order to ensure we cancel all peers when we terminate.
        if self.event_bus is not None:
            self.run_daemon_task(self.handle_peer_count_requests())
            self.run_daemon_task(self.maybe_connect_more_peers())
            self.run_daemon_task(self.accept_connect_commands())
        self.run_daemon_task(self._periodically_report_stats())
        await self.cancel_token.wait()

    async def stop_all_peers(self) -> None:
        self.logger.info("Stopping all peers ...")
        peers = self.connected_nodes.values()
        await asyncio.gather(*[
            peer.disconnect(DisconnectReason.client_quitting) for peer in peers if peer.is_running
        ])

    async def _cleanup(self) -> None:
        await self.stop_all_peers()

    async def connect(self, remote: Node) -> BasePeer:
        """
        Connect to the given remote and return a Peer instance when successful.
        Returns None if the remote is unreachable, times out or is useless.
        """
        if remote in self.connected_nodes:
            self.logger.debug("Skipping %s; already connected to it", remote)
            return None
        if not self.peer_info.can_connect_to(remote):
            return None
        expected_exceptions = (
            HandshakeFailure,
            PeerConnectionLost,
            TimeoutError,
            UnreachablePeer,
        )
        try:
            self.logger.debug2("Connecting to %s...", remote)
            # We use self.wait() as well as passing our CancelToken to handshake() as a workaround
            # for https://github.com/ethereum/py-evm/issues/670.
            peer = await self.wait(handshake(remote, self.get_peer_factory()))

            return peer
        except OperationCancelled:
            # Pass it on to instruct our main loop to stop.
            raise
        except BadAckMessage:
            # This is kept separate from the `expected_exceptions` to be sure that we aren't
            # silencing an error in our authentication code.
            self.logger.error('Got bad auth ack from %r', remote)
            # dump the full stacktrace in the debug logs
            self.logger.debug('Got bad auth ack from %r', remote, exc_info=True)
        except MalformedMessage:
            # This is kept separate from the `expected_exceptions` to be sure that we aren't
            # silencing an error in how we decode messages during handshake.
            self.logger.error('Got malformed response from %r during handshake', remote)
            # dump the full stacktrace in the debug logs
            self.logger.debug('Got malformed response from %r', remote, exc_info=True)
        except WrongNetworkFailure as e:
            self.peer_info.wrong_network_failure(remote, e)
        except WrongGenesisFailure as e:
            self.peer_info.wrong_genesis_failure(remote, e)
        except HandshakeFailure as e:
            if 'too_many_peers' in repr(e):
                # TODO: this should become a separate exception?
                self.peer_info.too_many_peers(remote)
            else:
                self.peer_info.handshake_failure(remote, e)
                self.logger.debug("Could not complete handshake with %r: %s", remote, repr(e))
        except UnreachablePeer as e:
            self.peer_info.unreachable_peer(remote)
        except expected_exceptions as e:
            # TODO: should TimeoutError also be added to peer_info?
            self.logger.debug("Could not complete handshake with %r: %s", remote, repr(e))
        except Exception:
            self.logger.exception("Unexpected error during auth/p2p handshake with %r", remote)
        return None

    async def connect_to_nodes(self, nodes: Iterator[Node]) -> None:
        # To save on resources only attempt to connect to a few nodes at a time
        max_connect_concurrency = 10

        # TODO: turn this into a method on BaseService / CancelToken
        # TODO: this spams the log with unretrieved exceptions when we cancel
        async def wait_first(tasks: set):
            # TODO: we're turning a future which was turned into a coro into a future
            token_wait = asyncio.create_task(self.cancel_token.wait())
            try:
                done, remaining = await asyncio.wait(
                    tasks.union({token_wait}),
                    return_when=asyncio.FIRST_COMPLETED,
                )
            except asyncio.futures.CancelledError:
                for task in tasks:
                    task.cancel()
                raise
            token_wait.cancel()
            return done, remaining.difference({token_wait})

        async def bounded_wait(coros, concurrency: int):
            tasks = set()
            while coros or tasks:
                additional_tasks_required = concurrency - len(tasks)
                new_tasks, coros = (
                    coros[:additional_tasks_required], coros[additional_tasks_required:]
                )
                tasks = tasks.union(asyncio.create_task(coro) for coro in new_tasks)
                #_, tasks = await wait_first(tasks)
                _, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        async def connect(node):
            if self.is_full or not self.is_operational:
                return
            # TODO: Consider changing connect() to raise an exception instead of returning None,
            # as discussed in
            # https://github.com/ethereum/py-evm/pull/139#discussion_r152067425
            peer = await self.connect(node)
            if peer is not None:
                await self.start_peer(peer)

        coros = [connect(node) for node in nodes]
        await bounded_wait(coros, concurrency=max_connect_concurrency)

    def _peer_finished(self, peer: BaseService) -> None:
        """Remove the given peer from our list of connected nodes.
        This is passed as a callback to be called when a peer finishes.
        """
        peer = cast(BasePeer, peer)
        if peer.remote in self.connected_nodes:
            self.connected_nodes.pop(peer.remote)
            self.logger.warning(
                "%s finished, removing from pool. Have %s peers",
                peer, len(self.connected_nodes),
            )
        else:
            self.logger.warning(
                "%s finished but was not found in connected_nodes (%s)", peer, self.connected_nodes)
        for subscriber in self._subscribers:
            subscriber.deregister_peer(peer)

    def __aiter__(self) -> AsyncIterator[BasePeer]:
        return ConnectedPeersIterator(tuple(self.connected_nodes.values()))

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
                self.logger.info(
                    "Peer subscribers: %d, longest queue: %s(%d)",
                    subscribers, longest_queue.__class__.__name__, longest_queue.queue_size)

            self.logger.debug("== Peer details == ")
            for peer in self.connected_nodes.values():
                if not peer.is_running:
                    self.logger.warning(
                        "%s is no longer alive but has not been removed from pool", peer)
                    continue
                most_received_type, count = max(
                    peer.received_msgs.items(), key=operator.itemgetter(1))
                self.logger.debug(
                    "%s: uptime=%s, received_msgs=%d, most_received=%s(%d)",
                    peer, peer.uptime, peer.received_msgs_count,
                    most_received_type, count)
                for line in peer.get_extra_stats():
                    self.logger.debug("    %s", line)
            self.logger.debug("== End peer details == ")
            await self.sleep(self._report_interval)


class ConnectedPeersIterator(AsyncIterator[BasePeer]):

    def __init__(self, peers: Tuple[BasePeer, ...]) -> None:
        self.iter = iter(peers)

    async def __anext__(self) -> BasePeer:
        while True:
            # Yield control to ensure we process any disconnection requests from peers. Otherwise
            # we could return peers that should have been disconnected already.
            await asyncio.sleep(0)
            try:
                peer = next(self.iter)
                if not peer.is_closing:
                    return peer
            except StopIteration:
                raise StopAsyncIteration
