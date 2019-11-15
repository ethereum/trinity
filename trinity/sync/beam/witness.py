import asyncio
from collections import Counter, defaultdict
import typing
from typing import Any, Dict, FrozenSet, Iterable, NamedTuple, Set, Tuple, Type, cast

from cancel_token import CancelToken, OperationCancelled
from lahja import EndpointAPI

from eth.abc import AtomicDatabaseAPI
from eth_typing import Hash32
from p2p.abc import CommandAPI
from p2p.exceptions import BaseP2PError, PeerConnectionLost
from p2p.peer import BasePeer, PeerSubscriber
from p2p.service import BaseService
from trinity._utils.datastructures import TaskQueue
from trinity.protocol.eth import constants as eth_constants
from trinity.protocol.eth.peer import ETHPeer, ETHPeerPool
from trinity.protocol.fh.commands import NewBlockWitnessHashes
from trinity.protocol.fh.events import CreatedNewBlockWitnessHashes
from trinity.protocol.fh.peer import FirehosePeer
from trinity.protocol.fh.proto import FirehoseProtocol
from trinity.sync.beam.constants import (
    GAP_BETWEEN_WITNESS_DOWNLOADS,
    NON_IDEAL_RESPONSE_PENALTY,
    WITNESS_QUEUE_SIZE,
)

from .queen import QueeningQueue, QueenTrackerAPI


class NodeDownloadTask(NamedTuple):
    """Prioritize trie node downloads by block number"""
    node_hash: Hash32
    block_number: int

    @classmethod
    def nodes_at_block(
            cls,
            node_hashes: Iterable[Hash32],
            block_number: int) -> Tuple['NodeDownloadTask', ...]:

        return tuple(cls(node_hash, block_number) for node_hash in node_hashes)


class BeamStateWitnessCollector(BaseService, PeerSubscriber, QueenTrackerAPI):
    """
    Collect witnesses that peers claim we need. Along the way, track the speed
    of the fastest peer, so that they can be our go-to peer ("Queen" peer)
    when an actively imported block gets stuck waiting for data.
    """
    # We are only interested in witness metadata
    subscription_msg_types: FrozenSet[Type[CommandAPI[Any]]] = frozenset({NewBlockWitnessHashes})

    # This is a rather arbitrary value, but when the sync is operating normally we never see
    # the msg queue grow past a few hundred items, so this should be a reasonable limit for
    # now.
    msg_queue_maxsize: int = 2000

    _total_processed_nodes = 0
    _num_added = 0
    _num_missed = 0
    _report_interval = 10

    _num_requests_by_peer: typing.Counter[ETHPeer]

    def __init__(
            self,
            db: AtomicDatabaseAPI,
            peer_pool: ETHPeerPool,
            token: CancelToken = None) -> None:

        # in case there is no token set, make sure this gets cancelled when the peer pool does
        if token is None:
            token = peer_pool.cancel_token

        super().__init__(token=token)
        self._db = db
        self._peer_pool = peer_pool

        self._num_requests_by_peer = Counter()

        # {header_hash: {node_hash: num_peers_that_broadcast}}
        self._aggregated_witness_metadata: Dict[Hash32, Dict[Hash32, int]] = defaultdict(Counter)

        # Track the nodes to download for the witness

        self._witness_node_tasks = TaskQueue[NodeDownloadTask](
            WITNESS_QUEUE_SIZE,
            lambda task: task.block_number,
        )

        self._queening_queue = QueeningQueue(peer_pool, token=token)

    async def get_queen_peer(self) -> ETHPeer:
        return await self._queening_queue.get_queen_peer()

    def penalize_queen(self, peer: ETHPeer) -> None:
        self._queening_queue.penalize_queen(peer)

    async def trigger_download(self, block_hash: Hash32, block_number: int, urgent: bool) -> None:
        """
        Identify witness for the related block, and add it to the download queue
        """
        # There is an opportunity to do neat stuff here preferring nodes that
        #   more peers are announcing. For now, do the simple thing and get all nodes.
        node_hashes = self._aggregated_witness_metadata[block_hash].keys()
        node_tasks = NodeDownloadTask.nodes_at_block(node_hashes, block_number)

        if not len(node_tasks):
            # there aren't any available nodes to trigger a download for, so end early
            return

        new_node_tasks = tuple(
            task for task in node_tasks
            if task.node_hash not in self._db and task not in self._witness_node_tasks
        )

        await self._witness_node_tasks.add(new_node_tasks)

    async def _collect_peer_witness_metadata(self) -> None:
        while self.is_operational:
            peer, cmd, msg = await self.wait(self.msg_queue.get())
            counter = self._aggregated_witness_metadata[msg.get('block_hash')]
            counter.update(msg['node_hashes'])

    async def _run(self) -> None:
        self.run_daemon_task(self._periodically_report_progress())

        self.run_daemon(self._queening_queue)

        with self.subscribe(self._peer_pool):
            self.run_daemon_task(self._collect_peer_witness_metadata())
            await self.wait(self._collect_witnesses())

    async def _collect_witnesses(self) -> None:
        while self.is_operational:
            peer = await self._queening_queue.pop_fastest_peasant()

            batch_id, urgent_hash_tasks = await self.wait(
                self._witness_node_tasks.get(eth_constants.MAX_STATE_FETCH),
            )

            self.run_task(self._make_request(peer, urgent_hash_tasks, batch_id))

    async def _make_request(
            self,
            peer: ETHPeer,
            request_tasks: Tuple[NodeDownloadTask, ...], batch_id: int) -> None:

        self._num_requests_by_peer[peer] += 1
        try:
            nodes = await peer.eth_api.get_node_data(
                tuple(set(task.node_hash for task in request_tasks))
            )
        except asyncio.TimeoutError:
            self._witness_node_tasks.complete(batch_id, ())
            self._queening_queue.readd_peasant(peer, NON_IDEAL_RESPONSE_PENALTY)
        except (PeerConnectionLost, OperationCancelled):
            # Something unhappy, but we don't really care, peer will be gone by next loop
            self._witness_node_tasks.complete(batch_id, ())
        except (BaseP2PError, Exception) as exc:
            self.logger.info("Unexpected err while getting witness nodes from %s: %s", peer, exc)
            self.logger.debug("Problem downloading witness nodes from peer...", exc_info=True)
            self._witness_node_tasks.complete(batch_id, ())
            self._queening_queue.readd_peasant(peer, NON_IDEAL_RESPONSE_PENALTY)
        else:
            self._queening_queue.readd_peasant(peer, GAP_BETWEEN_WITNESS_DOWNLOADS)

            node_lookup = dict(nodes)
            completed_tasks = tuple(
                task for task in request_tasks if task.node_hash in node_lookup
            )
            self._witness_node_tasks.complete(batch_id, completed_tasks)
            self._insert_results(tuple(node_lookup.keys()), node_lookup)

    def _insert_results(
            self,
            requested_hashes: Tuple[Hash32, ...],
            returned_nodes: Dict[Hash32, bytes]) -> None:

        with self._db.atomic_batch() as write_batch:
            for requested_hash in requested_hashes:
                if requested_hash in returned_nodes:
                    self._num_added += 1
                    self._total_processed_nodes += 1
                    encoded_node = returned_nodes[requested_hash]
                    write_batch[requested_hash] = encoded_node
                else:
                    self._num_missed += 1

    async def _periodically_report_progress(self) -> None:
        while self.is_operational:
            await self.sleep(self._report_interval)

            msg = "all=%d" % self._total_processed_nodes
            msg += "  new=%d" % self._num_added
            msg += "  missed=%d" % self._num_missed
            msg += "  queen=%s" % self._queening_queue.queen
            self.logger.debug("Witness-Filler: %s", msg)

            self._num_added = 0
            self._num_missed = 0

            # log peer counts
            show_top_n_peers = 3
            self.logger.debug(
                "Witness-Filler-Top-%d-Peers: %s",
                show_top_n_peers,
                self._num_requests_by_peer.most_common(show_top_n_peers),
            )
            self._num_requests_by_peer.clear()


class WitnessBroadcaster(BaseService, PeerSubscriber):
    """
    Broadcast block witness node hashes to all Firehose-compatible peers.
    """

    # We are only interested in peers entering or leaving the pool
    subscription_msg_types: FrozenSet[Type[CommandAPI[Any]]] = frozenset()

    # This is a rather arbitrary value, but when the sync is operating normally we never see
    # the msg queue grow past a few hundred items, so this should be a reasonable limit for
    # now.
    msg_queue_maxsize: int = 2000

    _firehose_peers: Set[FirehosePeer]

    def __init__(
            self,
            peer_pool: ETHPeerPool,
            event_bus: EndpointAPI,
            token: CancelToken = None) -> None:
        super().__init__(token)
        self._peer_pool = peer_pool
        self._event_bus = event_bus

        self._firehose_peers = set()

    def register_peer(self, peer: BasePeer) -> None:
        super().register_peer(peer)
        # This service is only interested in peers that implement firehose
        if peer.connection.has_protocol(FirehoseProtocol):
            self.logger.warning("Added Firehose peer: %s", peer)
            self._firehose_peers.add(cast(FirehosePeer, peer))
        else:
            self.logger.warning(
                "Ignoring peer for witness broadcast, not firehose-enabled: %s",
                peer,
            )

    def deregister_peer(self, peer: BasePeer) -> None:
        super().deregister_peer(peer)
        if peer in self._firehose_peers:
            self.logger.warning("Removing Firehose peer: %s", peer)
            self._firehose_peers.remove(cast(FirehosePeer, peer))

    async def _broadcast_new_witnesses(self) -> None:
        async for event in self.wait_iter(self._event_bus.stream(CreatedNewBlockWitnessHashes)):
            if not event.witness_hashes:
                self.logger.warning("Witness metadata for %s is completely empty", event.block)
            else:
                self.logger.warning(
                    "Witness broadcastor received block import complete event: %s",
                    event.block,
                )
                self._broadcast_witness_metadata(event.block.hash, event.witness_hashes)

    def _broadcast_witness_metadata(
            self, header_hash: Hash32, witness_hashes: Tuple[Hash32, ...]) -> None:

        for peer in self._firehose_peers:
            self.logger.warning("Sending %d hashes of witness to: %s", len(witness_hashes), peer)
            peer.fh_api.send_new_block_witness_hashes(header_hash, witness_hashes)

    async def _run(self) -> None:
        """
        Request all nodes in the queue, running indefinitely
        """
        self.logger.info("Starting witness metadata broadcaster")
        with self.subscribe(self._peer_pool):
            await self._broadcast_new_witnesses()
