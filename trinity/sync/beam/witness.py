import asyncio
from collections import Counter, defaultdict
from concurrent.futures import CancelledError
import typing
from typing import (
    Any,
    DefaultDict,
    Dict,
    FrozenSet,
    Iterable,
    NamedTuple,
    Set,
    Tuple,
    Type,
    cast,
)

from async_service import Service

from eth_utils import encode_hex

from lahja import EndpointAPI

from eth.abc import (
    AtomicDatabaseAPI,
    BlockAPI,
)
from eth_typing import Hash32

from p2p.abc import CommandAPI
from p2p.disconnect import DisconnectReason
from p2p.exceptions import BaseP2PError, PeerConnectionLost
from p2p.peer import BasePeer, PeerSubscriber
from trinity._utils.datastructures import TaskQueue
from trinity._utils.logging import get_logger
from trinity.protocol.eth import constants as eth_constants
from trinity.protocol.eth.peer import ETHPeer, ETHPeerPool
from trinity.protocol.fh.commands import NewBlockWitnessHashes, NewBlockWitnessHashesPayload
from trinity.protocol.fh.proto import FirehoseProtocol
from trinity.sync.beam.constants import (
    GAP_BETWEEN_WITNESS_DOWNLOADS,
    NON_IDEAL_RESPONSE_PENALTY,
    NUM_BLOCKS_WITH_DOWNLOADABLE_STATE,
    WITNESS_QUEUE_SIZE,
)
from trinity.sync.common.events import StatelessBlockImportDone

from .queen import QueeningQueue, QueenTrackerAPI


class WitnessBroadcaster(Service, PeerSubscriber):
    """
    Broadcast block witness node hashes to all Firehose-compatible peers.
    """

    # We are only interested in peers entering or leaving the pool
    subscription_msg_types: FrozenSet[Type[CommandAPI[Any]]] = frozenset()

    # This is a rather arbitrary value, but when the sync is operating normally we never see
    # the msg queue grow past a few hundred items, so this should be a reasonable limit for
    # now.
    msg_queue_maxsize: int = 2000

    _firehose_peers: Set[ETHPeer]

    def __init__(self, peer_pool: ETHPeerPool, event_bus: EndpointAPI) -> None:
        self.logger = get_logger('trinity.sync.beam.WitnessBroadcaster')
        self._peer_pool = peer_pool
        self._event_bus = event_bus

        self._firehose_peers = set()

    def register_peer(self, peer: BasePeer) -> None:
        super().register_peer(peer)
        # This service is only interested in peers that implement firehose
        if peer.connection.has_protocol(FirehoseProtocol):
            fh_peer = cast(ETHPeer, peer)
            self.logger.debug("Added *confirmed* Firehose for broadcast: %s", fh_peer)
            if fh_peer in self._firehose_peers:
                self.logger.warning("%s was already in the set of Firehose peers", fh_peer)
            else:
                self._firehose_peers.add(fh_peer)

    def deregister_peer(self, peer: BasePeer) -> None:
        super().deregister_peer(peer)
        if peer in self._firehose_peers:
            self.logger.warning("Removing Firehose peer: %s", peer)
            self._firehose_peers.remove(cast(ETHPeer, peer))

    async def _broadcast_new_witnesses(self) -> None:
        async for event in self._event_bus.stream(StatelessBlockImportDone):
            if not event.witness_hashes:
                self.logger.warning("Witness metadata for %s is empty", event.block)
            else:
                await self._broadcast_witness_metadata(event.block, event.witness_hashes)

    async def _broadcast_witness_metadata(
            self, block: BlockAPI, witness_hashes: Tuple[Hash32, ...]) -> None:

        # make a copy, so the set doesn't modify during loop
        eligible_peers = tuple(self._firehose_peers)

        if len(eligible_peers) == 0:
            self.logger.debug(
                "Skipping witness broadcast for %s, because not connected to any peers",
                block,
            )

        for peer in eligible_peers:
            self.logger.info("Sending %d hashes of witness to: %s", len(witness_hashes), peer)
            try:
                peer.fh_api.send_new_block_witness_hashes(block.hash, witness_hashes)
            except asyncio.TimeoutError:
                self.logger.debug("Timed out broadcasting witness to %s", peer)
                continue
            except CancelledError:
                self.logger.debug("Peer %s witness broadcast cancelled", peer)
                raise
            except PeerConnectionLost:
                self.logger.warning(
                    "Peer %s went away, dropping the witness broadcast and peer",
                    peer,
                    exc_info=True,
                )
                await peer.disconnect(DisconnectReason.TIMEOUT)
                self.logger.warning("Peer %s successfully disconnected", peer)
                continue
            except Exception:
                self.logger.exception("Unknown error when broadcasting witness")
                raise

    async def run(self) -> None:
        """
        Request all nodes in the queue, running indefinitely
        """
        self.logger.info("Starting witness metadata broadcaster")
        with self.subscribe(self._peer_pool):
            await self._broadcast_new_witnesses()


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


class BeamStateWitnessCollector(Service, PeerSubscriber, QueenTrackerAPI):
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
    _firehose_peers: Set[ETHPeer]

    def __init__(self, db: AtomicDatabaseAPI, peer_pool: ETHPeerPool) -> None:
        self.logger = get_logger('trinity.sync.beam.BeamStateWitnessCollector')
        self._db = db
        self._peer_pool = peer_pool

        self._num_requests_by_peer = Counter()

        # {header_hash: {node_hash: num_peers_that_broadcast}}
        self._aggregated_witness_metadata: DefaultDict[Hash32, typing.Counter[Hash32]]
        self._aggregated_witness_metadata = defaultdict(Counter)

        # Track the nodes to download for the witness

        self._witness_node_tasks = TaskQueue[NodeDownloadTask](
            WITNESS_QUEUE_SIZE,
            lambda task: task.block_number,
        )

        self._queening_queue = QueeningQueue(peer_pool)
        self._block_number_lookup: Dict[Hash32, Tuple[int, bool]] = {}  # TODO: delete after import

        self._firehose_peers = set()
        self._highest_block_number = -1
        self._lowest_block_number: int = None

    async def get_queen_peer(self) -> ETHPeer:
        return await self._queening_queue.get_queen_peer()

    def penalize_queen(self, peer: ETHPeer, delay: float = NON_IDEAL_RESPONSE_PENALTY) -> None:
        self._queening_queue.penalize_queen(peer, delay=delay)

    def insert_peer(self, peer: ETHPeer, delay: float = 0) -> None:
        self._queening_queue.insert_peer(peer, delay=delay)

    async def pop_fastest_peasant(self) -> ETHPeer:
        # XXX: Do we need to do something simillar to the backfiller here?
        # async with self._external_peasant_usage.make_noise():
        return await self._queening_queue.pop_fastest_peasant()

    def set_desired_knight_count(self, desired_knights: int) -> None:
        self._queening_queue.set_desired_knight_count(desired_knights)

    async def trigger_download(self, block_hash: Hash32, block_number: int, urgent: bool) -> None:
        """
        Identify witness for the related block, and add it to the download queue
        """
        node_hashes = self._get_node_hashes(block_hash)
        node_tasks = NodeDownloadTask.nodes_at_block(node_hashes, block_number)

        self._highest_block_number = max(block_number, self._highest_block_number)
        if self._lowest_block_number is None:
            self._lowest_block_number = block_number
        else:
            self._lowest_block_number = min(block_number, self._lowest_block_number)

        if urgent or block_hash not in self._block_number_lookup:
            self._block_number_lookup[block_hash] = (block_number, urgent)
            '''
            self.logger.warning(
                "Adding (urgent? %s) block number lookup for hash %s...",
                "Y" if urgent else "N",
                encode_hex(block_hash[:3]),
            )
'''

        if not len(node_tasks):
            # there aren't any available nodes to trigger a download for, so end early
            return

        new_node_tasks = tuple(
            task for task in node_tasks
            if task.node_hash not in self._db and task not in self._witness_node_tasks
        )

        if new_node_tasks:
            self.logger.warning(
                "Triggering download of %d nodes for block %d:%s...",
                len(new_node_tasks),
                block_number,
                encode_hex(block_hash[:3]),
            )
            await self._witness_node_tasks.add(new_node_tasks)
        else:
            self.logger.warning(
                "All witness data for block %d:%s is available, ignoring",
                block_number,
                encode_hex(block_hash[:3]),
            )

    def _get_node_hashes(self, block_hash: Hash32) -> Tuple[Hash32, ...]:
        # Potential future option when msg_queue stops working:
        '''
        return tuple(set(concat(
            peer.witnesses.get_node_hashes(block_hash)
            for peer in self._firehose_peers
            if peer.witnesses.has_witness(block_hash)
        )))
        '''

        # There is an opportunity to do neat stuff here preferring nodes that
        #   more peers are announcing. For now, do the simple thing and get all nodes.
        return tuple(self._aggregated_witness_metadata[block_hash].keys())

    def register_peer(self, peer: BasePeer) -> None:
        super().register_peer(peer)
        # This service is only interested in peers that implement firehose
        if peer.connection.has_protocol(FirehoseProtocol):
            fh_peer = cast(ETHPeer, peer)
            self.logger.warning("Added *confirmed* Firehose for collection: %s", fh_peer)
            if fh_peer in self._firehose_peers:
                self.logger.warning("%s was already in the set of Firehose peers", fh_peer)
            else:
                self._firehose_peers.add(fh_peer)
                fh_peer.fh_api.witnesses.subscribe(self._handle_new_hashes)

    def deregister_peer(self, peer: BasePeer) -> None:
        super().deregister_peer(peer)
        if peer in self._firehose_peers:
            self.logger.debug("Removing Firehose peer: %s", peer)
            self._firehose_peers.remove(cast(ETHPeer, peer))

    async def _handle_new_hashes(self, payload: NewBlockWitnessHashesPayload) -> None:
        block_hash, node_hashes = payload
        counter = self._aggregated_witness_metadata[block_hash]
        counter.update(node_hashes)
        self.logger.info(
            "Received %d witness hashes for block hash %s...",
            len(node_hashes),
            encode_hex(block_hash[:3]),
        )

        if block_hash in self._block_number_lookup:
            block_number, urgent = self._block_number_lookup[block_hash]
            self.logger.warning(
                "Block already triggered for #%d as %surgent, so immediately starting download",
                block_number,
                '' if urgent else 'non-',
            )
            self.manager.run_task(self.trigger_download, block_hash, block_number, urgent)

    async def _collect_peer_witness_metadata(self) -> None:
        while self.manager.is_running:
            peer, cmd = await self.msg_queue.get()
            new_hashes = cast(NewBlockWitnessHashes, cmd)
            if peer not in self._firehose_peers:
                self.logger.warning(
                    "Handling new hashes from %s not recognized as a firehose peer. Huh?", peer)
                await self._handle_new_hashes(new_hashes.payload)

    async def run(self) -> None:
        self.manager.run_daemon_task(self._periodically_report_progress)

        self.manager.run_daemon_child_service(self._queening_queue)

        with self.subscribe(self._peer_pool):
            self.manager.run_daemon_task(self._collect_peer_witness_metadata)
            await self._collect_witnesses()

    async def _collect_witnesses(self) -> None:
        while self.manager.is_running:
            peer = await self._queening_queue.pop_fastest_peasant()

            remaining_tasks = len(self._witness_node_tasks)
            if remaining_tasks == 0:
                self.logger.warning("There are no witness tasks to pop off. Pausing...")

            batch_id, urgent_hash_tasks = await self._witness_node_tasks.get(
                eth_constants.MAX_STATE_FETCH)

            self.manager.run_task(self._make_request, peer, urgent_hash_tasks, batch_id)

    async def _make_request(
            self,
            peer: ETHPeer,
            request_tasks: Tuple[NodeDownloadTask, ...], batch_id: int) -> None:

        pre_side_channelled_tasks = set(
            task for task in request_tasks
            if task.node_hash in self._db
        )
        unknown_node_tasks = tuple(
            task for task in request_tasks
            if task.node_hash not in self._db
        )

        if not unknown_node_tasks:
            self._queening_queue.insert_peer(peer, GAP_BETWEEN_WITNESS_DOWNLOADS)

            self.logger.info(
                "Skipped all %d trie node download tasks as side-channelled, from blocks %r",
                len(pre_side_channelled_tasks),
                set(task.block_number for task in pre_side_channelled_tasks),
            )
            self._witness_node_tasks.complete(batch_id, pre_side_channelled_tasks)
            # Early return, because we don't want to make any request to the peer
            return

        # TODO: This is duplicated from other services... Check that they are identical

        self.logger.debug("Requesting %d nodes from %s", len(unknown_node_tasks), peer)
        try:
            nodes = await peer.eth_api.get_node_data(
                tuple(set(task.node_hash for task in request_tasks))
            )
        except asyncio.TimeoutError:
            self.logger.warning("%s timeout error during witness collection", peer)
            self._witness_node_tasks.complete(batch_id, ())
            self._queening_queue.insert_peer(peer, NON_IDEAL_RESPONSE_PENALTY)
        except PeerConnectionLost:
            # Something unhappy, but we don't really care, peer will be gone by next loop
            self.logger.warning("%s cancellation during witness collection", peer)
            self._witness_node_tasks.complete(batch_id, ())
        except (BaseP2PError, Exception) as exc:
            self.logger.info("Unexpected err while getting witness nodes from %s: %s", peer, exc)
            self.logger.debug("Problem downloading witness nodes from peer...", exc_info=True)
            self._witness_node_tasks.complete(batch_id, ())
            self._queening_queue.insert_peer(peer, NON_IDEAL_RESPONSE_PENALTY)
        else:
            node_lookup = dict(nodes)
            num_nodes = len(node_lookup)
            if num_nodes > 0:
                self._queening_queue.insert_peer(peer, GAP_BETWEEN_WITNESS_DOWNLOADS)
            else:
                self._queening_queue.insert_peer(peer, NON_IDEAL_RESPONSE_PENALTY)

            completed_tasks = set(
                task for task in request_tasks
                if task.node_hash in node_lookup
            )

            # find just the tasks that were side-channeled during the request
            side_channel_tasks = set(
                task for task in request_tasks
                if task.node_hash in self._db
            ) - pre_side_channelled_tasks

            # self._lowest_block_number must be set here, because the download shouldn't happen
            # until a trigger, which sets lowest block number
            stale_tasks = set(
                task for task in request_tasks
                if task.block_number < self._highest_block_number - NUM_BLOCKS_WITH_DOWNLOADABLE_STATE or task.block_number < self._lowest_block_number  # noqa: E501
            )

            closing_tasks_set = (
                completed_tasks | side_channel_tasks | stale_tasks | pre_side_channelled_tasks)
            remaining_tasks = set(request_tasks) - closing_tasks_set
            if len(closing_tasks_set):
                self.logger.info(
                    "%s returned %d witness nodes, with %d completed tasks w/ block #s %r, " +
                    "%d pre-side-channeled tasks w/ block #s %r, %d side-channeled tasks w/ " +
                    "block #s %r, and %d stale tasks w/ block #s %r, for a total of %d closed " +
                    "tasks; %d tasks remain from block #s %r",
                    peer,
                    num_nodes,
                    len(completed_tasks),
                    set(task.block_number for task in completed_tasks),
                    len(pre_side_channelled_tasks),
                    set(task.block_number for task in pre_side_channelled_tasks),
                    len(side_channel_tasks),
                    set(task.block_number for task in side_channel_tasks),
                    len(stale_tasks),
                    set(task.block_number for task in stale_tasks),
                    len(closing_tasks_set),
                    len(remaining_tasks),
                    set(task.block_number for task in remaining_tasks),
                )
            else:
                self.logger.debug(
                    "%s returned no witness nodes, and nothing else interesting happened", peer)

            closing_tasks = tuple(closing_tasks_set)
            self._witness_node_tasks.complete(batch_id, closing_tasks)
            self._insert_results(tuple(node_lookup.keys()), node_lookup)
            self._num_requests_by_peer[peer] += num_nodes

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
        while self.manager.is_running:
            await asyncio.sleep(self._report_interval)

            msg = "all=%d" % self._total_processed_nodes
            msg += "  new=%d" % self._num_added
            msg += "  missed=%d" % self._num_missed
            msg += "  pending=%d" % len(self._witness_node_tasks)
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
