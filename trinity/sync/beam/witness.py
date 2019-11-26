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

from cancel_token import CancelToken, OperationCancelled
from eth_utils import encode_hex
from lahja import EndpointAPI

from eth.abc import AtomicDatabaseAPI
from eth.rlp.blocks import BaseBlock
from eth_typing import Hash32

from p2p.abc import CommandAPI
from p2p.disconnect import DisconnectReason
from p2p.exceptions import BaseP2PError, PeerConnectionLost
from p2p.peer import BasePeer, PeerSubscriber
from p2p.service import BaseService
from trinity._utils.datastructures import TaskQueue
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
    _firehose_peers: Set[ETHPeer]

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
        self._aggregated_witness_metadata: DefaultDict[Hash32, typing.Counter[Hash32]]
        self._aggregated_witness_metadata = defaultdict(Counter)

        # Track the nodes to download for the witness

        self._witness_node_tasks = TaskQueue[NodeDownloadTask](
            WITNESS_QUEUE_SIZE,
            lambda task: task.block_number,
        )

        self._queening_queue = QueeningQueue(peer_pool, token=self.cancel_token)
        self._block_number_lookup: Dict[Hash32, Tuple[int, bool]] = {}  # TODO: delete after import

        self._firehose_peers = set()
        self._highest_block_number = -1
        self._lowest_block_number: int = None

    async def get_queen_peer(self) -> ETHPeer:
        return await self._queening_queue.get_queen_peer()

    def penalize_queen(self, peer: ETHPeer) -> None:
        self._queening_queue.penalize_queen(peer)

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

        self.logger.warning("Triggering download for block %d:%s...", block_number, encode_hex(block_hash[:3]))
        await self._witness_node_tasks.add(new_node_tasks)

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
            self.logger.warning("Removing Firehose peer: %s", peer)
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
            self.run_task(self.trigger_download(block_hash, block_number, urgent))

    async def _collect_peer_witness_metadata(self) -> None:
        while self.is_operational:
            peer, cmd = await self.wait(self.msg_queue.get())
            new_hashes = cast(NewBlockWitnessHashes, cmd)
            if peer not in self._firehose_peers:
                self.logger.warning("Handling new hashes from %s not recognized as a firehose peer. Huh?", peer)
                await self._handle_new_hashes(new_hashes.payload)

    async def _run(self) -> None:
        self.run_daemon_task(self._periodically_report_progress())

        self.run_daemon(self._queening_queue)

        with self.subscribe(self._peer_pool):
            self.run_daemon_task(self._collect_peer_witness_metadata())
            await self.wait(self._collect_witnesses())

    async def _collect_witnesses(self) -> None:
        while self.is_operational:
            peer = await self.wait(self._queening_queue.pop_fastest_peasant())

            remaining_tasks = len(self._witness_node_tasks)
            if remaining_tasks == 0:
                self.logger.warning("There are no witness tasks to pop off. Pausing...")

            batch_id, urgent_hash_tasks = await self.wait(
                self._witness_node_tasks.get(eth_constants.MAX_STATE_FETCH),
            )

            self.run_task(self._make_request(peer, urgent_hash_tasks, batch_id))

    async def _make_request(
            self,
            peer: ETHPeer,
            request_tasks: Tuple[NodeDownloadTask, ...], batch_id: int) -> None:

        self.logger.debug("Requesting %d nodes from %s", len(request_tasks), peer)
        try:
            nodes = await peer.eth_api.get_node_data(
                tuple(set(task.node_hash for task in request_tasks))
            )
        except asyncio.TimeoutError:
            self.logger.warning("%s timeout error during witness collection", peer)
            self._witness_node_tasks.complete(batch_id, ())
            self._queening_queue.readd_peasant(peer, NON_IDEAL_RESPONSE_PENALTY)
        except (PeerConnectionLost, OperationCancelled):
            # Something unhappy, but we don't really care, peer will be gone by next loop
            self.logger.warning("%s cancellation during witness collection", peer)
            self._witness_node_tasks.complete(batch_id, ())
        except (BaseP2PError, Exception) as exc:
            self.logger.info("Unexpected err while getting witness nodes from %s: %s", peer, exc)
            self.logger.debug("Problem downloading witness nodes from peer...", exc_info=True)
            self._witness_node_tasks.complete(batch_id, ())
            self._queening_queue.readd_peasant(peer, NON_IDEAL_RESPONSE_PENALTY)
        else:
            node_lookup = dict(nodes)
            num_nodes = len(node_lookup)
            if num_nodes > 0:
                self._queening_queue.readd_peasant(peer, GAP_BETWEEN_WITNESS_DOWNLOADS)
            else:
                self._queening_queue.readd_peasant(peer, NON_IDEAL_RESPONSE_PENALTY)

            completed_tasks = set(
                task for task in request_tasks
                if task.node_hash in node_lookup
            )
            side_channel_tasks = set(
                task for task in request_tasks
                if task.node_hash in self._db
            )
            # self._lowest_block_number must be set here, because the download shouldn't happen
            # until a trigger, which sets lowest block number
            stale_tasks = set(
                task for task in request_tasks
                if task.block_number < self._highest_block_number - NUM_BLOCKS_WITH_DOWNLOADABLE_STATE or task.block_number < self._lowest_block_number  # noqa: E501
            )

            closing_tasks_set = completed_tasks | side_channel_tasks | stale_tasks
            remaining_tasks = set(request_tasks) - closing_tasks_set
            if len(closing_tasks_set):
                self.logger.info(
                    "%s returned %d witness nodes, with %d completed tasks w/ block #s %r, %d side-channeled tasks w/ block #s %r, and %d stale tasks w/ block #s %r, for a total of %d closed tasks; %d tasks remain from block #s %r",
                    peer,
                    num_nodes,
                    len(completed_tasks),
                    set(task.block_number for task in completed_tasks),
                    len(side_channel_tasks),
                    set(task.block_number for task in side_channel_tasks),
                    len(stale_tasks),
                    set(task.block_number for task in stale_tasks),
                    len(closing_tasks_set),
                    len(remaining_tasks),
                    set(task.block_number for task in remaining_tasks),
                )
            else:
                self.logger.debug("%s returned no witness nodes, and nothing else interesting happened", peer)

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
        while self.is_operational:
            await self.sleep(self._report_interval)

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

    _firehose_peers: Set[ETHPeer]

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
            fh_peer = cast(ETHPeer, peer)
            self.logger.warning("Added *confirmed* Firehose for broadcast: %s", fh_peer)
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
        async for event in self.wait_iter(self._event_bus.stream(StatelessBlockImportDone)):
            if not event.witness_hashes:
                self.logger.warning("Witness metadata for %s is empty", event.block)
            else:
                await self._broadcast_witness_metadata(event.block, event.witness_hashes)

    async def _broadcast_witness_metadata(
            self, block: BaseBlock, witness_hashes: Tuple[Hash32, ...]) -> None:

        if len(self._firehose_peers) == 0:
            self.logger.info(
                "Skipping witness broadcast for %s, because not connected to any peers",
                block,
            )
        for peer in self._firehose_peers:
            self.logger.warning("Sending %d hashes of witness to: %s", len(witness_hashes), peer)
            try:
                peer.fh_api.send_new_block_witness_hashes(block.hash, witness_hashes)
            except asyncio.TimeoutError as err:
                self.logger.debug("Timed out broadcasting witness to %s", peer)
                continue
            except CancelledError:
                self.logger.debug("Peer %s witness broadcast cancelled", peer)
                raise
            except OperationCancelled:
                self.logger.debug("Peer %s witness broadcast cancelled via service", peer)
                raise
            except PeerConnectionLost as e:
                self.logger.warning("Peer %s went away, dropping the witness broadcast and peer", peer, exc_info=True)
                await peer.disconnect(DisconnectReason.TIMEOUT)
                self.logger.warning("Peer %s successfully disconnected", peer)
                continue
            except Exception:
                self.logger.exception("Unknown error when broadcasting witness")
                raise

    async def _run(self) -> None:
        """
        Request all nodes in the queue, running indefinitely
        """
        self.logger.info("Starting witness metadata broadcaster")
        with self.subscribe(self._peer_pool):
            await self.wait(self._broadcast_new_witnesses())
