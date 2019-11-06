from abc import ABC, abstractmethod
import asyncio
from collections import Counter, defaultdict
import typing
from typing import (
    FrozenSet,
    Iterable,
    List,
    Set,
    Tuple,
    Type,
)

from cancel_token import CancelToken, OperationCancelled
from eth.abc import AtomicDatabaseAPI
from eth_typing import Hash32
import rlp

from p2p.abc import CommandAPI
from p2p.exceptions import BaseP2PError, PeerConnectionLost
from p2p.exchange import PerformanceAPI
from p2p.peer import BasePeer, PeerSubscriber
from p2p.service import BaseService

from trinity.protocol.eth.commands import (
    NodeData,
)
from trinity.protocol.eth.peer import ETHPeer, ETHPeerPool
from trinity.protocol.fh.commands import NewBlockWitnessHashes
from trinity.sync.beam.constants import (
    GAP_BETWEEN_TESTS,
    NON_IDEAL_RESPONSE_PENALTY,
)
from trinity.sync.common.peers import WaitingPeers

from .queen import QueenTrackerMixin

### dups? unnecessary?
import asyncio
from collections import Counter
from concurrent.futures import CancelledError
import itertools
import typing
from typing import (
    FrozenSet,
    Iterable,
    Set,
    Tuple,
    Type,
)

from lahja import EndpointAPI

from eth_hash.auto import keccak
from eth_utils import (
    to_checksum_address,
    ValidationError,
)
from eth_typing import (
    Address,
    Hash32,
)

from eth.abc import AtomicDatabaseAPI

from cancel_token import CancelToken, OperationCancelled

from p2p.abc import CommandAPI
from p2p.exceptions import BaseP2PError, PeerConnectionLost
from p2p.peer import BasePeer, PeerSubscriber
from p2p.service import BaseService

from trie import HexaryTrie
from trie.exceptions import MissingTrieNode

from trinity._utils.datastructures import TaskQueue
from trinity._utils.timer import Timer
from trinity.protocol.common.typing import (
    NodeDataBundles,
)
from trinity.protocol.eth.commands import (
    NodeData,
)
from trinity.protocol.eth.constants import (
    MAX_STATE_FETCH,
)
from trinity.protocol.eth.peer import ETHPeer, ETHPeerPool
from trinity.protocol.eth import (
    constants as eth_constants,
)
from trinity.protocol.fh import (
    CreatedNewBlockWitnessHashes,
)

from trinity.sync.common.event import StatelessBlockImportDone
from trinity.sync.common.peers import WaitingPeers



class NodeDownloadTask(NamedTuple):
    """Prioritize trie node downloads by block number"""
    node_hash: Hash32
    block_number: int

    @classmethod
    def nodes_at_block(cls, node_hashes: Iterable[Hash32], block_number: int) -> Tuple[NodeDownloadTask, ...]:
        return tuple(cls(node_hash, block_number) for node_hash in node_hashes)


class BeamStateWitnessCollector(BaseService, PeerSubscriber, QueenTrackerMixin):
    """
    Collect witnesses that peers claim we need. Along the way, track the speed
    of the fastest peer, so that they can be our go-to peer ("Queen" peer)
    when an actively imported block gets stuck waiting for data.
    """
    # We are only interested in witness metadata
    subscription_msg_types: FrozenSet[Type[CommandAPI]] = frozenset({NewBlockWitnessHashes})

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

        self._waiting_peers = WaitingPeers[ETHPeer](NodeData, sort_key=_get_items_per_second)

        self._num_requests_by_peer = Counter()

        # {header_hash: {node_hash: num_peers_that_broadcast}}
        self._aggregated_witness_metadata = defaultdict(Counter)

        # Track the nodes to download for the witness

        # How many pending witness node hashes can we have?
        buffer_size = 10000 * 200  # 10k hashes is high-end for typical block, 200 blocks would be very far behind
        self._witness_node_tasks = TaskQueue[Hash32](buffer_size, NodeDownloadTask.block_number.fget)

    async def trigger_download(block_hash: Hash32, block_number: int, urgent: bool) -> None:
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

        with self.subscribe(self._peer_pool):
            self.run_daemon_task(self._collect_peer_witness_metadata())
            await self.wait(self._collect_witnesses())

    async def _collect_witnesses(self) -> None:
        while self.is_operational:

            peer = await self._waiting_peers.get_fastest()
            if not peer.is_operational:
                # drop any peers that aren't alive anymore
                self.logger.warning("Dropping %s from backfill as no longer operational", peer)
                if peer == self._queen_peer:
                    self._queen_peer = None
                continue

            old_queen = self._queen_peer
            self._update_queen(peer)
            if peer == self._queen_peer:
                self.logger.debug("Switching queen peer from %s to %s", old_queen, peer)
                continue

            if peer.eth_api.get_node_data.is_requesting:
                # skip the peer if there's an active request
                self.logger.debug("Backfill is skipping active peer %s", peer)
                self.call_later(10, self._waiting_peers.put_nowait, peer)
                continue

            batch_id, urgent_hash_tasks = await self.wait(
                self._witness_node_tasks.get(eth_constants.MAX_STATE_FETCH),
            )

            self.run_task(self._make_request(peer, urgent_hash_tasks, block_number, batch_id))

    async def _make_request(self, peer: ETHPeer, request_hashes: Tuple[Hash32, ...], batch_id: int) -> None:
        self._num_requests_by_peer[peer] += 1
        try:
            nodes = await peer.eth_api.get_node_data(request_hashes)
        except asyncio.TimeoutError:
            self._witness_node_tasks.complete(batch_id, ())
            self.call_later(NON_IDEAL_RESPONSE_PENALTY, self._waiting_peers.put_nowait, peer)
        except (PeerConnectionLost, OperationCancelled):
            # Something unhappy, but we don't really care, peer will be gone by next loop
            self._witness_node_tasks.complete(batch_id, ())
        except (BaseP2PError, Exception) as exc:
            self.logger.info("Unexpected err while getting witness nodes from %s: %s", peer, exc)
            self.logger.debug("Problem downloading witness nodes from peer...", exc_info=True)
            self._witness_node_tasks.complete(batch_id, ())
            self.call_later(NON_IDEAL_RESPONSE_PENALTY, self._waiting_peers.put_nowait, peer)
        else:
            self._waiting_peers.put_nowait(peer)

            node_lookup = dict(nodes)
            completed_tasks = tuple(
                task for task in request_hashes if task.node_hash in node_lookup
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

    def register_peer(self, peer: BasePeer) -> None:
        super().register_peer(peer)
        # when a new peer is added to the pool, add it to the idle peer list
        self._waiting_peers.put_nowait(peer)  # type: ignore

    def deregister_peer(self, peer: BasePeer) -> None:
        super().deregister_peer(peer)
        if self._queen_peer == peer:
            self._queen_peer = None

    async def _periodically_report_progress(self) -> None:
        while self.is_operational:
            await self.sleep(self._report_interval)

            msg = "all=%d" % self._total_processed_nodes
            msg += "  new=%d" % self._num_added
            msg += "  missed=%d" % self._num_missed
            msg += "  queen=%s" % self._queen_peer
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
    subscription_msg_types: FrozenSet[Type[CommandAPI]] = frozenset()

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
            self._firehose_peers.add(peer)
        else:
            self.logger.warning("Ignoring peer for witness broadcast, not firehose-enabled: %s", peer)

    def deregister_peer(self, peer: BasePeer) -> None:
        super().deregister_peer(peer)
        if peer in self._firehose_peers:
            self.logger.warning("Removing Firehose peer: %s", peer)
            self._firehose_peers.remove(peer)

    async def _broadcast_new_witnesses(self) -> None:
        async for event in self.wait_iter(self._event_bus.stream(CreatedNewBlockWitnessHashes)):
            if not event.witness_hashes:
                self.logger.warning("Witness metadata for %s is completely empty", event.header_hash)
            else:
                self.logger.warning("Witness broadcastor received block import complete event: %s", event.block)
                self._broadcast_witness_metadata(event.header_hash, event.witness_hashes)

    def _broadcast_witness_metadata(self, header_hash: Hash32, witness_hashes: Tuple[Hash32, ...]) -> None:
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
