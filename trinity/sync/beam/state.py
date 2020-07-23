import asyncio
from collections import Counter
from concurrent.futures import CancelledError
import itertools
import time
import typing
from typing import (
    Any,
    Collection,
    FrozenSet,
    Iterable,
    Set,
    Tuple,
    Type,
)

from async_service import Service

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

from p2p.abc import CommandAPI
from p2p.exceptions import BaseP2PError, PeerConnectionLost
from p2p.peer import BasePeer, PeerSubscriber

from trie import HexaryTrie
from trie.exceptions import MissingTrieNode

from trinity._utils.datastructures import TaskQueue
from trinity._utils.logging import get_logger
from trinity._utils.timer import Timer
from trinity.protocol.common.typing import (
    NodeDataBundles,
)
from trinity.protocol.eth.constants import (
    MAX_STATE_FETCH,
)
from trinity.protocol.eth.peer import ETHPeer, ETHPeerPool
from trinity.protocol.eth import (
    constants as eth_constants,
)
from trinity.sync.beam.queen import (
    QueenTrackerAPI,
)
from trinity.sync.beam.constants import (
    ESTIMATED_BEAMABLE_SECONDS,
    REQUEST_BUFFER_MULTIPLIER,
)


class BeamDownloader(Service, PeerSubscriber):
    """
    Coordinate the request of needed state data: accounts, storage, bytecodes, and
    other arbitrary intermediate nodes in the trie.
    """
    _total_processed_nodes = 0
    _urgent_processed_nodes = 0
    _predictive_processed_nodes = 0
    _total_timeouts = 0
    _predictive_requests = 0
    _urgent_requests = 0
    _time_on_urgent = 0.0
    _timer = Timer(auto_start=False)
    _report_interval = 10  # Number of seconds between progress reports.
    _reply_timeout = 10  # seconds

    _num_urgent_requests_by_peer: typing.Counter[ETHPeer]
    _num_predictive_requests_by_peer: typing.Counter[ETHPeer]

    # We are only interested in peers entering or leaving the pool
    subscription_msg_types: FrozenSet[Type[CommandAPI[Any]]] = frozenset()

    # This is a rather arbitrary value, but when the sync is operating normally we never see
    # the msg queue grow past a few hundred items, so this should be a reasonable limit for
    # now.
    msg_queue_maxsize: int = 2000

    def __init__(
            self,
            db: AtomicDatabaseAPI,
            peer_pool: ETHPeerPool,
            queen_tracker: QueenTrackerAPI,
            event_bus: EndpointAPI) -> None:
        self.logger = get_logger('trinity.sync.beam.BeamDownloader')
        self._db = db
        self._trie_db = HexaryTrie(db)
        self._event_bus = event_bus

        # Track the needed node data that is urgent and important:
        buffer_size = MAX_STATE_FETCH * REQUEST_BUFFER_MULTIPLIER
        self._node_tasks = TaskQueue[Hash32](buffer_size, lambda task: 0)

        # list of events waiting on new data
        self._new_data_events: Set[asyncio.Event] = set()

        self._peer_pool = peer_pool

        # Track node data that might be useful: hashes we bumped into while getting urgent nodes
        self._maybe_useful_nodes = TaskQueue[Hash32](
            buffer_size,
            # Everything is the same priority, for now
            lambda node_hash: 0,
        )

        self._num_urgent_requests_by_peer = Counter()
        self._num_predictive_requests_by_peer = Counter()

        self._queen_tracker = queen_tracker

    async def ensure_nodes_present(
            self,
            node_hashes: Iterable[Hash32],
            urgent: bool = True) -> int:
        """
        Wait until the nodes that are the preimages of `node_hashes` are available in the database.
        If one is not available in the first check, request it from peers.

        :param urgent: Should this node be downloaded urgently? If False, download as backfill

        Note that if your ultimate goal is an account or storage data, it's probably better to use
        download_account or download_storage. This method is useful for other
        scenarios, like bytecode lookups or intermediate node lookups.

        :return: how many nodes had to be downloaded
        """
        if urgent:
            queue = self._node_tasks
        else:
            queue = self._maybe_useful_nodes

        return await self._wait_for_nodes(node_hashes, queue)

    async def _wait_for_nodes(
            self,
            node_hashes: Iterable[Hash32],
            queue: TaskQueue[Hash32]) -> int:
        missing_nodes = set(
            node_hash for node_hash in node_hashes if self._is_node_missing(node_hash)
        )
        unrequested_nodes = tuple(
            node_hash for node_hash in missing_nodes if node_hash not in queue
        )
        if unrequested_nodes:
            await queue.add(unrequested_nodes)
        if missing_nodes:
            await self._node_hashes_present(missing_nodes)
        return len(unrequested_nodes)

    def _is_node_missing(self, node_hash: Hash32) -> bool:
        if len(node_hash) != 32:
            raise ValidationError(f"Must request node by its 32-byte hash: 0x{node_hash.hex()}")

        self.logger.debug2("checking if node 0x%s is present", node_hash.hex())

        return node_hash not in self._db

    async def download_accounts(
            self,
            account_addresses: Collection[Address],
            root_hash: Hash32,
            urgent: bool = True) -> int:
        """
        Like :meth:`download_account`, but waits for multiple addresses to be available.

        :return: total number of trie node downloads that were required to locally prove
        """
        if len(account_addresses) == 0:
            return 0

        last_log_time = time.monotonic()

        missing_account_hashes = set(keccak(address) for address in account_addresses)
        completed_account_hashes = set()
        nodes_downloaded = 0
        # will never take more than 64 attempts to get a full account
        for _ in range(64):
            need_nodes = set()
            with self._trie_db.at_root(root_hash) as snapshot:
                for account_hash in missing_account_hashes:
                    try:
                        snapshot[account_hash]
                    except MissingTrieNode as exc:
                        need_nodes.add(exc.missing_node_hash)
                    else:
                        completed_account_hashes.add(account_hash)

            # Log if taking a long time to download addresses
            now = time.monotonic()
            if urgent and now - last_log_time > ESTIMATED_BEAMABLE_SECONDS:
                self.logger.info(
                    "Beam account download: %d/%d (%.0f%%)",
                    len(completed_account_hashes),
                    len(account_addresses),
                    100 * len(completed_account_hashes) / len(account_addresses),
                )
                last_log_time = now

            await self.ensure_nodes_present(need_nodes, urgent)
            nodes_downloaded += len(need_nodes)
            missing_account_hashes -= completed_account_hashes

            if not missing_account_hashes:
                return nodes_downloaded
        else:
            raise Exception(
                f"State Downloader failed to download {account_addresses!r} at "
                f"state root 0x{root_hash.hex} in 64 runs"
            )

    async def download_account(
            self,
            account_hash: Hash32,
            root_hash: Hash32,
            urgent: bool = True) -> Tuple[bytes, int]:
        """
        Check the given account address for presence in the state database.
        Wait until we have the state proof for the given address.
        If the account is not available in the first check, then request any trie nodes
        that we need to determine and prove the account rlp.

        Mark these nodes as urgent and important, which increases request priority.

        :return: The downloaded account rlp, and how many state trie node downloads were required
        """
        # will never take more than 64 attempts to get a full account
        for num_downloads_required in range(64):
            try:
                with self._trie_db.at_root(root_hash) as snapshot:
                    account_rlp = snapshot[account_hash]
            except MissingTrieNode as exc:
                await self.ensure_nodes_present({exc.missing_node_hash}, urgent)
            else:
                # Account is fully available within the trie
                return account_rlp, num_downloads_required
        else:
            raise Exception(
                f"State Downloader failed to download 0x{account_hash.hex()} at "
                f"state root 0x{root_hash.hex} in 64 runs"
            )

    async def download_storage(
            self,
            storage_key: Hash32,
            storage_root_hash: Hash32,
            account: Address,
            urgent: bool = True) -> int:
        """
        Check the given storage key for presence in the account's storage database.
        Wait until we have a trie proof for the given storage key.
        If the storage key value is not available in the first check, then request any trie nodes
        that we need to determine and prove the storage value.

        Mark these nodes as urgent and important, which increases request priority.

        :return: how many storage trie node downloads were required
        """
        # should never take more than 64 attempts to get a full account
        for num_downloads_required in range(64):
            try:
                with self._trie_db.at_root(storage_root_hash) as snapshot:
                    # request the data just to see which part is missing
                    snapshot[storage_key]
            except MissingTrieNode as exc:
                await self.ensure_nodes_present({exc.missing_node_hash}, urgent)
            else:
                # Account is fully available within the trie
                return num_downloads_required
        else:
            raise Exception(
                f"State Downloader failed to download storage 0x{storage_key.hex()} in "
                f"{to_checksum_address(account)} at storage root 0x{storage_root_hash.hex()} "
                f"in 64 runs."
            )

    async def _match_urgent_node_requests_to_peers(self) -> None:
        """
        Monitor for urgent trie node needs. An urgent node means that a current block import
        is paused until that trie node is retrieved.

        Ask our best peer for that trie node, and then wait for the next urgent node need.
        Repeat indefinitely.
        """
        while self.manager.is_running:
            urgent_batch_id, urgent_hashes = await self._node_tasks.get(
                eth_constants.MAX_STATE_FETCH
            )

            # Get best peer, by GetNodeData speed
            peer = await self._queen_tracker.get_queen_peer()

            peer_is_requesting = peer.eth_api.get_node_data.is_requesting

            if peer_is_requesting:
                # Our best peer for node data has an in-flight GetNodeData request
                # Probably, backfill is asking this peer for data
                # This is right in the critical path, so we'd prefer this never happen
                self.logger.debug(
                    "Want to download urgent data, but %s is locked on other request",
                    peer,
                )
                # Don't do anything different, allow the request lock to handle the situation

            self._num_urgent_requests_by_peer[peer] += 1
            self._urgent_requests += 1

            # Request all the urgent nodes from the queen peer. EVM execution is blocked
            #   anyway, so just hang on this peer to return.
            await self._get_nodes_from_peer(
                peer,
                urgent_hashes,
                urgent_batch_id,
                urgent=True,
            )

    async def _match_predictive_node_requests_to_peers(self) -> None:
        """
        Monitor for predictive nodes. These might be required by future blocks. They might not,
        because we run a speculative execution which might follow a different code path than
        the final block import does.

        When predictive nodes are queued up, ask the fastest available peasant (non-queen) peer
        for them. Without waiting for a response from the peer, continue and check if more
        predictive trie nodes are requested. Repeat indefinitely.
        """
        while self.manager.is_running:
            batch_id, hashes = await self._maybe_useful_nodes.get(eth_constants.MAX_STATE_FETCH)
            peer = await self._queen_tracker.pop_fastest_peasant()

            self._num_predictive_requests_by_peer[peer] += 1
            self._predictive_requests += 1

            self.manager.run_task(
                self._get_predictive_nodes_from_peer,
                peer,
                hashes,
                batch_id,
            )

    @staticmethod
    def _append_unique_hashes(
            first_hashes: Tuple[Hash32, ...],
            non_unique_hashes: Tuple[Hash32, ...]) -> Tuple[Hash32, ...]:
        unique_hashes_to_add = tuple(set(non_unique_hashes).difference(first_hashes))
        return first_hashes + unique_hashes_to_add

    async def _get_predictive_nodes_from_peer(
            self,
            peer: ETHPeer,
            node_hashes: Tuple[Hash32, ...],
            batch_id: int) -> None:
        await self._get_nodes_from_peer(peer, node_hashes, batch_id, urgent=False)
        self._queen_tracker.insert_peer(peer)

    async def _get_nodes_from_peer(
            self,
            peer: ETHPeer,
            node_hashes: Tuple[Hash32, ...],
            batch_id: int,
            urgent: bool) -> None:
        if urgent:
            urgent_timer = Timer()

        nodes, new_nodes = await self._store_nodes(peer, node_hashes)

        if len(nodes) == 0 and urgent:
            self.logger.debug("%s returned no urgent nodes from %r", peer, node_hashes)

        self._total_processed_nodes += len(nodes)

        if urgent:
            self._node_tasks.complete(batch_id, tuple(node_hash for node_hash, _ in nodes))
            self._urgent_processed_nodes += len(nodes)

            time_on_urgent = urgent_timer.elapsed
            self.logger.debug(
                "beam-rtt: got %d/%d urgent nodes in %.3fs from %s (%s)",
                len(nodes),
                len(node_hashes),
                time_on_urgent,
                peer.remote,
                node_hashes[0][:2].hex()
            )
            self._time_on_urgent += time_on_urgent
        else:
            self._maybe_useful_nodes.complete(batch_id, tuple(node_hash for node_hash, _ in nodes))
            self._predictive_processed_nodes += len(nodes)

    async def _store_nodes(
            self,
            peer: ETHPeer,
            node_hashes: Tuple[Hash32, ...]) -> Tuple[NodeDataBundles, NodeDataBundles]:
        nodes = await self._request_nodes(peer, node_hashes)

        new_nodes = tuple(
            (node_hash, node) for node_hash, node in nodes
            if self._is_node_missing(node_hash)
        )

        if new_nodes:
            # batch all DB writes into one, for performance
            with self._db.atomic_batch() as batch:
                for node_hash, node in new_nodes:
                    batch[node_hash] = node

        if len(new_nodes):
            for new_data in self._new_data_events:
                new_data.set()

        return nodes, new_nodes

    def _is_node_present(self, node_hash: Hash32) -> bool:
        """
        Check if node_hash has data in the database or in the predicted node set.
        """
        return node_hash in self._db

    async def _node_hashes_present(self, node_hashes: Set[Hash32]) -> None:
        remaining_hashes = node_hashes.copy()

        # save an event that gets triggered when new data comes in
        new_data = asyncio.Event()
        self._new_data_events.add(new_data)

        iterations = itertools.count()

        while remaining_hashes and next(iterations) < 1000:
            await new_data.wait()

            found_hashes = set(found for found in remaining_hashes if self._is_node_present(found))
            remaining_hashes -= found_hashes

            new_data.clear()

        if remaining_hashes:
            self.logger.error("Never collected node data for hashes %r", remaining_hashes)

        self._new_data_events.remove(new_data)

    def register_peer(self, peer: BasePeer) -> None:
        super().register_peer(peer)
        # when a new peer is added to the pool, add it to the idle peer list

    async def _request_nodes(
            self,
            peer: ETHPeer,
            original_node_hashes: Tuple[Hash32, ...]) -> NodeDataBundles:
        node_hashes = tuple(set(original_node_hashes))
        num_nodes = len(node_hashes)
        self.logger.debug2("Requesting %d nodes from %s", num_nodes, peer)
        try:
            completed_nodes = await peer.eth_api.get_node_data(
                node_hashes, timeout=self._reply_timeout)
        except PeerConnectionLost:
            self.logger.debug("%s went away, cancelling the nodes request and moving on...", peer)
            self._queen_tracker.penalize_queen(peer)
            return tuple()
        except BaseP2PError as exc:
            self.logger.warning("Unexpected p2p err while downloading nodes from %s: %s", peer, exc)
            self.logger.debug("Problem downloading nodes from peer, dropping...", exc_info=True)
            self._queen_tracker.penalize_queen(peer)
            return tuple()
        except CancelledError:
            self.logger.debug("Pending nodes call to %r future cancelled", peer)
            self._queen_tracker.penalize_queen(peer)
            raise
        except asyncio.TimeoutError:
            # This kind of exception shouldn't necessarily *drop* the peer,
            # so capture error, log and swallow
            self.logger.debug("Timed out requesting %d nodes from %s", num_nodes, peer)
            self._queen_tracker.penalize_queen(peer)
            self._total_timeouts += 1
            return tuple()
        except Exception as exc:
            self.logger.info("Unexpected err while downloading nodes from %s: %s", peer, exc)
            self.logger.debug(
                "Problem downloading nodes from %s",
                peer,
                exc_info=True,
            )
            self._queen_tracker.penalize_queen(peer)
            return tuple()
        else:
            if len(completed_nodes) > 0:
                # peer completed successfully, so have it get back in line for processing
                pass
            else:
                # peer didn't return enough results, wait a while before trying again
                self.logger.debug("%s returned 0 state trie nodes, penalize...", peer)
                self._queen_tracker.penalize_queen(peer)
            return completed_nodes

    async def run(self) -> None:
        """
        Request all nodes in the queue, running indefinitely
        """
        self._timer.start()
        self.logger.info("Starting beam state sync")
        self.manager.run_task(self._periodically_report_progress)
        with self.subscribe(self._peer_pool):
            self.manager.run_daemon_task(self._match_predictive_node_requests_to_peers)
            await self._match_urgent_node_requests_to_peers()

    async def _periodically_report_progress(self) -> None:
        while self.manager.is_running:
            self._time_on_urgent = 0
            interval_timer = Timer()
            await asyncio.sleep(self._report_interval)

            msg = "all=%d  " % self._total_processed_nodes
            msg += "urgent=%d  " % self._urgent_processed_nodes
            # The percent of time spent in the last interval waiting on an urgent node
            #   from the queen peer:
            msg += "crit=%.0f%%  " % (100 * self._time_on_urgent / interval_timer.elapsed)
            msg += "pred=%d  " % self._predictive_processed_nodes
            msg += "all/sec=%d  " % (self._total_processed_nodes / self._timer.elapsed)
            msg += "urgent/sec=%d  " % (self._urgent_processed_nodes / self._timer.elapsed)
            msg += "urg_reqs=%d  " % (self._urgent_requests)
            msg += "pred_reqs=%d  " % (self._predictive_requests)
            msg += "timeouts=%d" % self._total_timeouts
            msg += "  u_pend=%d" % self._node_tasks.num_pending()
            msg += "  u_prog=%d" % self._node_tasks.num_in_progress()
            msg += "  p_pend=%d" % self._maybe_useful_nodes.num_pending()
            msg += "  p_prog=%d" % self._maybe_useful_nodes.num_in_progress()
            self.logger.debug("beam-sync: %s", msg)

            # log peer counts
            show_top_n_peers = 5
            self.logger.debug(
                "beam-queen-usage-top-%d: urgent=%s, predictive=%s",
                show_top_n_peers,
                [
                    (str(peer.remote), num) for peer, num in
                    self._num_urgent_requests_by_peer.most_common(show_top_n_peers)
                ],
                [
                    (str(peer.remote), num) for peer, num in
                    self._num_predictive_requests_by_peer.most_common(show_top_n_peers)
                ],
            )
            self._num_urgent_requests_by_peer.clear()
            self._num_predictive_requests_by_peer.clear()
