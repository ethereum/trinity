import asyncio
from collections import Counter, defaultdict
from concurrent.futures import CancelledError, ThreadPoolExecutor
import time
import typing
from typing import (
    Any,
    Callable,
    Collection,
    DefaultDict,
    Dict,
    FrozenSet,
    Iterable,
    Set,
    Tuple,
    Type,
    TypeVar,
)

from async_service import Service

from lahja import EndpointAPI

from eth_hash.auto import keccak
from eth_utils import (
    clamp,
    to_checksum_address,
)
from eth_typing import (
    Address,
    BlockNumber,
    Hash32,
)

from eth.abc import AtomicDatabaseAPI

from p2p.abc import CommandAPI
from p2p.asyncio_utils import (
    cleanup_tasks,
    create_task,
)
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
    BLOCK_IMPORT_MISSING_STATE_TIMEOUT,
    CHECK_PREVIEW_STATE_TIMEOUT,
    ESTIMATED_BEAMABLE_SECONDS,
    MAX_ACCEPTABLE_WAIT_FOR_URGENT_NODE,
    NON_IDEAL_RESPONSE_PENALTY,
    REQUEST_BUFFER_MULTIPLIER,
    TOO_LONG_PREDICTIVE_PEER_DELAY,
)

TReturn = TypeVar('TReturn')


class BeamDownloader(Service, PeerSubscriber):
    """
    Coordinate the request of needed state data: accounts, storage, bytecodes, and
    other arbitrary intermediate nodes in the trie.
    """
    _total_processed_nodes = 0
    _urgent_processed_nodes = 0
    _predictive_processed_nodes = 0
    _predictive_found_nodes_woke_up = 0
    _predictive_found_nodes_during_timeout = 0
    _total_timeouts = 0
    _predictive_requests = 0
    _urgent_requests = 0
    _time_on_urgent = 0.0
    _timer = Timer(auto_start=False)
    _report_interval = 10  # Number of seconds between progress reports.
    _reply_timeout = 10  # seconds

    _num_urgent_requests_by_peer: typing.Counter[ETHPeer]
    _num_predictive_requests_by_peer: typing.Counter[ETHPeer]

    _preview_events: Dict[asyncio.Event, Set[Hash32]]

    _num_peers = 0
    # How many extra peers (besides the queen) should we ask for the urgently-needed trie node?
    _spread_factor = 0
    # We periodically reduce the "spread factor" once every N seconds:
    _reduce_spread_factor_interval = 120
    # We might reserve some peers to ask for predictive nodes, if we start to fall behind
    _min_predictive_peers = 0
    # Keep track of the block number for each predictive missing node hash
    _block_number_lookup: DefaultDict[Hash32, BlockNumber]

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
        self._new_data_event: asyncio.Event = asyncio.Event()
        self._preview_events = {}

        self._peer_pool = peer_pool

        # Track node data for upcoming blocks
        self._block_number_lookup = defaultdict(lambda: BlockNumber(0))
        self._maybe_useful_nodes = TaskQueue[Hash32](
            buffer_size,
            # Prefer trie nodes from earliest blocks
            lambda node_hash: self._block_number_lookup[node_hash],
        )

        self._num_urgent_requests_by_peer = Counter()
        self._num_predictive_requests_by_peer = Counter()

        self._queen_tracker = queen_tracker
        self._threadpool = ThreadPoolExecutor()
        asyncio.get_event_loop().set_default_executor(self._threadpool)

    async def ensure_nodes_present(
            self,
            node_hashes: Collection[Hash32],
            block_number: BlockNumber,
            urgent: bool = True) -> int:
        """
        Wait until the nodes that are the preimages of `node_hashes` are available in the database.
        If one is not available in the first check, request it from peers.

        If any nodes cannot be downloaded in BLOCK_IMPORT_MISSING_STATE_TIMEOUT seconds, return
        anyway.

        :param urgent: Should this node be downloaded urgently? If False, download as backfill

        Note that if your ultimate goal is an account or storage data, it's probably better to use
        download_account or download_storage. This method is useful for other
        scenarios, like bytecode lookups or intermediate node lookups.

        :return: how many nodes were downloaded
        """
        if urgent:
            num_nodes_found = await self._wait_for_nodes(
                node_hashes,
                urgent,
            )
        else:
            for node_hash in node_hashes:
                # Priority is based on lowest block number that needs the given node
                if self._block_number_lookup.get(node_hash, block_number + 1) > block_number:
                    self._block_number_lookup[node_hash] = block_number

            num_nodes_found = await self._wait_for_nodes(
                node_hashes,
                urgent,
            )
            requested_node_count = len(node_hashes)
            if num_nodes_found == requested_node_count:
                for node_hash in node_hashes:
                    self._block_number_lookup.pop(node_hash, None)
            elif num_nodes_found < requested_node_count:
                found_hashes = await asyncio.get_event_loop().run_in_executor(
                    None,
                    self._get_unique_present_hashes,
                    node_hashes,
                )
                for node_hash in found_hashes:
                    self._block_number_lookup.pop(node_hash, None)

        return num_nodes_found

    def _max_spread_beam_factor(self) -> int:
        max_factor = self._num_peers - 1 - self._min_predictive_peers
        return max(0, max_factor)

    def _get_unique_missing_hashes(self, hashes: Iterable[Hash32]) -> Set[Hash32]:
        return set(
            node_hash for node_hash in hashes if node_hash not in self._db
        )

    def _get_unique_present_hashes(self, hashes: Iterable[Hash32]) -> Set[Hash32]:
        return set(
            node_hash for node_hash in hashes if node_hash in self._db
        )

    async def _wait_for_nodes(
            self,
            node_hashes: Iterable[Hash32],
            urgent: bool) -> int:
        """
        Insert the given node hashes into the queue to be retrieved, then block
        until they become present in the database.

        :return: number of new nodes received -- might be smaller than len(node_hashes) on timeout
        """
        missing_nodes = await self._run_preview_in_thread(
            urgent,
            self._get_unique_missing_hashes,
            node_hashes,
        )

        if urgent:
            queue = self._node_tasks
        else:
            queue = self._maybe_useful_nodes

        unrequested_nodes = tuple(
            node_hash for node_hash in missing_nodes if node_hash not in queue
        )
        if missing_nodes:
            if unrequested_nodes:
                await queue.add(unrequested_nodes)
            return await self._node_hashes_present(missing_nodes, urgent)
        else:
            return 0

    def _account_review(
            self,
            account_address_hashes: Iterable[Hash32],
            root_hash: Hash32) -> Tuple[Set[Hash32], Dict[Hash32, bytes]]:
        """
        Check these accounts in the trie.
        :return: (missing trie nodes, completed_hashes->encoded_account_rlp)
        """
        need_nodes = set()
        completed_accounts = {}
        with self._trie_db.at_root(root_hash) as snapshot:
            for account_hash in account_address_hashes:
                try:
                    account_rlp = snapshot[account_hash]
                except MissingTrieNode as exc:
                    need_nodes.add(exc.missing_node_hash)
                else:
                    completed_accounts[account_hash] = account_rlp

        return need_nodes, completed_accounts

    def _get_unique_hashes(self, addresses: Collection[Address]) -> Set[Hash32]:
        uniques = set(addresses)
        return {keccak(address) for address in uniques}

    async def download_accounts(
            self,
            account_addresses: Collection[Address],
            root_hash: Hash32,
            block_number: BlockNumber,
            urgent: bool = True) -> int:
        """
        Like :meth:`download_account`, but waits for multiple addresses to be available.

        :return: total number of trie node downloads that were required to locally prove
        """
        if len(account_addresses) == 0:
            return 0

        last_log_time = time.monotonic()

        missing_account_hashes = await self._run_preview_in_thread(
            urgent,
            self._get_unique_hashes,
            account_addresses,
        )

        completed_account_hashes: Set[Hash32] = set()
        nodes_downloaded = 0
        # will never take more than 64 attempts to get a full account
        for _ in range(64):
            need_nodes, newly_completed = await self._run_preview_in_thread(
                urgent,
                self._account_review,
                missing_account_hashes,
                root_hash,
            )
            completed_account_hashes.update(newly_completed.keys())

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

            await self.ensure_nodes_present(need_nodes, block_number, urgent)
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
            block_number: BlockNumber,
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
            need_nodes, newly_completed = await self._run_preview_in_thread(
                urgent,
                self._account_review,
                [account_hash],
                root_hash,
            )
            if need_nodes:
                await self.ensure_nodes_present(need_nodes, block_number, urgent)
            else:
                # Account is fully available within the trie
                return newly_completed[account_hash], num_downloads_required
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
            block_number: BlockNumber,
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
            need_nodes = await self._run_preview_in_thread(
                urgent,
                self._storage_review,
                storage_key,
                storage_root_hash,
            )
            if need_nodes:
                await self.ensure_nodes_present(need_nodes, block_number, urgent)
            else:
                # Account is fully available within the trie
                return num_downloads_required
        else:
            raise Exception(
                f"State Downloader failed to download storage 0x{storage_key.hex()} in "
                f"{to_checksum_address(account)} at storage root 0x{storage_root_hash.hex()} "
                f"in 64 runs."
            )

    def _storage_review(
            self,
            storage_key: Hash32,
            storage_root_hash: Hash32) -> Set[Hash32]:
        """
        Check this storage slot in the trie.
        :return: missing trie nodes
        """
        with self._trie_db.at_root(storage_root_hash) as snapshot:
            try:
                # request the data just to see which part is missing
                snapshot[storage_key]
            except MissingTrieNode as exc:
                return {exc.missing_node_hash}
            else:
                return set()

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
            queen = await self._queen_tracker.get_queen_peer()

            queen_is_requesting = queen.eth_api.get_node_data.is_requesting

            if queen_is_requesting:
                # Our best peer for node data has an in-flight GetNodeData request
                # Probably, backfill is asking this peer for data
                # This is right in the critical path, so we'd prefer this never happen
                self.logger.debug(
                    "Want to download urgent data, but %s is locked on other request",
                    queen,
                )
                # Don't do anything different, allow the request lock to handle the situation

            self._num_urgent_requests_by_peer[queen] += 1
            self._urgent_requests += 1

            await self._find_urgent_nodes(
                queen,
                urgent_hashes,
                urgent_batch_id,
            )

    async def _find_urgent_nodes(
            self,
            queen: ETHPeer,
            urgent_hashes: Tuple[Hash32, ...],
            batch_id: int) -> None:

        # Generate and schedule the tasks to request the urgent node(s) from multiple peers
        knights = tuple(self._queen_tracker.pop_knights())
        urgent_requests = [
            create_task(
                self._get_nodes(peer, urgent_hashes, urgent=True),
                name=f"BeamDownloader._get_nodes({peer.remote}, ...)",
            )
            for peer in (queen,) + knights
        ]

        # Process the returned nodes, in the order they complete
        urgent_timer = Timer()
        async with cleanup_tasks(*urgent_requests):
            for result_coro in asyncio.as_completed(urgent_requests):
                nodes_returned, new_nodes, peer = await result_coro
                time_on_urgent = urgent_timer.elapsed

                # After the first peer returns something, cancel all other pending tasks
                if len(nodes_returned) > 0:
                    # Stop waiting for other peer responses
                    break
                elif peer == queen:
                    self.logger.debug("queen %s returned 0 urgent nodes of %r", peer, urgent_hashes)
                    # Wait for the next peer response

        # Log the received urgent nodes
        if peer == queen:
            log_header = "beam-queen-urgent-rtt"
        else:
            log_header = "spread-beam-urgent-rtt"
        self.logger.debug(
            "%s: got %d/%d +%d nodes in %.3fs from %s (%s)",
            log_header,
            len(nodes_returned),
            len(urgent_hashes),
            len(new_nodes),
            time_on_urgent,
            peer.remote,
            urgent_hashes[0][:2].hex(),
        )

        # Stat updates
        self._total_processed_nodes += len(new_nodes)
        self._urgent_processed_nodes += len(new_nodes)
        self._time_on_urgent += time_on_urgent

        # If it took to long to get a single urgent node, then increase "spread" factor
        if len(urgent_hashes) == 1 and time_on_urgent > MAX_ACCEPTABLE_WAIT_FOR_URGENT_NODE:
            new_spread_factor = clamp(
                0,
                self._max_spread_beam_factor(),
                self._spread_factor + 1,
            )
            if new_spread_factor != self._spread_factor:
                self.logger.debug(
                    "spread-beam-update: Urgent node latency=%.3fs, update factor %d to %d",
                    time_on_urgent,
                    self._spread_factor,
                    new_spread_factor,
                )
                self._queen_tracker.set_desired_knight_count(new_spread_factor)
                self._spread_factor = new_spread_factor

        # Complete the task in the TaskQueue
        task_hashes = tuple(node_hash for node_hash, _ in nodes_returned)
        await self._node_tasks.complete(batch_id, task_hashes)

        # Re-insert the peers for the next request
        for knight in knights:
            self._queen_tracker.insert_peer(knight)

    async def _match_predictive_node_requests_to_peers(self) -> None:
        """
        Monitor for predictive nodes. These might be required by future blocks. They might not,
        because we run a speculative execution which might follow a different code path than
        the final block import does.

        When predictive nodes are queued up, ask the fastest available peasant (non-queen) peer
        for them. Without waiting for a response from the peer, continue and check if more
        predictive trie nodes are requested. Repeat indefinitely.
        """
        # If self._queen_tracker terminates we need to exit as well, so check that on every
        # iteration.
        while self.manager.is_running and self._queen_tracker.get_manager().is_running:
            try:
                batch_id, hashes = await asyncio.wait_for(
                    self._maybe_useful_nodes.get(eth_constants.MAX_STATE_FETCH),
                    timeout=TOO_LONG_PREDICTIVE_PEER_DELAY,
                )
            except asyncio.TimeoutError:
                # Reduce the number of predictive peers, we seem to have plenty
                if self._min_predictive_peers > 0:
                    self._min_predictive_peers -= 1
                    self.logger.debug(
                        "Decremented predictive peers to %d",
                        self._min_predictive_peers,
                    )
                # Re-attempt
                continue

            # Find any hashes that were discovered through other means, like urgent requests:
            existing_hashes = await asyncio.get_event_loop().run_in_executor(
                None,
                self._get_unique_present_hashes,
                hashes,
            )
            # If any hashes are already found, clear them out and retry
            if existing_hashes:
                # Wake up any paused preview threads
                await self._wakeup_preview_waiters(existing_hashes)
                # Clear out any tasks that are no longer necessary
                await self._maybe_useful_nodes.complete(batch_id, tuple(existing_hashes))
                # Restart from the top
                continue

            try:
                peer = await asyncio.wait_for(
                    self._queen_tracker.pop_fastest_peasant(),
                    timeout=TOO_LONG_PREDICTIVE_PEER_DELAY,
                )
            except asyncio.TimeoutError:
                # Increase the minimum number of predictive peers, we seem to not have enough
                new_predictive_peers = min(
                    self._min_predictive_peers + 1,
                    # Don't reserve more than half the peers for prediction
                    self._num_peers // 2,
                )
                if new_predictive_peers != self._min_predictive_peers:
                    self.logger.debug(
                        "Updating predictive peer count from %d to %d",
                        self._min_predictive_peers,
                        new_predictive_peers,
                    )
                    self._min_predictive_peers = new_predictive_peers

                cancel_attempt = True
            else:
                if peer.eth_api.get_node_data.is_requesting:
                    self.logger.debug(
                        "Want predictive nodes from %s, but it has an active request, skipping...",
                        peer,
                    )
                    self._queen_tracker.insert_peer(peer, NON_IDEAL_RESPONSE_PENALTY)
                    cancel_attempt = True
                else:
                    cancel_attempt = False

            if cancel_attempt:
                # Prepare to restart
                await self._maybe_useful_nodes.complete(batch_id, ())
                continue

            self._num_predictive_requests_by_peer[peer] += 1
            self._predictive_requests += 1

            self.manager.run_task(
                self._get_predictive_nodes_from_peer,
                peer,
                hashes,
                batch_id,
            )

        if self.manager.is_running and not self._queen_tracker.get_manager().is_running:
            self.logger.info(
                "Backfill is complete, halting predictive downloads..."
            )

    async def _get_predictive_nodes_from_peer(
            self,
            peer: ETHPeer,
            node_hashes: Tuple[Hash32, ...],
            batch_id: int) -> None:

        nodes, new_nodes, _ = await self._get_nodes(peer, node_hashes, urgent=False)

        self._total_processed_nodes += len(nodes)
        self._predictive_processed_nodes += len(new_nodes)

        task_hashes = tuple(node_hash for node_hash, _ in nodes)
        await self._maybe_useful_nodes.complete(batch_id, task_hashes)

        # Re-insert the peasant into the tracker
        if len(nodes):
            delay = 0.0
        else:
            delay = 8.0
        self._queen_tracker.insert_peer(peer, delay)

    async def _get_nodes(
            self,
            peer: ETHPeer,
            node_hashes: Tuple[Hash32, ...],
            urgent: bool) -> Tuple[NodeDataBundles, NodeDataBundles, ETHPeer]:
        nodes = await self._request_nodes(peer, node_hashes)

        (
            new_nodes,
            found_independent,
        ) = await self._run_preview_in_thread(urgent, self._store_nodes, node_hashes, nodes, urgent)

        if urgent:
            if new_nodes or found_independent:
                # If there are any new nodes returned, then notify any coros that are waiting on
                #   node data to resume.
                # If the data was retrieved another way (like backfilled), then
                #   still trigger a new data event. That way, urgent coros don't
                #   get stuck hanging until a timeout. This can cause an especially
                #   flaky test_beam_syncer_backfills_all_state[42].
                self._new_data_event.set()
        elif new_nodes:
            new_hashes = set(node_hash for node_hash, _ in new_nodes)
            await self._wakeup_preview_waiters(new_hashes)

        return nodes, new_nodes, peer

    async def _wakeup_preview_waiters(self, node_hashes: Iterable[Hash32]) -> None:
        # Wake up any coroutines waiting for the particular data that was returned.
        #   (If no data returned, then no coros should wake up, and we can skip the block)
        preview_waiters = await asyncio.get_event_loop().run_in_executor(
            None,
            self._get_preview_waiters,
            node_hashes,
        )
        for waiter in preview_waiters:
            waiter.set()

    def _get_preview_waiters(self, node_hashes: Iterable[Hash32]) -> Tuple[asyncio.Event, ...]:
        # Convert to set for a faster presence-test in the returned tuple comprehension
        new_hashes = set(node_hashes)

        # defensive copy _preview_events, since this method runs in a thread
        waiters = tuple(self._preview_events.items())

        return tuple(
            waiting_event
            for waiting_event, node_hashes in waiters
            if new_hashes & node_hashes
        )

    def _store_nodes(
            self,
            node_hashes: Tuple[Hash32, ...],
            nodes: NodeDataBundles,
            urgent: bool) -> Tuple[NodeDataBundles, bool]:
        """
        Store supplied nodes in the database, return the subset of them that are new.
        Also, return whether the requested nodes were found another way, if the
        nodes are urgently-needed.
        """

        new_nodes = tuple(
            (node_hash, node) for node_hash, node in nodes
            if node_hash not in self._db
        )

        if new_nodes:
            # batch all DB writes into one, for performance
            with self._db.atomic_batch() as batch:
                for node_hash, node in new_nodes:
                    batch[node_hash] = node

            # Don't bother checking if the nodes were found another way
            found_independent = False
        elif urgent:
            # Check if the nodes were found another way, if they are urgently needed
            for requested_hash in node_hashes:
                if requested_hash in self._db:
                    found_independent = True
                    break
            else:
                found_independent = False
        else:
            # Don't bother checking if the nodes were found another way, if they are predictive
            found_independent = False

        return new_nodes, found_independent

    async def _node_hashes_present(self, node_hashes: Set[Hash32], urgent: bool) -> int:
        """
        Block until the supplied node hashes have been inserted into the database.

        :return: number of new nodes received -- might be smaller than len(node_hashes) on timeout
        """
        remaining_hashes = node_hashes.copy()
        timeout = BLOCK_IMPORT_MISSING_STATE_TIMEOUT

        start_time = time.monotonic()
        if not urgent:
            wait_event = asyncio.Event()
            self._preview_events[wait_event] = node_hashes
        while remaining_hashes and time.monotonic() - start_time < timeout:
            if urgent:
                await self._new_data_event.wait()
                self._new_data_event.clear()
            else:
                try:
                    await asyncio.wait_for(
                        wait_event.wait(),
                        timeout=CHECK_PREVIEW_STATE_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    # Check if the data showed up due to an urgent import
                    preview_timeout = True
                    pass
                else:
                    preview_timeout = False
                finally:
                    wait_event.clear()

            found_hashes = await self._run_preview_in_thread(
                urgent,
                self._get_unique_present_hashes,
                remaining_hashes,
            )

            if not urgent:
                if preview_timeout:
                    self._predictive_found_nodes_during_timeout += len(found_hashes)
                else:
                    self._predictive_found_nodes_woke_up += len(found_hashes)

            if found_hashes:
                remaining_hashes -= found_hashes
                if not urgent and remaining_hashes:
                    self._preview_events[wait_event] = remaining_hashes

        if not urgent:
            del self._preview_events[wait_event]

        if remaining_hashes:
            if urgent:
                logger = self.logger.error
            else:
                logger = self.logger.warning
            logger(
                "Could not collect node data for %d %s hashes %r within %.0f seconds (took %.1fs)",
                len(remaining_hashes),
                "urgent" if urgent else "preview",
                list(remaining_hashes)[0:2],
                timeout,
                time.monotonic() - start_time,
            )

        return len(node_hashes) - len(remaining_hashes)

    def register_peer(self, peer: BasePeer) -> None:
        self._num_peers += 1

    def deregister_peer(self, peer: BasePeer) -> None:
        self._num_peers -= 1

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

    async def _run_preview_in_thread(
            self,
            urgent: bool,
            method: Callable[..., TReturn],
            *args: Any) -> TReturn:

        if urgent:
            return method(*args)
        else:
            return await asyncio.get_event_loop().run_in_executor(
                None,
                method,
                *args,
            )

    async def run(self) -> None:
        """
        Request all nodes in the queue, running indefinitely
        """
        self._timer.start()
        self.logger.info("Starting beam state sync")
        self.manager.run_daemon_task(self._periodically_report_progress)
        self.manager.run_daemon_task(self._reduce_spread_factor)
        with self.subscribe(self._peer_pool):
            # The next task is not a daemon task, as a quick fix for
            # https://github.com/ethereum/trinity/issues/2095
            self.manager.run_task(self._match_predictive_node_requests_to_peers)
            await self._match_urgent_node_requests_to_peers()

    async def _reduce_spread_factor(self) -> None:
        # The number of backup urgent requester peers increases when the RTT is too high
        #   This method makes sure that it eventually drops back to 0 in a healthy sync
        #   environment.
        while self.manager.is_running:
            await asyncio.sleep(self._reduce_spread_factor_interval)
            if self._spread_factor > 0:
                self.logger.debug(
                    "spread-beam-update: Reduce spread beam factor %d to %d",
                    self._spread_factor,
                    self._spread_factor - 1,
                )
                self._spread_factor -= 1
                self._queen_tracker.set_desired_knight_count(self._spread_factor)

    async def _periodically_report_progress(self) -> None:
        try:
            # _work_queue is only defined in python 3.8 -- don't report the stat otherwise
            threadpool_queue = self._threadpool._work_queue  # type: ignore
        except AttributeError:
            threadpool_queue = None

        while self.manager.is_running:
            self._time_on_urgent = 0
            interval_timer = Timer()
            await asyncio.sleep(self._report_interval)

            if threadpool_queue:
                threadpool_queue_len = threadpool_queue.qsize()
            else:
                threadpool_queue_len = "?"

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
            msg += "  p_wait=%d" % len(self._preview_events)
            msg += "  p_woke=%d" % self._predictive_found_nodes_woke_up
            msg += "  p_found=%d" % self._predictive_found_nodes_during_timeout
            msg += "  thread_Q=20+%s" % threadpool_queue_len
            self.logger.debug("beam-sync: %s", msg)

            self._predictive_found_nodes_woke_up = 0
            self._predictive_found_nodes_during_timeout = 0

            # log peer counts
            show_top_n_peers = 5
            self.logger.debug(
                "beam-queen-usage-top-%d: urgent=%s, predictive=%s, spread=%d, reserve_pred=%d",
                show_top_n_peers,
                [
                    (str(peer.remote), num) for peer, num in
                    self._num_urgent_requests_by_peer.most_common(show_top_n_peers)
                ],
                [
                    (str(peer.remote), num) for peer, num in
                    self._num_predictive_requests_by_peer.most_common(show_top_n_peers)
                ],
                self._spread_factor,
                self._min_predictive_peers,
            )
            self._num_urgent_requests_by_peer.clear()
            self._num_predictive_requests_by_peer.clear()
