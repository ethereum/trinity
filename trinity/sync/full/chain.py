import asyncio
from concurrent.futures import CancelledError
import datetime
import enum
from functools import (
    partial,
)
from operator import attrgetter
import time
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    List,
    NamedTuple,
    FrozenSet,
    Sequence,
    Tuple,
    Type,
    cast,
)

from async_service import Service, background_asyncio_service

from cancel_token import OperationCancelled
from eth_typing import BlockNumber, Hash32
from eth_utils import (
    humanize_hash,
    humanize_seconds,
    ValidationError,
)
from eth_utils.toolz import (
    concat,
    first,
    groupby,
    merge,
    valfilter,
)

from eth.abc import (
    BlockAPI,
    BlockHeaderAPI,
    ReceiptAPI,
    SignedTransactionAPI,
)
from eth.constants import (
    BLANK_ROOT_HASH,
    EMPTY_UNCLE_HASH,
)
from eth.exceptions import HeaderNotFound

from p2p.abc import CommandAPI
from p2p.disconnect import DisconnectReason
from p2p.exceptions import BaseP2PError, PeerConnectionLost, UnknownAPI
from p2p.peer import BasePeer, PeerSubscriber
from p2p.stats.ema import EMA
from p2p.token_bucket import TokenBucket

from trinity.chains.base import AsyncChainAPI
from trinity.db.eth1.chain import BaseAsyncChainDB
from trinity.protocol.eth.monitors import ETHChainTipMonitor
from trinity.protocol.eth import commands
from trinity.protocol.eth.constants import (
    MAX_BODIES_FETCH,
    MAX_RECEIPTS_FETCH,
)
from trinity.protocol.eth.peer import ETHPeer, ETHPeerPool
from trinity.protocol.eth.sync import ETHHeaderChainSyncer
from trinity.rlp.block_body import BlockBody
from trinity.sync.common.chain import (
    BaseBlockImporter,
    SimpleBlockImporter,
)
from trinity.sync.common.constants import (
    EMPTY_PEER_RESPONSE_PENALTY,
)
from trinity.sync.common.headers import HeaderSyncerAPI
from trinity.sync.common.peers import WaitingPeers
from trinity.sync.full.constants import (
    HEADER_QUEUE_SIZE_TARGET,
    BLOCK_QUEUE_SIZE_TARGET,
    BLOCK_IMPORT_QUEUE_SIZE,
)
from trinity._utils.datastructures import (
    BaseOrderedTaskPreparation,
    MissingDependency,
    OrderedTaskPreparation,
    TaskQueue,
)
from trinity._utils.headers import (
    skip_complete_headers,
)
from trinity._utils.humanize import (
    humanize_integer_sequence,
)
from trinity._utils.logging import get_logger
from trinity._utils.timer import Timer

# (ReceiptBundle, (Receipt, (root_hash, receipt_trie_data))
ReceiptBundle = Tuple[Tuple[ReceiptAPI, ...], Tuple[Hash32, Dict[Hash32, bytes]]]
# (BlockBody, (txn_root, txn_trie_data), uncles_hash)
BlockBodyBundle = Tuple[
    BlockBody,
    Tuple[Hash32, Dict[Hash32, bytes]],
    Hash32,
]

# How big should the pending request queue get, as a multiple of the largest request size
REQUEST_BUFFER_MULTIPLIER = 16


class BaseBodyChainSyncer(Service, PeerSubscriber):

    NO_PEER_RETRY_PAUSE = 5.0
    "If no peers are available for downloading the chain data, retry after this many seconds"

    # We are only interested in peers entering or leaving the pool
    subscription_msg_types: FrozenSet[Type[CommandAPI[Any]]] = frozenset()

    # This is a rather arbitrary value, but when the sync is operating normally we never see
    # the msg queue grow past a few hundred items, so this should be a reasonable limit for
    # now.
    msg_queue_maxsize = 2000

    tip_monitor_class = ETHChainTipMonitor

    _pending_bodies: Dict[BlockHeaderAPI, BlockBody]

    def __init__(self,
                 chain: AsyncChainAPI,
                 db: BaseAsyncChainDB,
                 peer_pool: ETHPeerPool,
                 header_syncer: HeaderSyncerAPI) -> None:
        self.logger = get_logger('trinity.sync.full.chain.BaseBodyChainSyncer')
        self.chain = chain
        self.db = db
        self._peer_pool = peer_pool
        self._pending_bodies = {}

        self._header_syncer = header_syncer

        # queue up any idle peers, in order of how fast they return block bodies
        self._body_peers: WaitingPeers[ETHPeer] = WaitingPeers(commands.BlockBodiesV65)

        # Track incomplete block body download tasks
        # - arbitrarily allow several requests-worth of headers queued up
        # - try to get bodies from lower block numbers first
        buffer_size = MAX_BODIES_FETCH * REQUEST_BUFFER_MULTIPLIER
        self._block_body_tasks = TaskQueue(buffer_size, attrgetter('block_number'))

        # Track if there is capacity for more block importing
        self._db_buffer_capacity = asyncio.Event()
        self._db_buffer_capacity.set()  # start with capacity

        # Track if any headers have been received yet
        self._got_first_header = asyncio.Event()

        # Keep a copy of old state roots to use when previewing transactions
        # Preview needs a header's *parent's* state root. Sometimes the parent
        # header isn't in the database yet, so must be looked up here.
        self._block_hash_to_state_root: Dict[Hash32, Hash32] = {}

        # Keep track of some statistics, which is useful for deciding if syncing has stalled

        # What is the largest block number reported by a header? (before importing the block)
        self._highest_header_number = 0

        # What is the most recently imported block number? (after importing the block)
        self._latest_block_number = 0

    async def run(self) -> None:
        with self.subscribe(self._peer_pool):
            await self.manager.wait_finished()

    async def _sync_from_headers(
            self,
            task_integrator: BaseOrderedTaskPreparation[BlockHeaderAPI, Hash32],
            completion_check: Callable[[BlockHeaderAPI], Awaitable[bool]],
    ) -> AsyncIterator[Tuple[BlockHeaderAPI, ...]]:
        """
        Watch for new headers to be added to the queue, and add the prerequisite
        tasks as they become available.
        """
        get_headers_coro = self._header_syncer.new_sync_headers(HEADER_QUEUE_SIZE_TARGET)

        # Track the highest registered block header by number, purely for stats/logging
        highest_block_num = -1

        async for headers in get_headers_coro:
            self._got_first_header.set()
            for h in headers:
                self._block_hash_to_state_root[h.hash] = h.state_root
            try:
                # We might end up with duplicates that can be safely ignored.
                # Likely scenario: switched which peer downloads headers, and the new peer isn't
                # aware of some of the in-progress headers
                new_headers = task_integrator.register_tasks(headers, ignore_duplicates=True)
            except MissingDependency as missing_exc:
                # The parent of this header is not registered as a dependency yet.
                # Some reasons this might happen, in rough descending order of likelihood:
                #   - a normal fork: the canonical head isn't the parent of the first header synced
                #   - a bug: headers were queued out of order in new_sync_headers
                #   - a bug: old headers were pruned out of the tracker, but not in DB yet

                # Skip over all headers found in db, (could happen with a long backtrack)
                completed_headers, new_headers = await skip_complete_headers(
                    headers, completion_check)
                if completed_headers:
                    self.logger.debug(
                        "Chain sync skipping over (%d) already stored headers %s: %s..%s",
                        len(completed_headers),
                        humanize_integer_sequence(h.block_number for h in completed_headers),
                        completed_headers[0],
                        completed_headers[-1],
                    )
                    if not new_headers:
                        # no new headers to process, wait for next batch to come in
                        continue

                # If the parent header doesn't exist yet, this is a legit bug instead of a fork,
                # let the HeaderNotFound exception bubble up
                try:
                    parent_header = await self.db.coro_get_block_header_by_hash(
                        new_headers[0].parent_hash)
                    self._block_hash_to_state_root[parent_header.hash] = parent_header.state_root
                except HeaderNotFound:
                    await self._log_missing_parent(new_headers[0], highest_block_num, missing_exc)

                    # Nowhere to go from here, re-raise
                    raise

                # If this isn't a trivial case, log it as a possible fork
                canonical_head = await self.db.coro_get_canonical_head()
                if canonical_head not in new_headers and canonical_head != parent_header:
                    self.logger.info(
                        "Received a header before processing its parent during regular sync. "
                        "Canonical head is %s, the received header "
                        "is %s, with parent %s. This might be a fork, importing to determine if it "
                        "is the longest chain",
                        canonical_head,
                        new_headers[0],
                        parent_header,
                    )

                # Set first header's parent as finished
                task_integrator.set_finished_dependency(parent_header)
                # Re-register the header tasks, which will now succeed
                task_integrator.register_tasks(new_headers, ignore_duplicates=True)

            if not new_headers:
                continue
            else:
                header_numbers = [header.block_number for header in new_headers]
                self._highest_header_number = max(self._highest_header_number, *header_numbers)

            yield new_headers

            # Don't race ahead of the database, by blocking when the persistance queue is too long
            await self._db_buffer_capacity.wait()

            highest_block_num = max(new_headers[-1].block_number, highest_block_num)

    async def _assign_body_download_to_peers(self) -> None:
        """
        Loop indefinitely, assigning idle peers to download any block bodies needed for syncing.
        """
        while self.manager.is_running:
            # get headers for bodies that we need to download, preferring lowest block number
            batch_id, headers = await self._block_body_tasks.get(MAX_BODIES_FETCH)

            # from all the peers that are not currently downloading block bodies, get the
            # fastest
            peer = await self._body_peers.get_fastest()
            # NOTE: If there are any async calls between getting the peer above and the
            # run_task() below, we need to ensure the peer is still running, otherwise it may have
            # stopped and run_task() will raise a LifecycleError.
            peer.manager.run_task(self._run_body_download_batch, peer, batch_id, headers)

    async def _block_body_bundle_processing(self, bundles: Tuple[BlockBodyBundle, ...]) -> None:
        """
        By default, no body bundle processing is needed.

        Subclasses may choose to do some post-processing. Notably, fast sync immediately saves
        block body bundles to the database.
        """
        pass

    async def _run_body_download_batch(
            self,
            peer: ETHPeer,
            batch_id: int,
            all_headers: Sequence[BlockHeaderAPI]) -> None:
        """
        Given a single batch retrieved from self._block_body_tasks, get as many of the block bodies
        as possible, and mark them as complete.
        """

        non_trivial_headers = tuple(header for header in all_headers if not _is_body_empty(header))
        trivial_headers = tuple(header for header in all_headers if _is_body_empty(header))

        if trivial_headers:
            self.logger.debug2(
                "Found %d/%d trivial block bodies, skipping those requests",
                len(trivial_headers),
                len(all_headers),
            )

        # even if trivial_headers is (), assign it so the finally block can run, in case of error
        completed_headers = trivial_headers

        try:
            if non_trivial_headers:
                bundles, received_headers = await self._get_block_bodies(
                    peer, non_trivial_headers)
                await self._block_body_bundle_processing(bundles)
                completed_headers = trivial_headers + received_headers

        except BaseP2PError as exc:
            self.logger.info("Unexpected p2p error while downloading body from %s: %s", peer, exc)
            self.logger.debug("Problem downloading body from peer, dropping...", exc_info=True)
        else:
            if len(non_trivial_headers) == 0:
                # peer had nothing to do, so have it get back in line for processing
                self._body_peers.put_nowait(peer)
            elif len(completed_headers) > 0:
                # peer completed with at least 1 result, so have it get back in line for processing
                self._body_peers.put_nowait(peer)
            else:
                # peer returned no results, wait a while before trying again
                delay = EMPTY_PEER_RESPONSE_PENALTY
                self.logger.debug("Pausing %s for %.1fs, for sending 0 block bodies", peer, delay)
                loop = asyncio.get_event_loop()
                loop.call_later(delay, partial(self._body_peers.put_nowait, peer))
        finally:
            self._mark_body_download_complete(batch_id, completed_headers)

    def _mark_body_download_complete(
            self,
            batch_id: int,
            completed_headers: Sequence[BlockHeaderAPI]) -> None:
        self._block_body_tasks.complete(batch_id, completed_headers)

    async def _get_block_bodies(
            self,
            peer: ETHPeer,
            headers: Sequence[BlockHeaderAPI],
    ) -> Tuple[Tuple[BlockBodyBundle, ...], Tuple[BlockHeaderAPI, ...]]:
        """
        Request and return block bodies, pairing them with the associated headers.
        Store the bodies for later use, during block import (or persist).

        Note the difference from _request_block_bodies, which only issues the request,
        and doesn't pair the results with the associated block headers that were successfully
        delivered.
        """
        block_body_bundles = await self._request_block_bodies(peer, headers)

        if len(block_body_bundles) == 0:
            self.logger.debug(
                "Got block bodies for 0/%d headers from %s, from %r..%r",
                len(headers),
                peer,
                headers[0],
                headers[-1],
            )
            return tuple(), tuple()

        bodies_by_root = {
            (transaction_root, uncles_hash): block_body
            for block_body, (transaction_root, _), uncles_hash
            in block_body_bundles
        }

        header_roots = {header: (header.transaction_root, header.uncles_hash) for header in headers}

        completed_header_roots = valfilter(lambda root: root in bodies_by_root, header_roots)

        completed_headers = tuple(completed_header_roots.keys())

        # store bodies for later usage, during block import
        pending_bodies = {
            header: bodies_by_root[root]
            for header, root in completed_header_roots.items()
        }
        self._pending_bodies = merge(self._pending_bodies, pending_bodies)

        self.logger.debug(
            "Got block bodies for %d/%d headers from %s, from %r..%r",
            len(completed_header_roots),
            len(headers),
            peer,
            headers[0],
            headers[-1],
        )

        return block_body_bundles, completed_headers

    async def _request_block_bodies(
            self,
            peer: ETHPeer,
            batch: Sequence[BlockHeaderAPI]) -> Tuple[BlockBodyBundle, ...]:
        """
        Requests the batch of block bodies from the given peer, returning the
        returned block bodies data, or an empty tuple on an error.
        """
        self.logger.debug("Requesting block bodies for %d headers from %s", len(batch), peer)
        try:
            block_body_bundles = await peer.eth_api.get_block_bodies(tuple(batch))
        except asyncio.TimeoutError:
            self.logger.debug(
                "Timed out requesting block bodies for %d headers from %s", len(batch), peer,
            )
            return tuple()
        except CancelledError:
            self.logger.debug("Pending block bodies call to %r future cancelled", peer)
            return tuple()
        except OperationCancelled:
            self.logger.debug2("Pending block bodies call to %r operation cancelled", peer)
            return tuple()
        except PeerConnectionLost:
            self.logger.debug("Peer went away, cancelling the block body request and moving on...")
            return tuple()
        except UnknownAPI as exc:
            self.logger.debug(
                "Peer was missing API, cancelling the block body request and moving on... %r",
                exc,
            )
            return tuple()
        except Exception:
            self.logger.exception("Unknown error when getting block bodies from %s", peer)
            raise

        return block_body_bundles

    async def _log_missing_parent(
            self,
            first_header: BlockHeaderAPI,
            highest_block_num: int,
            missing_exc: Exception) -> None:
        self.logger.warning("Parent missing for header %r, restarting header sync", first_header)
        block_num = first_header.block_number
        try:
            local_header = await self.db.coro_get_canonical_block_header_by_number(block_num)
        except HeaderNotFound as exc:
            self.logger.debug("Could not find canonical header at #%d: %s", block_num, exc)
            local_header = None

        try:
            local_parent = await self.db.coro_get_canonical_block_header_by_number(
                BlockNumber(block_num - 1)
            )
        except HeaderNotFound as exc:
            self.logger.debug("Could not find canonical header parent at #%d: %s", block_num, exc)
            local_parent = None

        try:
            canonical_tip = await self.db.coro_get_canonical_head()
        except HeaderNotFound as exc:
            self.logger.debug("Could not find canonical tip: %s", exc)
            canonical_tip = None

        self.logger.debug(
            (
                "Header syncer returned header %s, which has no parent in our DB. "
                "Instead at #%d, our header is %s, whose parent is %s, with canonical tip %s. "
                "The highest received header is %d. Triggered by missing dependency: %s"
            ),
            first_header,
            block_num,
            local_header,
            local_parent,
            canonical_tip,
            highest_block_num,
            missing_exc,
        )


class FastChainSyncer(Service):
    def __init__(self,
                 chain: AsyncChainAPI,
                 db: BaseAsyncChainDB,
                 peer_pool: ETHPeerPool,) -> None:
        self._header_syncer = ETHHeaderChainSyncer(chain, db, peer_pool)
        self._body_syncer = FastChainBodySyncer(
            chain,
            db,
            peer_pool,
            self._header_syncer,
        )

    @property
    def is_complete(self) -> bool:
        return self._body_syncer.is_complete

    async def run(self) -> None:
        self.manager.run_daemon_child_service(self._header_syncer)
        async with background_asyncio_service(self._body_syncer) as manager:
            # The body syncer will exit when the body for the target header hash has been
            # persisted
            await manager.wait_finished()
        self.manager.cancel()


@enum.unique
class BlockPersistPrereqs(enum.Enum):
    STORE_BLOCK_BODIES = enum.auto()
    STORE_RECEIPTS = enum.auto()


class ChainSyncStats(NamedTuple):
    prev_head: BlockHeaderAPI
    latest_head: BlockHeaderAPI

    elapsed: float

    num_blocks: int
    blocks_per_second: float

    num_transactions: int
    transactions_per_second: float


class ChainSyncPerformanceTracker:
    def __init__(self, head: BlockHeaderAPI) -> None:
        # The `head` from the previous time we reported stats
        self.prev_head = head
        # The latest `head` we have synced
        self.latest_head = head

        # A `Timer` object to report elapsed time between reports
        self.timer = Timer()

        # EMA of the blocks per second
        self.blocks_per_second_ema = EMA(initial_value=0, smoothing_factor=0.05)

        # EMA of the transactions per second
        self.transactions_per_second_ema = EMA(initial_value=0, smoothing_factor=0.05)

        # Number of transactions processed
        self.num_transactions = 0

    def record_transactions(self, count: int) -> None:
        self.num_transactions += count

    def set_latest_head(self, head: BlockHeaderAPI) -> None:
        self.latest_head = head

    def report(self) -> ChainSyncStats:
        elapsed = self.timer.pop_elapsed()

        num_blocks = self.latest_head.block_number - self.prev_head.block_number
        blocks_per_second = num_blocks / elapsed
        transactions_per_second = self.num_transactions / elapsed

        self.blocks_per_second_ema.update(blocks_per_second)
        self.transactions_per_second_ema.update(transactions_per_second)

        stats = ChainSyncStats(
            prev_head=self.prev_head,
            latest_head=self.latest_head,
            elapsed=elapsed,
            num_blocks=num_blocks,
            blocks_per_second=self.blocks_per_second_ema.value,
            num_transactions=self.num_transactions,
            transactions_per_second=self.transactions_per_second_ema.value,
        )

        # reset the counters
        self.num_transactions = 0
        self.prev_head = self.latest_head

        return stats


class FastChainBodySyncer(BaseBodyChainSyncer):
    """
    Sync with the Ethereum network by fetching block headers/bodies and storing them in our DB.

    Here, the run() method returns as soon as we complete a sync with the peer that announced the
    highest TD, at which point we must run the StateDownloader to fetch the state for our chain
    head.
    """
    def __init__(self,
                 chain: AsyncChainAPI,
                 db: BaseAsyncChainDB,
                 peer_pool: ETHPeerPool,
                 header_syncer: HeaderSyncerAPI) -> None:
        super().__init__(chain, db, peer_pool, header_syncer)

        # queue up any idle peers, in order of how fast they return receipts
        self._receipt_peers: WaitingPeers[ETHPeer] = WaitingPeers(commands.ReceiptsV65)

        # Track receipt download tasks
        # - arbitrarily allow several requests-worth of headers queued up
        # - try to get receipts from lower block numbers first
        buffer_size = MAX_RECEIPTS_FETCH * REQUEST_BUFFER_MULTIPLIER
        self._receipt_tasks = TaskQueue(buffer_size, attrgetter('block_number'))

        # track when both bodies and receipts are collected, so that blocks can be persisted
        self._block_persist_tracker = OrderedTaskPreparation(
            BlockPersistPrereqs,
            id_extractor=attrgetter('hash'),
            # make sure that a block is not persisted until the parent block is persisted
            dependency_extractor=attrgetter('parent_hash'),
        )
        # Track whether the fast chain syncer completed its goal
        self.is_complete = False

    async def _sync_from(self) -> BlockHeaderAPI:
        """
        Select which header should be the last known header.
        Start by importing headers that are children of this tip.
        """
        return await self.db.coro_get_canonical_head()

    async def run(self) -> None:
        head = await self._sync_from()
        self.tracker = ChainSyncPerformanceTracker(head)

        self._block_persist_tracker.set_finished_dependency(head)
        self.manager.run_daemon_task(self._launch_prerequisite_tasks)
        self.manager.run_daemon_task(self._assign_receipt_download_to_peers)
        self.manager.run_daemon_task(self._assign_body_download_to_peers)
        self.manager.run_daemon_task(self._persist_ready_blocks)
        self.manager.run_daemon_task(self._display_stats)
        # This wil block until we're complete, at which point self._persist_ready_blocks detects
        # it and cancel()s ourselves.
        await super().run()

    def register_peer(self, peer: BasePeer) -> None:
        # when a new peer is added to the pool, add it to the idle peer lists
        super().register_peer(peer)
        peer = cast(ETHPeer, peer)
        self._body_peers.put_nowait(peer)
        self._receipt_peers.put_nowait(peer)

    async def _should_skip_header(self, header: BlockHeaderAPI) -> bool:
        """
        Should we skip trying to import this header?
        Return True if the syncing of header appears to be complete.
        This is fairly relaxed about the definition, preferring speed over slow precision.
        """
        return await self.db.coro_header_exists(header.hash)

    async def _launch_prerequisite_tasks(self) -> None:
        """
        Watch for new headers to be added to the queue, and add the prerequisite
        tasks as they become available.
        """
        async for headers in self._sync_from_headers(
                self._block_persist_tracker,
                self._should_skip_header):

            # Sometimes duplicates are added to the queue, when switching from one sync to another.
            # We can simply ignore them.
            new_body_tasks = tuple(h for h in headers if h not in self._block_body_tasks)
            new_receipt_tasks = tuple(h for h in headers if h not in self._receipt_tasks)

            # if any one of the output queues gets full, hang until there is room
            await asyncio.gather(
                self._block_body_tasks.add(new_body_tasks),
                self._receipt_tasks.add(new_receipt_tasks),
            )

    async def _display_stats(self) -> None:
        while self.manager.is_running:
            await asyncio.sleep(5)
            self.logger.debug(
                "(in progress, queued, max size) of bodies, receipts: %r. Write capacity? %s",
                [(q.num_in_progress(), len(q), q._maxsize) for q in (
                    self._block_body_tasks,
                    self._receipt_tasks,
                )],
                "yes" if self._db_buffer_capacity.is_set() else "no",
            )

            stats = self.tracker.report()
            utcnow = int(datetime.datetime.utcnow().timestamp())
            head_age = utcnow - stats.latest_head.timestamp
            self.logger.info(
                (
                    "blks=%-4d  "
                    "txs=%-5d  "
                    "bps=%-3d  "
                    "tps=%-4d  "
                    "elapsed=%0.1f  "
                    "head=#%d %s  "
                    "age=%s"
                ),
                stats.num_blocks,
                stats.num_transactions,
                stats.blocks_per_second,
                stats.transactions_per_second,
                stats.elapsed,
                stats.latest_head.block_number,
                humanize_hash(stats.latest_head.hash),
                humanize_seconds(head_age),
            )

    async def _persist_ready_blocks(self) -> None:
        """
        Persist blocks as soon as all their prerequisites are done: body and receipt downloads.
        Persisting must happen in order, so that the block's parent has already been persisted.

        Also, determine if fast sync with this peer should end, having reached (or surpassed)
        its target hash. If so, shut down this service.
        """
        while self.manager.is_running:
            # This tracker waits for all prerequisites to be complete, and returns headers in
            # order, so that each header's parent is already persisted.
            get_completed_coro = self._block_persist_tracker.ready_tasks(BLOCK_QUEUE_SIZE_TARGET)
            completed_headers = await get_completed_coro

            self.chain.validate_chain_extension(completed_headers)

            if self._block_persist_tracker.has_ready_tasks():
                # Even after clearing out a big batch, there is no available capacity, so
                # pause any coroutines that might wait for capacity
                self._db_buffer_capacity.clear()
            else:
                # There is available capacity, let any waiting coroutines continue
                self._db_buffer_capacity.set()

            await self._persist_blocks(completed_headers)

            target_hash = self._header_syncer.get_target_header_hash()

            if target_hash in [header.hash for header in completed_headers]:
                # exit the service when reaching the target hash
                self._mark_complete()
                break

    def _mark_complete(self) -> None:
        self.is_complete = True
        self.manager.cancel()

    async def _persist_blocks(self, headers: Sequence[BlockHeaderAPI]) -> None:
        """
        Persist blocks for the given headers, directly to the database

        :param headers: headers for which block bodies and receipts have been downloaded
        """
        for header in headers:
            vm_class = self.chain.get_vm_class(header)
            block_class = vm_class.get_block_class()

            if _is_body_empty(header):
                transactions: List[SignedTransactionAPI] = []
                uncles: List[BlockHeaderAPI] = []
            else:
                body = self._pending_bodies.pop(header)
                uncles = body.uncles

                # transaction data was already persisted in _block_body_bundle_processing, but
                # we need to include the transactions for them to be added to the hash->txn lookup
                tx_class = block_class.get_transaction_class()
                transactions = [tx_class.from_base_transaction(tx) for tx in body.transactions]

                # record progress in the tracker
                self.tracker.record_transactions(len(transactions))

            block = block_class(header, transactions, uncles)
            await self.db.coro_persist_block(block)
            self.tracker.set_latest_head(header)

    async def _assign_receipt_download_to_peers(self) -> None:
        """
        Loop indefinitely, assigning idle peers to download receipts needed for syncing.
        """
        while self.manager.is_running:
            # get headers for receipts that we need to download, preferring lowest block number
            batch_id, headers = await self._receipt_tasks.get(MAX_RECEIPTS_FETCH)

            # from all the peers that are not currently downloading receipts, get the fastest
            peer = await self._receipt_peers.get_fastest()
            # NOTE: If there are any async calls between getting the peer above and the
            # run_task() below, we need to ensure the peer is still running, otherwise it may have
            # stopped and run_task() will raise a LifecycleError.
            peer.manager.run_task(self._run_receipt_download_batch, peer, batch_id, headers)

    def _mark_body_download_complete(
            self,
            batch_id: int,
            completed_headers: Sequence[BlockHeaderAPI]) -> None:
        super()._mark_body_download_complete(batch_id, completed_headers)
        self._block_persist_tracker.finish_prereq(
            BlockPersistPrereqs.STORE_BLOCK_BODIES,
            completed_headers,
        )

    async def _run_receipt_download_batch(
            self,
            peer: ETHPeer,
            batch_id: int,
            headers: Sequence[BlockHeaderAPI]) -> None:
        """
        Given a single batch retrieved from self._receipt_tasks, get as many of the receipt bundles
        as possible, and mark them as complete.
        """
        # If there is an exception during _process_receipts, prepare to mark the task as finished
        # with no headers collected:
        completed_headers: Tuple[BlockHeaderAPI, ...] = tuple()
        try:
            completed_headers = await self._process_receipts(peer, headers)

            self._block_persist_tracker.finish_prereq(
                BlockPersistPrereqs.STORE_RECEIPTS,
                completed_headers,
            )
        except BaseP2PError as exc:
            self.logger.info("Unexpected p2p err while downloading receipt from %s: %s", peer, exc)
            self.logger.debug("Problem downloading receipt from peer, dropping...", exc_info=True)
        else:
            # peer completed successfully, so have it get back in line for processing
            if len(completed_headers) > 0:
                # peer completed successfully, so have it get back in line for processing
                self._receipt_peers.put_nowait(peer)
            else:
                # peer returned no results, wait a while before trying again
                delay = EMPTY_PEER_RESPONSE_PENALTY
                self.logger.debug("Pausing %s for %.1fs, for sending 0 receipts", peer, delay)
                loop = asyncio.get_event_loop()
                loop.call_later(delay, partial(self._receipt_peers.put_nowait, peer))
        finally:
            self._receipt_tasks.complete(batch_id, completed_headers)

    async def _block_body_bundle_processing(self, bundles: Tuple[BlockBodyBundle, ...]) -> None:
        """
        Fast sync writes all the block body bundle data directly to the database,
        in order to make it... fast.
        """
        for (_, (_, trie_data_dict), _) in bundles:
            await self.db.coro_persist_trie_data_dict(trie_data_dict)

    async def _process_receipts(
            self,
            peer: ETHPeer,
            all_headers: Sequence[BlockHeaderAPI]) -> Tuple[BlockHeaderAPI, ...]:
        """
        Downloads and persists the receipts for the given set of block headers.
        Some receipts may be trivial, having a blank root hash, and will not be requested.

        :param peer: to issue the receipt request to
        :param all_headers: attempt to get receipts for as many of these headers as possible
        :return: the headers for receipts that were successfully downloaded (or were trivial)
        """
        # Post-Byzantium blocks may have identical receipt roots (e.g. when they have the same
        # number of transactions and all succeed/failed: ropsten blocks 2503212 and 2503284),
        # so we do this to avoid requesting the same receipts multiple times.

        # combine headers with the same receipt root, so we can mark them as completed, later
        receipt_root_to_headers = groupby(attrgetter('receipt_root'), all_headers)

        # Ignore headers that have an empty receipt root
        trivial_headers = tuple(receipt_root_to_headers.pop(BLANK_ROOT_HASH, tuple()))

        # pick one of the headers for each missing receipt root
        unique_headers_needed = tuple(
            first(headers)
            for root, headers in receipt_root_to_headers.items()
        )

        if not unique_headers_needed:
            return trivial_headers

        receipt_bundles = await self._request_receipts(peer, unique_headers_needed)

        if not receipt_bundles:
            return trivial_headers

        try:
            await self._validate_receipts(unique_headers_needed, receipt_bundles)
        except ValidationError as err:
            self.logger.info(
                "Disconnecting from %s: sent invalid receipt: %s",
                peer,
                err,
            )
            await peer.disconnect(DisconnectReason.BAD_PROTOCOL)
            return trivial_headers

        # process all of the returned receipts, storing their trie data
        # dicts in the database
        receipts, trie_roots_and_data_dicts = zip(*receipt_bundles)
        receipt_roots, trie_data_dicts = zip(*trie_roots_and_data_dicts)
        for trie_data in trie_data_dicts:
            await self.db.coro_persist_trie_data_dict(trie_data)

        # Identify which headers have the receipt roots that are now complete.
        completed_header_groups = tuple(
            headers
            for root, headers in receipt_root_to_headers.items()
            if root in receipt_roots
        )
        newly_completed_headers = tuple(concat(completed_header_groups))

        self.logger.debug(
            "Got receipts for %d/%d headers from %s, %d trivial, from request for %r..%r",
            len(newly_completed_headers),
            len(all_headers) - len(trivial_headers),
            peer,
            len(trivial_headers),
            all_headers[0],
            all_headers[-1],
        )
        return newly_completed_headers + trivial_headers

    async def _validate_receipts(
            self,
            headers: Sequence[BlockHeaderAPI],
            receipt_bundles: Tuple[ReceiptBundle, ...]) -> None:

        header_by_root = {
            header.receipt_root: header
            for header in headers
            if not _is_receipts_empty(header)
        }
        receipts_by_root = {
            receipt_root: receipts
            for (receipts, (receipt_root, _))
            in receipt_bundles
            if receipt_root != BLANK_ROOT_HASH
        }
        for receipt_root, header in header_by_root.items():
            if receipt_root not in receipts_by_root:
                # this receipt group was not returned by the peer, skip validation
                continue
            for receipt in receipts_by_root[receipt_root]:
                await self.chain.coro_validate_receipt(receipt, header)

    async def _request_receipts(
            self,
            peer: ETHPeer,
            batch: Sequence[BlockHeaderAPI]) -> Tuple[ReceiptBundle, ...]:
        """
        Requests the batch of receipts from the given peer, returning the
        received receipt data.
        """
        self.logger.debug("Requesting receipts for %d headers from %s", len(batch), peer)
        try:
            receipt_bundles = await peer.eth_api.get_receipts(tuple(batch))
        except asyncio.TimeoutError:
            self.logger.debug(
                "Timed out requesting receipts for %d headers from %s", len(batch), peer,
            )
            return tuple()
        except CancelledError:
            self.logger.debug("Pending receipts call to %r future cancelled", peer)
            return tuple()
        except OperationCancelled:
            self.logger.debug2("Pending receipts call to %r operation cancelled", peer)
            return tuple()
        except PeerConnectionLost:
            self.logger.debug("Peer went away, cancelling the receipts request and moving on...")
            return tuple()
        except Exception:
            self.logger.exception("Unknown error when getting receipts")
            raise

        if not receipt_bundles:
            return tuple()

        return receipt_bundles


class RegularChainSyncer(Service):
    def __init__(self,
                 chain: AsyncChainAPI,
                 db: BaseAsyncChainDB,
                 peer_pool: ETHPeerPool) -> None:
        self._header_syncer = ETHHeaderChainSyncer(chain, db, peer_pool)
        self._body_syncer = RegularChainBodySyncer(
            chain,
            db,
            peer_pool,
            self._header_syncer,
            SimpleBlockImporter(chain),
        )

    async def run(self) -> None:
        self.manager.run_daemon_child_service(self._header_syncer)
        self.manager.run_daemon_child_service(self._body_syncer)
        # run regular sync until cancelled
        await self.manager.wait_finished()


@enum.unique
class BlockImportPrereqs(enum.Enum):
    STORE_BLOCK_BODIES = enum.auto()


class RegularChainBodySyncer(BaseBodyChainSyncer):
    """
    Sync with the Ethereum network by fetching block headers/bodies and importing them.

    Here, the run() method will execute the sync loop forever, until we're stopped.
    """

    def __init__(self,
                 chain: AsyncChainAPI,
                 db: BaseAsyncChainDB,
                 peer_pool: ETHPeerPool,
                 header_syncer: HeaderSyncerAPI,
                 block_importer: BaseBlockImporter) -> None:
        super().__init__(chain, db, peer_pool, header_syncer)

        # track when block bodies are downloaded, so that blocks can be imported
        self._block_import_tracker = OrderedTaskPreparation(
            BlockImportPrereqs,
            id_extractor=attrgetter('hash'),
            # make sure that a block is not imported until the parent block is imported
            dependency_extractor=attrgetter('parent_hash'),
            # Avoid problems by keeping twice as much data as the import queue size
            max_depth=BLOCK_IMPORT_QUEUE_SIZE * 2,
        )
        self._block_importer = block_importer

        # Track if any headers have been received yet
        self._got_first_header = asyncio.Event()

        # Rate limit the block import logs
        self._import_log_limiter = TokenBucket(
            0.33,  # show about one log per 3 seconds
            5,  # burst up to 5 logs after a lag
        )

        # the queue of blocks that are downloaded and ready to be imported
        self._import_queue: 'asyncio.Queue[BlockAPI]' = asyncio.Queue(BLOCK_IMPORT_QUEUE_SIZE)

        self._import_active = asyncio.Lock()

    async def run(self) -> None:
        head = await self.db.coro_get_canonical_head()
        self._block_import_tracker.set_finished_dependency(head)
        self.manager.run_daemon_task(self._launch_prerequisite_tasks)
        self.manager.run_daemon_task(self._assign_body_download_to_peers)
        self.manager.run_daemon_task(self._import_ready_blocks)
        self.manager.run_daemon_task(self._preview_ready_blocks)
        self.manager.run_daemon_task(self._display_stats)
        await super().run()

    def register_peer(self, peer: BasePeer) -> None:
        # when a new peer is added to the pool, add it to the idle peer list
        super().register_peer(peer)
        self._body_peers.put_nowait(cast(ETHPeer, peer))

    async def _should_skip_header(self, header: BlockHeaderAPI) -> bool:
        """
        Should we skip trying to import this header?
        Return True if the syncing of header appears to be complete.
        This is fairly relaxed about the definition, preferring speed over slow precision.
        """
        return await self.db.coro_exists(header.state_root)

    async def _launch_prerequisite_tasks(self) -> None:
        """
        Watch for new headers to be added to the queue, and add the prerequisite
        tasks (downloading block bodies) as they become available.
        """
        async for headers in self._sync_from_headers(
                self._block_import_tracker,
                self._should_skip_header):

            # Sometimes duplicates are added to the queue, when switching from one sync to another.
            # We can simply ignore them.
            new_headers = tuple(h for h in headers if h not in self._block_body_tasks)

            # if the output queue gets full, hang until there is room
            await self._block_body_tasks.add(new_headers)

    def _mark_body_download_complete(
            self,
            batch_id: int,
            completed_headers: Sequence[BlockHeaderAPI]) -> None:
        super()._mark_body_download_complete(batch_id, completed_headers)
        self._block_import_tracker.finish_prereq(
            BlockImportPrereqs.STORE_BLOCK_BODIES,
            completed_headers,
        )

    async def _preview_ready_blocks(self) -> None:
        """
        Wait for block bodies to be downloaded, then compile the blocks and
        preview them to the importer.

        It's important to do this in a separate step from importing so that
        previewing can get ahead of import by a few blocks.
        """
        await self._got_first_header.wait()
        while self.manager.is_running:
            # This tracker waits for all prerequisites to be complete, and returns headers in
            # order, so that each header's parent is already persisted.
            get_ready_coro = self._block_import_tracker.ready_tasks(1)
            completed_headers = await get_ready_coro

            if self._block_import_tracker.has_ready_tasks():
                # Even after clearing out a big batch, there is no available capacity, so
                # pause any coroutines that might wait for capacity
                self._db_buffer_capacity.clear()
            else:
                # There is available capacity, let any waiting coroutines continue
                self._db_buffer_capacity.set()

            header = completed_headers[0]
            block = self._header_to_block(header)

            # Put block in short queue for import, wait here if queue is full
            await self._import_queue.put(block)

            # Load the state root of the parent header
            try:
                parent_state_root = self._block_hash_to_state_root[header.parent_hash]
            except KeyError:
                # For the very first header that we load, we have to look up the parent's
                # state from the database:
                parent = await self.chain.coro_get_block_header_by_hash(header.parent_hash)
                parent_state_root = parent.state_root

            # Emit block for preview
            #   - look up the addresses referenced by the transaction (eg~ sender and recipient)
            #   - execute the block ahead of time to start collecting any missing state
            #   - store the header (for future evm execution that might look up old block hashes)
            await self._block_importer.preview_transactions(
                header,
                block.transactions,
                parent_state_root,
            )

    async def _import_ready_blocks(self) -> None:
        """
        Wait for block bodies to be downloaded, then compile the blocks and
        preview them to the importer.
        """
        await self._got_first_header.wait()
        while self.manager.is_running:
            if self._import_queue.empty():
                if self._import_active.locked():
                    self._import_active.release()
                waiting_for_next_block = Timer()

            block = await self._import_queue.get()
            if not self._import_active.locked():
                self.logger.info(
                    "Waited %.1fs for %s body",
                    waiting_for_next_block.elapsed,
                    block.header,
                )
                await self._import_active.acquire()

            await self._import_block(block)

    def get_block_count_lag(self) -> int:
        if self._latest_block_number == 0:
            return 0
        else:
            return self._highest_header_number - self._latest_block_number

    async def _import_block(self, block: BlockAPI) -> None:
        timer = Timer()

        # Log the latest import block so that we can accurately report lag
        self._latest_block_number = block.number

        import_result = await self._block_importer.import_block(block)
        new_canonical_blocks = import_result.new_canonical_blocks
        old_canonical_blocks = import_result.old_canonical_blocks

        # How much is the imported block's header behind the current time?
        lag = time.time() - block.header.timestamp
        humanized_lag = humanize_seconds(lag)

        blocks_behind = self.get_block_count_lag()

        if new_canonical_blocks == (block,):
            # simple import of a single new block.

            # decide whether to log to info or debug, based on log rate
            if self._import_log_limiter.can_take(1):
                log_fn = self.logger.info
                self._import_log_limiter.take_nowait(1)
            else:
                log_fn = self.logger.debug
            log_fn(
                "Imported block %d (%d txs) in %.2f seconds, lagging %d blocks | %s",
                block.number,
                len(block.transactions),
                timer.elapsed,
                blocks_behind,
                humanized_lag,
            )
        elif not new_canonical_blocks:
            # imported block from a fork.
            self.logger.info(
                "Imported non-canonical block %d (%d txs) in %.2f seconds, lagging %d blocks | %s",
                block.number,
                len(block.transactions),
                timer.elapsed,
                blocks_behind,
                humanized_lag,
            )
        elif old_canonical_blocks:
            self.logger.info(
                "Chain Reorganization: Imported block %d (%d txs) in %.2f seconds, "
                "%d blocks discarded and %d new canonical blocks added, lagging %d blocks | %s",
                block.number,
                len(block.transactions),
                timer.elapsed,
                len(old_canonical_blocks),
                len(new_canonical_blocks),
                blocks_behind,
                humanized_lag,
            )
        else:
            raise Exception("Invariant: unreachable code path")

    def _header_to_block(self, header: BlockHeaderAPI) -> BlockAPI:
        """
        This method converts a header that was queued up for sync into its full block
        representation. It may not be called until after the body is marked as fully
        downloaded, as tracked by self._block_import_tracker.
        """
        vm_class = self.chain.get_vm_class(header)
        block_class = vm_class.get_block_class()

        if _is_body_empty(header):
            transactions: List[SignedTransactionAPI] = []
            uncles: List[BlockHeaderAPI] = []
        else:
            body = self._pending_bodies.pop(header)
            tx_class = block_class.get_transaction_class()
            transactions = [tx_class.from_base_transaction(tx)
                            for tx in body.transactions]
            uncles = body.uncles

        return block_class(header, transactions, uncles)

    async def _display_stats(self) -> None:
        self.logger.debug("Regular sync waiting for first header to arrive")
        await self._got_first_header.wait()
        self.logger.debug("Regular sync first header arrived")

        while self.manager.is_running:
            await asyncio.sleep(5)
            self.logger.debug(
                "(progress, queued, max) of bodies, receipts: %r. Write capacity? %s Importing? %s",
                [(q.num_in_progress(), len(q), q._maxsize) for q in (
                    self._block_body_tasks,
                )],
                self._db_buffer_capacity.is_set(),
                self._import_active.locked(),
            )


def _is_body_empty(header: BlockHeaderAPI) -> bool:
    return header.transaction_root == BLANK_ROOT_HASH and header.uncles_hash == EMPTY_UNCLE_HASH


def _is_receipts_empty(header: BlockHeaderAPI) -> bool:
    return header.receipt_root == BLANK_ROOT_HASH
