import asyncio
import time
from typing import (
    AsyncIterator,
    Iterable,
    Sequence,
    Set,
    Tuple,
)

from async_service import Service, background_asyncio_service

from lahja import EndpointAPI
from pyformance import MetricsRegistry

from eth.abc import (
    AtomicDatabaseAPI,
    BlockAPI,
    BlockHeaderAPI,
    BlockImportResult,
    DatabaseAPI,
    SignedTransactionAPI,
)
from eth.constants import GENESIS_PARENT_HASH
from eth.typing import BlockRange
from eth_typing import (
    Address,
    BlockNumber,
    Hash32,
)
from eth_utils import (
    ValidationError,
)
import rlp

from trinity.chains.base import AsyncChainAPI
from trinity.constants import FIRE_AND_FORGET_BROADCASTING
from trinity.db.eth1.chain import BaseAsyncChainDB
from trinity.db.eth1.header import BaseAsyncHeaderDB
from trinity.exceptions import (
    WitnessHashesUnavailable,
    BaseTrinityError,
)
from trinity.protocol.eth.peer import ETHPeerPool
from trinity.protocol.eth.sync import ETHHeaderChainSyncer
from trinity.protocol.wit.db import AsyncWitnessDB
from trinity.sync.beam.constants import (
    BEAM_PIVOT_BUFFER_FRACTION,
    BLOCK_BACKFILL_IDLE_TIME,
    BLOCK_IMPORT_MISSING_STATE_TIMEOUT,
    FULL_BLOCKS_NEEDED_TO_START_BEAM,
    MAX_BEAM_SYNC_LAG,
    PAUSE_BACKFILL_AT_LAG,
    RESUME_BACKFILL_AT_LAG,
    PREDICTED_BLOCK_TIME,
)
from trinity.sync.beam.queen import (
    QueenTrackerAPI,
    QueeningQueue,
)
from trinity.sync.common.constants import (
    MAX_BACKFILL_BLOCK_BODIES_AT_ONCE,
)
from trinity.sync.common.checkpoint import (
    Checkpoint,
)
from trinity.sync.common.chain import (
    BaseBlockImporter,
)
from trinity.sync.common.events import (
    CollectMissingAccount,
    CollectMissingBytecode,
    CollectMissingStorage,
    CollectMissingTrieNodes,
    DoStatelessBlockImport,
    DoStatelessBlockPreview,
    FetchBlockWitness,
    MissingAccountResult,
    MissingBytecodeResult,
    MissingStorageResult,
    MissingTrieNodesResult,
)
from trinity.sync.common.headers import (
    DatabaseBlockRangeHeaderSyncer,
    HeaderSyncerAPI,
    ManualHeaderSyncer,
    persist_headers,
)
from trinity.sync.common.strategies import (
    FromCheckpointLaunchStrategy,
    FromGenesisLaunchStrategy,
    SyncLaunchStrategyAPI,
)
from trinity.sync.full.chain import (
    FastChainBodySyncer,
    RegularChainBodySyncer,
)
from trinity.sync.header.chain import SequentialHeaderChainGapSyncer
from trinity.sync.beam.state import (
    BeamDownloader,
)
from trinity._utils.pauser import Pauser
from trinity._utils.timer import Timer
from trinity._utils.logging import get_logger
from trinity._utils.headers import body_for_header_exists

from .backfill import BeamStateBackfill

STATS_DISPLAY_PERIOD = 10


class BeamSyncer(Service):
    """
    Organizes several moving parts to coordinate beam sync. Roughly:

        - Sync *only* headers up until you have caught up with a peer, ie~ the launchpoint
        - Launch a service responsible for serving event bus requests for missing state data
        - When you catch up with a peer, start downloading transactions needed to execute a block
        - At the launchpoint, switch to full block imports, with a custom importer

    This syncer relies on a seperately orchestrated beam sync component, which:

        - listens for DoStatelessBlockImport events
        - emits events when data is missing, like CollectMissingAccount
        - emits StatelessBlockImportDone when the block import is completed in the DB

    There is an option, currently only used for testing, to force beam sync at a particular
    block number (rather than trigger it when catching up with a peer).
    """
    def __init__(
            self,
            chain: AsyncChainAPI,
            db: AtomicDatabaseAPI,
            chain_db: BaseAsyncChainDB,
            peer_pool: ETHPeerPool,
            event_bus: EndpointAPI,
            metrics_registry: MetricsRegistry,
            checkpoint: Checkpoint = None,
            force_beam_block_number: BlockNumber = None,
            enable_backfill: bool = True,
            enable_state_backfill: bool = True) -> None:
        self.logger = get_logger('trinity.sync.beam.chain.BeamSyncer')

        self.metrics_registry = metrics_registry
        self._body_for_header_exists = body_for_header_exists(chain_db, chain)

        if checkpoint is None:
            self._launch_strategy: SyncLaunchStrategyAPI = FromGenesisLaunchStrategy(chain_db)
        else:
            self._launch_strategy = FromCheckpointLaunchStrategy(
                chain_db,
                chain,
                checkpoint,
                peer_pool,
            )

        self._header_syncer = ETHHeaderChainSyncer(
            chain,
            chain_db,
            peer_pool,
            self._launch_strategy,
        )
        self._header_persister = HeaderOnlyPersist(
            self._header_syncer,
            chain_db,
            force_beam_block_number,
            self._launch_strategy,
        )

        self._backfiller = BeamStateBackfill(db, peer_pool)

        if enable_state_backfill:
            self._queen_queue: QueenTrackerAPI = self._backfiller
        else:
            self._queen_queue = QueeningQueue(peer_pool)

        self._state_downloader = BeamDownloader(
            db,
            peer_pool,
            self._queen_queue,
            event_bus,
        )
        self._data_hunter = MissingDataEventHandler(
            self._state_downloader,
            event_bus,
            self.metrics_registry,
        )

        self._block_importer = BeamBlockImporter(
            chain,
            db,
            self._state_downloader,
            self._backfiller,
            event_bus,
            self.metrics_registry,
        )
        self._launchpoint_header_syncer = HeaderLaunchpointSyncer(self._header_syncer)
        self._body_syncer = RegularChainBodySyncer(
            chain,
            chain_db,
            peer_pool,
            self._launchpoint_header_syncer,
            self._block_importer,
        )

        self._manual_header_syncer = ManualHeaderSyncer()
        self._fast_syncer = RigorousFastChainBodySyncer(
            chain,
            chain_db,
            peer_pool,
            self._manual_header_syncer,
        )

        self._header_backfill = SequentialHeaderChainGapSyncer(chain, chain_db, peer_pool)
        self._block_backfill = BodyChainGapSyncer(chain, chain_db, peer_pool)

        self._chain = chain
        self._enable_backfill = enable_backfill
        self._enable_state_backfill = enable_state_backfill

    async def run(self) -> None:

        try:
            await self._launch_strategy.fulfill_prerequisites()
        except asyncio.TimeoutError as exc:
            self.logger.exception(
                "Timed out while trying to fulfill prerequisites of "
                f"sync launch strategy: {exc} from {self._launch_strategy}"
            )
            self.manager.cancel()
            return

        self.manager.run_daemon_child_service(self._block_importer)
        self.manager.run_daemon_child_service(self._header_syncer)

        # Kick off the body syncer early (it hangs on the launchpoint header syncer anyway)
        # It needs to start early because we want to "re-run" the header at the tip,
        # which it gets grumpy about. (it doesn't want to receive the canonical header tip
        # as a header to process)
        self.manager.run_daemon_child_service(self._body_syncer)

        # Launch the state syncer endpoint early
        self.manager.run_daemon_child_service(self._data_hunter)

        # Only persist headers at start
        async with background_asyncio_service(self._header_persister) as manager:
            await manager.wait_finished()
        # When header store exits, we have caught up

        # We want to trigger beam sync on the last block received,
        # not wait for the next one to be broadcast
        final_headers = self._header_persister.get_final_headers()

        # First, download block bodies for previous 6 blocks, for validation
        await self._download_blocks(final_headers[0])

        # Now, tell the MissingDataEventHandler about the minimum acceptable block number for
        # data requests. This helps during pivots to quickly reject requests from old block imports
        self._data_hunter.minimum_beam_block_number = min(
            header.block_number for header in final_headers
        )

        # Now let the beam sync importer kick in
        self._launchpoint_header_syncer.set_launchpoint_headers(final_headers)

        # We wait until beam sync has launched before starting backfill, because
        #   they both request block bodies, but beam sync needs them urgently.
        if self._enable_backfill:
            # There's no chance to introduce new gaps after this point. Therefore we can run this
            # until it has filled all gaps and let it finish.
            self.manager.run_child_service(self._header_backfill)

            # In contrast, block gap fill needs to run indefinitely because of beam sync pivoting.
            self.manager.run_daemon_child_service(self._block_backfill)

            # Now we can check the lag (presumably ~0) and start backfill
            self.manager.run_daemon_task(self._monitor_historical_backfill)

        # Will start the state background service or the basic queen queue
        self.manager.run_child_service(self._queen_queue)

        # TODO wait until first header with a body comes in?...
        # Start state downloader service
        self.manager.run_daemon_child_service(self._state_downloader)

        # run sync until cancelled
        await self.manager.wait_finished()

    def get_block_count_lag(self) -> int:
        """
        :return: the difference in block number between the currently importing block and
            the latest known block
        """
        return self._body_syncer.get_block_count_lag()

    async def _download_blocks(self, before_header: BlockHeaderAPI) -> None:
        """
        When importing a block, we need to validate uncles against the previous
        six blocks, so download those bodies and persist them to the database.
        """
        parents_needed = FULL_BLOCKS_NEEDED_TO_START_BEAM

        self.logger.info(
            "Downloading %d block bodies for uncle validation, before %s",
            parents_needed,
            before_header,
        )

        # select the recent ancestors to sync block bodies for
        parent_headers = tuple(reversed([
            header async for header
            in self._get_ancestors(parents_needed, header=before_header)
        ]))

        # identify starting tip and headers with possible uncle conflicts for validation
        if len(parent_headers) < parents_needed:
            self.logger.info(
                "Collecting %d blocks to genesis for uncle validation",
                len(parent_headers),
            )
            sync_from_tip = await self._chain.coro_get_canonical_block_header_by_number(
                BlockNumber(0)
            )
            uncle_conflict_headers = parent_headers
        else:
            sync_from_tip = parent_headers[0]
            uncle_conflict_headers = parent_headers[1:]

        # check if we already have the blocks for the uncle conflict headers
        if await self._all_verification_bodies_present(uncle_conflict_headers):
            self.logger.debug("All needed block bodies are already available")
        else:
            # tell the header syncer to emit those headers
            self._manual_header_syncer.emit(uncle_conflict_headers)

            # tell the fast syncer which tip to start from
            self._fast_syncer.set_starting_tip(sync_from_tip)

            # run the fast syncer (which downloads block bodies and then exits)
            self.logger.info("Getting recent block data for uncle validation")
            async with background_asyncio_service(self._fast_syncer) as manager:
                await manager.wait_finished()

        # When this completes, we have all the uncles needed to validate
        self.logger.info("Have all data needed for Beam validation, continuing...")

    async def _get_ancestors(self,
                             limit: int,
                             header: BlockHeaderAPI) -> AsyncIterator[BlockHeaderAPI]:
        """
        Return `limit` number of ancestor headers from the specified header.
        """
        headers_returned = 0
        while header.parent_hash != GENESIS_PARENT_HASH and headers_returned < limit:
            parent = await self._chain.coro_get_block_header_by_hash(header.parent_hash)
            yield parent
            headers_returned += 1
            header = parent

    async def _all_verification_bodies_present(
            self,
            headers_with_potential_conflicts: Iterable[BlockHeaderAPI]) -> bool:

        for header in headers_with_potential_conflicts:
            if not await self._body_for_header_exists(header):
                return False
        return True

    async def _monitor_historical_backfill(self) -> None:
        while self.manager.is_running:
            await asyncio.sleep(PREDICTED_BLOCK_TIME)
            if self._block_backfill.get_manager().is_cancelled:
                return
            else:
                lag = self.get_block_count_lag()
                if lag >= PAUSE_BACKFILL_AT_LAG and not self._block_backfill.is_paused:
                    self.logger.debug(
                        "Pausing historical header/block sync because we lag %s blocks",
                        lag,
                    )
                    self._block_backfill.pause()
                    self._header_backfill.pause()
                elif lag <= RESUME_BACKFILL_AT_LAG and self._block_backfill.is_paused:
                    self.logger.debug(
                        "Resuming historical header/block sync because we lag %s blocks",
                        lag,
                    )
                    self._block_backfill.resume()
                    self._header_backfill.resume()


class RigorousFastChainBodySyncer(Service):
    """
    Very much like the regular FastChainBodySyncer, but does a more robust
    check about whether we should skip syncing a header's body. We explicitly
    check if the body has been downloaded, instead of just trusting that if
    the header is present than the body must be. This is helpful, because
    the previous syncer is a header-only syncer.
    """
    _starting_tip: BlockHeaderAPI = None

    def __init__(self,
                 chain: AsyncChainAPI,
                 db: BaseAsyncChainDB,
                 peer_pool: ETHPeerPool,
                 header_syncer: HeaderSyncerAPI) -> None:
        self._body_syncer = FastChainBodySyncer(
            chain,
            db,
            peer_pool,
            header_syncer,
            launch_header_fn=self._sync_from,
            should_skip_header_fn=body_for_header_exists(db, chain)
        )

    async def _sync_from(self) -> BlockHeaderAPI:
        """
        Typically, the FastChainBodySyncer always starts syncing from the tip of the chain,
        but we actually want to sync from *behind* the tip, so we manually set the sync-from
        target.
        """
        if self._starting_tip is None:
            raise ValidationError("Must set a previous tip before rigorous-fast-syncing")
        else:
            return self._starting_tip

    def set_starting_tip(self, header: BlockHeaderAPI) -> None:
        """
        Explicitly set the sync-from target, to use instead of the canonical head.
        """
        self._starting_tip = header

    async def run(self) -> None:

        await self.manager.run_service(self._body_syncer)


class BodyChainGapSyncer(Service):
    """
    A service to sync historical blocks without executing them. This service is meant to be run
    in tandem with other operations that sync the state.
    """

    _idle_time = BLOCK_BACKFILL_IDLE_TIME
    _starting_tip: BlockHeaderAPI = None

    logger = get_logger('trinity.sync.beam.chain.BodyChainGapSyncer')

    def __init__(self,
                 chain: AsyncChainAPI,
                 db: BaseAsyncChainDB,
                 peer_pool: ETHPeerPool) -> None:
        self._chain = chain
        self._db = db
        self._peer_pool = peer_pool
        self._pauser = Pauser()
        self._body_syncer: FastChainBodySyncer = None
        self._max_backfill_block_bodies_at_once = MAX_BACKFILL_BLOCK_BODIES_AT_ONCE

    async def _setup_for_next_gap(self) -> None:
        gap_start, gap_end = self._get_next_gap()
        fill_start = BlockNumber(max(
            gap_start,
            gap_end - self._max_backfill_block_bodies_at_once,
        ))
        start_num = BlockNumber(fill_start - 1)
        _starting_tip = await self._db.coro_get_canonical_block_header_by_number(start_num)

        if self._pauser.is_paused:
            # If the syncer was paused while we were busy setting it up throw the current setup
            # away. A new setup will be performed as soon as `resume()` was called again.
            raise ValidationError("Syncer was paused by the user")

        async def _get_launch_header() -> BlockHeaderAPI:
            return _starting_tip

        self.logger.debug("Starting to sync missing blocks from #%s to #%s", fill_start, gap_end)

        self._body_syncer = FastChainBodySyncer(
            self._chain,
            self._db,
            self._peer_pool,
            DatabaseBlockRangeHeaderSyncer(self._db, (fill_start, gap_end,)),
            launch_header_fn=_get_launch_header,
            should_skip_header_fn=body_for_header_exists(self._db, self._chain)
        )
        self._body_syncer.logger = self.logger

    def _get_next_gap(self) -> BlockRange:
        gaps, future_tip_block = self._db.get_chain_gaps()
        header_gaps, future_tip_header = self._db.get_header_chain_gaps()
        try:
            actionable_gap = self.get_topmost_actionable_gap(gaps, header_gaps)

        except NoActionableGap:
            # We do not have gaps in the chain of blocks but we may still have a gap from the last
            # block up until the highest consecutive written header.
            if len(header_gaps) > 0:
                # The header chain has gaps, find out the lowest missing header
                lowest_missing_header, _ = header_gaps[0]
            else:
                # It doesn't have gaps, so the future_tip_header is the lowest missing header
                lowest_missing_header = future_tip_header

            highest_consecutive_header = lowest_missing_header - 1
            if highest_consecutive_header >= future_tip_block:
                # The header before the lowest missing header is the highest consecutive header
                # that exists in the db and it is higher than the future tip block. That's a gap
                # we can try to close.
                return future_tip_block, BlockNumber(highest_consecutive_header)
            else:
                raise ValidationError("No gaps in the chain of blocks")
        else:
            return actionable_gap

    def get_topmost_actionable_gap(self,
                                   gaps: Tuple[BlockRange, ...],
                                   header_gaps: Tuple[BlockRange, ...]) -> BlockRange:
        '''
        Returns the most recent gap of blocks of max size = _max_backfill_block_bodies_at_once
        for which the headers exist in DB, along with the header preceding the gap.
        '''
        for gap in gaps[::-1]:
            if gap[1] - gap[0] > self._max_backfill_block_bodies_at_once:
                gap = (BlockNumber(gap[1] - self._max_backfill_block_bodies_at_once), gap[1])
            # We want to be sure the header preceding the block gap is in DB
            gap_with_prev_block = (BlockNumber(gap[0] - 1), gap[1])
            for header_gap in header_gaps[::-1]:
                if not self._have_empty_intersection(gap_with_prev_block, header_gap):
                    break
            else:
                return gap
        else:
            raise NoActionableGap

    def _have_empty_intersection(self, block_gap: BlockRange, header_gap: BlockRange) -> bool:
        return block_gap[0] > header_gap[1] or block_gap[1] < header_gap[0]

    @property
    def is_paused(self) -> bool:
        """
        Return ``True`` if the sync is currently paused, otherwise ``False``.
        """
        return self._pauser.is_paused

    def pause(self) -> None:
        """
        Pause the sync. Pause and resume are wasteful actions and should not happen too frequently.
        """
        self._pauser.pause()
        if self._body_syncer:
            # Pausing the syncer is valid at any point that the service is running but that might
            # hit a point where we are still resolving the gaps and have no body_syncer set up yet.
            self._body_syncer.get_manager().cancel()
        self.logger.debug2("BodyChainGapSyncer paused")

    def resume(self) -> None:
        """
        Resume the sync.
        """
        self._pauser.resume()
        self.logger.debug2("BodyChainGapSyncer resumed")

    async def run(self) -> None:
        """
        Run the sync indefinitely until it is cancelled externally.
        """
        while True:
            if self._pauser.is_paused:
                await self._pauser.await_resume()
            try:
                await self._setup_for_next_gap()
            except ValidationError:
                self.logger.debug(
                    "There are no gaps in the chain of blocks at this time. Sleeping for %ss",
                    self._idle_time
                )
                await asyncio.sleep(self._idle_time)
            else:
                await self.manager.run_service(self._body_syncer)


class NoActionableGap(BaseTrinityError):
    """
    Raised when no actionable gap of blocks is found.
    """
    pass


class HeaderLaunchpointSyncer(HeaderSyncerAPI):
    """
    Wraps a "real" header syncer, and drops headers on the floor, until triggered
    at a "launchpoint".

    Return the headers at the launchpoint, and then pass through all the headers
    subsequently found by the header syncer.

    Can be used by a body syncer to pause syncing until a header launchpoint is reached.
    """
    logger = get_logger('trinity.sync.beam.chain.HeaderLaunchpointSyncer')

    def __init__(self, passthrough: HeaderSyncerAPI) -> None:
        self._real_syncer = passthrough
        self._at_launchpoint = asyncio.Event()
        self._launchpoint_headers: Tuple[BlockHeaderAPI, ...] = None

    def set_launchpoint_headers(self, headers: Tuple[BlockHeaderAPI, ...]) -> None:
        """
        Identify the given headers as launchpoint headers. These will be returned first.

        Immediately after these launchpoint headers are returned, start consuming and
        passing through all headers from the wrapped header syncer.
        """
        self._launchpoint_headers = headers
        self._at_launchpoint.set()

    async def new_sync_headers(
            self,
            max_batch_size: int = None) -> AsyncIterator[Tuple[BlockHeaderAPI, ...]]:
        await self._at_launchpoint.wait()

        self.logger.info(
            "Choosing %s as launchpoint headers to sync from",
            [str(header) for header in self._launchpoint_headers],
        )
        yield self._launchpoint_headers

        async for headers in self._real_syncer.new_sync_headers(max_batch_size):
            yield headers

    def get_target_header_hash(self) -> Hash32:
        return self._real_syncer.get_target_header_hash()


class HeaderOnlyPersist(Service):
    """
    Store all headers returned by the header syncer, until the target is reached, then exit.
    """
    def __init__(self,
                 header_syncer: ETHHeaderChainSyncer,
                 db: BaseAsyncHeaderDB,
                 force_end_block_number: int = None,
                 launch_strategy: SyncLaunchStrategyAPI = None) -> None:
        self.logger = get_logger('trinity.sync.beam.chain.HeaderOnlyPersist')
        self._db = db
        self._header_syncer = header_syncer
        self._final_headers: Tuple[BlockHeaderAPI, ...] = None
        self._force_end_block_number = force_end_block_number
        self._launch_strategy = launch_strategy

    async def run(self) -> None:
        self.manager.run_daemon_task(self._persist_headers_if_tip_too_old)
        # run sync until cancelled
        await self.manager.wait_finished()

    def _is_header_eligible_to_beam_sync(self, header: BlockHeaderAPI) -> bool:
        time_gap = time.time() - header.timestamp
        estimated_max_lag_seconds = MAX_BEAM_SYNC_LAG * PREDICTED_BLOCK_TIME
        return time_gap < (estimated_max_lag_seconds * (1 - BEAM_PIVOT_BUFFER_FRACTION))

    async def _persist_headers_if_tip_too_old(self) -> None:
        tip = await self._db.coro_get_canonical_head()
        if self._is_header_eligible_to_beam_sync(tip):
            self._force_end_block_number = tip.block_number + 1
            self.logger.info("Tip is recent enough, syncing from last synced header at %s", tip)
        else:
            self.logger.warning("Tip %s is too far behind to Beam Sync, skipping ahead...", tip)

        async for persist_info in persist_headers(
                self.logger, self._db, self._header_syncer, self._exit_if_launchpoint):

            if len(persist_info.new_canon_headers):
                head = persist_info.new_canon_headers[-1]
            else:
                head = await self._db.coro_get_canonical_head()
            self.logger.info(
                "Imported %d headers in %0.2f seconds, new head: %s",
                len(persist_info.imported_headers),
                persist_info.elapsed_time,
                head,
            )

    async def _exit_if_launchpoint(self, headers: Sequence[BlockHeaderAPI]) -> bool:
        """
        Determine if the supplied headers have reached the end of headers-only persist.
        This might be in the form of a forced launchpoint, or because we caught up to
        our peer's target launchpoint.

        In the case that we have reached the launchpoint:

            - trigger service exit
            - persist the headers before the launchpoint
            - save the headers that triggered the launchpoint (retrievable via get_final_headers)

        :return: whether we have reached the launchpoint
        """
        ending_header_search = [
            header for header in headers if header.block_number == self._force_end_block_number
        ]

        if ending_header_search:
            # Force an early exit to beam sync
            self.logger.info(
                "Forced the beginning of Beam Sync at %s",
                ending_header_search[0],
            )
            persist_headers = tuple(
                h for h in headers
                if h.block_number < self._force_end_block_number
            )
            final_headers = tuple(
                h for h in headers
                if h.block_number >= self._force_end_block_number
            )
        else:
            target_hash = self._header_syncer.get_target_header_hash()
            if target_hash in (header.hash for header in headers):
                self.logger.info(
                    "Caught up to skeleton peer. Switching to beam mode at %s",
                    headers[-1],
                )

                # We have reached the header syncer's target
                # Only sync against the most recent header
                persist_headers, final_headers = tuple(headers[:-1]), tuple(headers[-1:])
            else:
                # We have not reached the header syncer's target, continue normally
                return False

        new_canon_headers, old_canon_headers = await self._db.coro_persist_header_chain(
            persist_headers)

        if persist_headers:
            self.logger.debug(
                "Final header import before launchpoint: %s..%s, "
                "old canon: %s..%s, new canon: %s..%s",
                persist_headers[0],
                persist_headers[-1],
                old_canon_headers[0] if len(old_canon_headers) else None,
                old_canon_headers[-1] if len(old_canon_headers) else None,
                new_canon_headers[0] if len(new_canon_headers) else None,
                new_canon_headers[-1] if len(new_canon_headers) else None,
            )
        else:
            self.logger.debug("Final header import before launchpoint: None")

        self._final_headers = final_headers
        self.manager.cancel()

        return True

    def get_final_headers(self) -> Tuple[BlockHeaderAPI, ...]:
        """
        Which header(s) triggered the launchpoint to switch out of header-only persist state.

        :raise ValidationError: if the syncer has not reached the launchpoint yet
        """
        if self._final_headers is None:
            raise ValidationError("Must not try to access final headers before it has been set")
        else:
            return self._final_headers


class BeamBlockImporter(BaseBlockImporter, Service):
    """
    Block Importer that emits DoStatelessBlockImport and waits on the event bus for a
    StatelessBlockImportDone to show that the import is complete.

    It independently runs other state preloads, like the accounts for the
    block transactions.
    """
    def __init__(
            self,
            chain: AsyncChainAPI,
            db: DatabaseAPI,
            state_getter: BeamDownloader,
            backfiller: BeamStateBackfill,
            event_bus: EndpointAPI,
            metrics_registry: MetricsRegistry) -> None:
        self.logger = get_logger('trinity.sync.beam.chain.BeamBlockImporter')
        self._chain = chain
        self._db = db
        self._state_downloader = state_getter
        self._backfiller = backfiller
        self.metrics_registry = metrics_registry

        self._blocks_imported = 0
        self._preloaded_account_state = 0
        self._preloaded_previewed_account_state = 0
        self._preloaded_account_time: float = 0
        self._preloaded_previewed_account_time: float = 0
        self._import_time: float = 0

        self._event_bus = event_bus

    async def import_block(
            self,
            block: BlockAPI) -> BlockImportResult:
        self.logger.debug(
            "Beam importing %s (%d txns, %s gas) ...",
            block.header,
            len(block.transactions),
            f'{block.header.gas_used:,d}',
        )

        wit_db = AsyncWitnessDB(self._db)
        try:
            wit_hashes = wit_db.get_witness_hashes(block.hash)
        except WitnessHashesUnavailable:
            self.metrics_registry.counter('trinity.sync/block_witness_hashes_missing').inc()
            self.logger.info("Missing witness for %s. Attempting to fetch during import", block)
            preferred_peer = None
            if self.manager.is_running:
                self.manager.run_task(
                    self._event_bus.request,
                    FetchBlockWitness(preferred_peer, block.hash, block.number),
                )
        else:
            block_witness_uncollected = self._state_downloader._get_unique_missing_hashes(
                wit_hashes)
            self.logger.debug(
                "Missing %d nodes out of %d from witness of block %s",
                len(block_witness_uncollected), len(wit_hashes), block)
            if block_witness_uncollected:
                self.metrics_registry.counter('trinity.sync/block_witness_incomplete').inc()
            else:
                self.metrics_registry.counter('trinity.sync/block_witness_complete').inc()

        parent_header = await self._chain.coro_get_block_header_by_hash(block.header.parent_hash)
        new_account_nodes, collection_time = await self._load_address_state(
            block.header,
            parent_header.state_root,
            block.transactions,
        )
        self._preloaded_account_state += new_account_nodes
        self._preloaded_account_time += collection_time

        import_timer = Timer()
        import_done = await self._event_bus.request(DoStatelessBlockImport(block))
        self._import_time += import_timer.elapsed

        if not import_done.completed:
            raise ValidationError("Block import was cancelled, probably a shutdown")
        if import_done.exception:
            raise ValidationError("Block import failed") from import_done.exception
        if import_done.block.hash != block.hash:
            raise ValidationError(f"Requsted {block} to be imported, but ran {import_done.block}")
        self._blocks_imported += 1
        self._log_stats()
        return import_done.result

    async def preview_transactions(
            self,
            header: BlockHeaderAPI,
            transactions: Tuple[SignedTransactionAPI, ...],
            parent_state_root: Hash32,
            lagging: bool = True) -> None:

        if not self.manager.is_running:
            # If the service is shutting down, we can ignore preview requests
            return

        self.manager.run_task(self._preview_address_load, header, parent_state_root, transactions)

        # This is a hack, so that preview executions can load ancestor block-hashes
        self._db[header.hash] = rlp.encode(header)

        # Always broadcast, to start previewing transactions that are further ahead in the block
        old_state_header = header.copy(state_root=parent_state_root)
        self._event_bus.broadcast_nowait(
            DoStatelessBlockPreview(old_state_header, transactions),
            FIRE_AND_FORGET_BROADCASTING
        )

        self._backfiller.set_root_hash(header, parent_state_root)

    async def _preview_address_load(
            self,
            header: BlockHeaderAPI,
            parent_state_root: Hash32,
            transactions: Tuple[SignedTransactionAPI, ...]) -> None:
        """
        Get account state for transaction addresses on a block being previewed in parallel.
        """
        new_account_nodes, collection_time = await self._load_address_state(
            header,
            parent_state_root,
            transactions,
            urgent=False,
        )
        self._preloaded_previewed_account_state += new_account_nodes
        self._preloaded_previewed_account_time += collection_time

    async def _load_address_state(
            self,
            header: BlockHeaderAPI,
            parent_state_root: Hash32,
            transactions: Tuple[SignedTransactionAPI, ...],
            urgent: bool = True) -> Tuple[int, float]:
        """
        Load all state needed to read transaction account status.
        """

        address_timer = Timer()
        num_accounts, new_account_nodes = await self._request_address_nodes(
            header,
            parent_state_root,
            transactions,
            urgent,
        )
        collection_time = address_timer.elapsed

        self.logger.debug(
            "Previewed %s state for %d addresses in %.2fs; got %d trie nodes; urgent? %r",
            header,
            num_accounts,
            collection_time,
            new_account_nodes,
            urgent,
        )

        return new_account_nodes, collection_time

    def _log_stats(self) -> None:
        stats = {
            "preload_nodes": self._preloaded_account_state,
            "preload_time": self._preloaded_account_time,
            "preload_preview_nodes": self._preloaded_previewed_account_state,
            "preload_preview_time": self._preloaded_previewed_account_time,
            "import_time": self._import_time,
        }
        if self._blocks_imported:
            mean_stats = {key: val / self._blocks_imported for key, val in stats.items()}
        else:
            mean_stats = None
        self.logger.debug(
            "Beam Download of %d blocks: "
            "%r, block_average: %r",
            self._blocks_imported,
            stats,
            mean_stats,
        )

    def _extract_relevant_accounts(
            self,
            header: BlockHeaderAPI,
            transactions: Tuple[SignedTransactionAPI, ...]) -> Set[Address]:

        senders = [transaction.sender for transaction in transactions]
        recipients = [transaction.to for transaction in transactions if transaction.to]
        return set(senders + recipients + [header.coinbase])

    async def _request_address_nodes(
            self,
            header: BlockHeaderAPI,
            parent_state_root: Hash32,
            transactions: Tuple[SignedTransactionAPI, ...],
            urgent: bool = True) -> Tuple[int, int]:
        """
        Request any missing trie nodes needed to read account state for the given transactions.

        :param urgent: are these addresses needed immediately? If False, they should they queue
            up behind the urgent trie nodes.
        """
        addresses = await asyncio.get_event_loop().run_in_executor(
            None,
            self._extract_relevant_accounts,
            header,
            transactions,
        )
        collected_nodes = await self._state_downloader.download_accounts(
            addresses,
            parent_state_root,
            header.block_number,
            urgent=urgent,
        )
        return len(addresses), collected_nodes

    async def run(self) -> None:
        await self.manager.wait_finished()


class MissingDataEventHandler(Service):
    """
    Listen to event bus requests for missing account, storage and bytecode.
    Request the data on demand, and reply when it is available.
    """

    def __init__(
            self,
            state_downloader: BeamDownloader,
            event_bus: EndpointAPI,
            metrics_registry: MetricsRegistry) -> None:
        self.logger = get_logger('trinity.sync.beam.chain.MissingDataEventHandler')
        self._state_downloader = state_downloader
        self._event_bus = event_bus
        self.metrics_registry = metrics_registry
        self._minimum_beam_block_number = 0

    @property
    def minimum_beam_block_number(self) -> int:
        return self._minimum_beam_block_number

    @minimum_beam_block_number.setter
    def minimum_beam_block_number(self, new_minimum: int) -> None:
        if self._minimum_beam_block_number != 0:
            self.logger.warning(
                "Tried to re-set the starting Beam Import block number from %d to %d."
                " This is highly unusual, and probably a bug."
                " Treating the higher number as the new minimum...",
                self._minimum_beam_block_number,
                new_minimum,
            )
            self._minimum_beam_block_number = max(new_minimum, self._minimum_beam_block_number)
        else:
            self._minimum_beam_block_number = new_minimum

    async def run(self) -> None:
        await self._launch_server()
        await self.manager.wait_finished()

    async def _launch_server(self) -> None:
        self.manager.run_daemon_task(self._provide_missing_account_tries)
        self.manager.run_daemon_task(self._provide_missing_bytecode)
        self.manager.run_daemon_task(self._provide_missing_storage)
        self.manager.run_daemon_task(self._fetch_missing_trie_nodes)

    async def _fetch_missing_trie_nodes(self) -> None:
        async for event in self._event_bus.stream(CollectMissingTrieNodes):
            # Right now this is triggered only when we get a NewBlock msg, but if that changes we
            # should consider skipping the request if the block is too old, like we do in
            # _provide_missing_account_tries().
            downloader = self._state_downloader
            num_nodes_collected = await downloader.ensure_nodes_present(
                event.node_hashes, event.block_number, event.urgent)
            missing = downloader._get_unique_missing_hashes(event.node_hashes)
            if missing:
                self.logger.debug(
                    "Failed to download %d trie nodes out of %d requested for block %d",
                    len(missing),
                    len(event.node_hashes),
                    event.block_number,
                )
            await self._event_bus.broadcast(
                MissingTrieNodesResult(num_nodes_collected),
                event.broadcast_config(),
            )

    async def _provide_missing_account_tries(self) -> None:
        async for event in self._event_bus.stream(CollectMissingAccount):
            # If a request is coming in from an import on a block that's too old, cancel it
            should_respond = (
                event.block_number > self.minimum_beam_block_number
                or (event.urgent and event.block_number == self.minimum_beam_block_number)
            )
            if should_respond:
                self.manager.run_task(self._hang_until_account_served, event)
            else:
                await self._event_bus.broadcast(
                    MissingAccountResult(is_retry_acceptable=False),
                    event.broadcast_config(),
                )

    async def _provide_missing_bytecode(self) -> None:
        async for event in self._event_bus.stream(CollectMissingBytecode):
            should_respond = (
                event.block_number > self.minimum_beam_block_number
                or (event.urgent and event.block_number == self.minimum_beam_block_number)
            )
            if should_respond:
                self.manager.run_task(self._hang_until_bytecode_served, event)
            else:
                await self._event_bus.broadcast(
                    MissingBytecodeResult(is_retry_acceptable=False),
                    event.broadcast_config(),
                )

    async def _provide_missing_storage(self) -> None:
        async for event in self._event_bus.stream(CollectMissingStorage):
            should_respond = (
                event.block_number > self.minimum_beam_block_number
                or (event.urgent and event.block_number == self.minimum_beam_block_number)
            )
            if should_respond:
                self.manager.run_task(self._hang_until_storage_served, event)
            else:
                await self._event_bus.broadcast(
                    MissingStorageResult(is_retry_acceptable=False),
                    event.broadcast_config(),
                )

    async def _hang_until_account_served(self, event: CollectMissingAccount) -> None:
        try:
            await asyncio.wait_for(
                self._serve_account(event),
                timeout=BLOCK_IMPORT_MISSING_STATE_TIMEOUT,
            )
        except asyncio.CancelledError:
            # Beam sync is shutting down, probably either because the node is closing, or
            #   a pivot is required. So communicate that import should stop.
            await self._event_bus.broadcast(
                MissingAccountResult(is_retry_acceptable=False),
                event.broadcast_config(),
            )
            raise
        except asyncio.TimeoutError:
            pass

    async def _hang_until_bytecode_served(self, event: CollectMissingBytecode) -> None:
        try:
            await asyncio.wait_for(
                self._serve_bytecode(event),
                timeout=BLOCK_IMPORT_MISSING_STATE_TIMEOUT,
            )
        except asyncio.CancelledError:
            await self._event_bus.broadcast(
                MissingBytecodeResult(is_retry_acceptable=False),
                event.broadcast_config(),
            )
            raise
        except asyncio.TimeoutError:
            pass

    async def _hang_until_storage_served(self, event: CollectMissingStorage) -> None:
        try:
            await asyncio.wait_for(
                self._serve_storage(event),
                timeout=BLOCK_IMPORT_MISSING_STATE_TIMEOUT,
            )
        except asyncio.CancelledError:
            await self._event_bus.broadcast(
                MissingStorageResult(is_retry_acceptable=False),
                event.broadcast_config(),
            )
            raise
        except asyncio.TimeoutError:
            pass

    async def _serve_account(self, event: CollectMissingAccount) -> None:
        _, num_nodes_collected = await self._state_downloader.download_account(
            event.address_hash,
            event.state_root_hash,
            event.block_number,
            event.urgent,
        )
        bonus_node = await self._state_downloader.ensure_nodes_present(
            {event.missing_node_hash},
            event.block_number,
            event.urgent,
        )
        await self._event_bus.broadcast(
            MissingAccountResult(num_nodes_collected + bonus_node),
            event.broadcast_config(),
        )

    async def _serve_bytecode(self, event: CollectMissingBytecode) -> None:
        await self._state_downloader.ensure_nodes_present(
            {event.bytecode_hash},
            event.block_number,
            event.urgent,
        )
        await self._event_bus.broadcast(MissingBytecodeResult(), event.broadcast_config())

    async def _serve_storage(self, event: CollectMissingStorage) -> None:
        num_nodes_collected = await self._state_downloader.download_storage(
            event.storage_key,
            event.storage_root_hash,
            event.account_address,
            event.block_number,
            event.urgent,
        )
        bonus_node = await self._state_downloader.ensure_nodes_present(
            {event.missing_node_hash},
            event.block_number,
            event.urgent,
        )
        await self._event_bus.broadcast(
            MissingStorageResult(num_nodes_collected + bonus_node),
            event.broadcast_config(),
        )
