import asyncio
from typing import Sequence

from async_service import Service, background_asyncio_service
from eth.abc import BlockHeaderAPI
from eth.exceptions import CheckpointsMustBeCanonical
from eth_typing import BlockNumber

from trinity._utils.pauser import Pauser
from trinity.chains.base import AsyncChainAPI
from trinity.db.eth1.chain import BaseAsyncChainDB
from trinity.protocol.eth.peer import ETHPeerPool
from trinity.protocol.eth.sync import ETHHeaderChainSyncer
from trinity._utils.logging import get_logger
from trinity.sync.common.checkpoint import Checkpoint
from trinity.sync.common.constants import (
    MAX_BACKFILL_HEADERS_AT_ONCE,
    MAX_SKELETON_REORG_DEPTH,
)
from trinity.sync.common.headers import persist_headers
from trinity.sync.common.strategies import (
    FromCheckpointLaunchStrategy,
    FromGenesisLaunchStrategy,
    FromBlockNumberLaunchStrategy,
    SyncLaunchStrategyAPI,
)


class HeaderChainSyncer(Service):
    def __init__(self,
                 chain: AsyncChainAPI,
                 db: BaseAsyncChainDB,
                 peer_pool: ETHPeerPool,
                 enable_backfill: bool = True,
                 checkpoint: Checkpoint = None) -> None:
        self.logger = get_logger('trinity.sync.header.chain.HeaderChainSyncer')
        self._db = db
        self._checkpoint = checkpoint
        self._enable_backfill = enable_backfill
        self._chain = chain
        self._peer_pool = peer_pool

        if checkpoint is None:
            self._launch_strategy: SyncLaunchStrategyAPI = FromGenesisLaunchStrategy(db)
        else:
            self._launch_strategy = FromCheckpointLaunchStrategy(
                db,
                chain,
                checkpoint,
                peer_pool,
            )

        self._header_syncer = ETHHeaderChainSyncer(chain, db, peer_pool, self._launch_strategy)

    async def run(self) -> None:
        head = await self._db.coro_get_canonical_head()

        if self._checkpoint is not None:
            self.logger.info(
                "Initializing header-sync; current head: %s, using checkpoint: %s",
                head,
                self._checkpoint,
            )
        else:
            self.logger.info("Initializing header-sync; current head: %s", head)

        try:
            await self._launch_strategy.fulfill_prerequisites()
        except asyncio.TimeoutError as exc:
            self.logger.exception(
                "Timed out while trying to fulfill prerequisites of "
                f"sync launch strategy: {exc} from {self._launch_strategy}"
            )
            self.manager.cancel()
            return

        # Because checkpoints are only set at startup (for now): once all gaps are filled, no new
        # ones will be created. So we can simply run this service till completion and then exit.
        if self._enable_backfill:
            backfiller = SequentialHeaderChainGapSyncer(self._chain, self._db, self._peer_pool)
            self.manager.run_child_service(backfiller)

        self.manager.run_daemon_child_service(self._header_syncer)
        self.manager.run_daemon_task(self._persist_headers)
        # run sync until cancelled
        await self.manager.wait_finished()

    async def _persist_headers(self) -> None:
        async for persist_info in persist_headers(self.logger, self._db, self._header_syncer):

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


class HeaderChainGapSyncer(Service):
    def __init__(self,
                 chain: AsyncChainAPI,
                 db: BaseAsyncChainDB,
                 peer_pool: ETHPeerPool,
                 max_headers: int = None) -> None:

        self.logger = get_logger('trinity.sync.header.chain.HeaderChainGapSyncer')
        self._chain = chain
        self._db = db
        self._peer_pool = peer_pool
        self._max_headers = max_headers

    async def run(self) -> None:

        available_gaps, _ = self._db.get_header_chain_gaps()
        if len(available_gaps):
            gap = available_gaps[0]
        else:
            self.logger.debug("No gaps to fill. Stopping")
            return

        launch_block_number = BlockNumber(max(0, gap[0] - MAX_SKELETON_REORG_DEPTH))
        self.logger.info(f"Launching from %s", launch_block_number)
        launch_strategy = FromBlockNumberLaunchStrategy(self._db, launch_block_number)

        gap_length = gap[1] - gap[0]
        if self._max_headers and gap_length > self._max_headers:
            final_block_number = BlockNumber(gap[0] + self._max_headers)
        else:
            final_block_number = gap[1]

        self._header_syncer = ETHHeaderChainSyncer(
            self._chain, self._db, self._peer_pool, launch_strategy)

        await launch_strategy.fulfill_prerequisites()
        self.logger.info(
            "Initializing gap-fill header-sync; filling gap: %s", (gap[0], final_block_number)
        )

        self.manager.run_child_service(self._header_syncer)
        self.manager.run_task(self._persist_headers, final_block_number)
        # run sync until cancelled
        await self.manager.wait_finished()

    async def _persist_headers(self, gap_end: BlockNumber) -> None:

        async def _is_at_end_of_gap(headers: Sequence[BlockHeaderAPI]) -> bool:
            all_headers_too_advanced = headers[0].block_number > gap_end
            if all_headers_too_advanced:
                self.manager.cancel()
                return True
            else:
                return False

        try:
            async for persist_info in persist_headers(
                    self.logger, self._db, self._header_syncer, _is_at_end_of_gap):
                self.logger.info(
                    "Imported %d gap headers from %s to %s in %0.2f seconds,",
                    len(persist_info.imported_headers),
                    persist_info.imported_headers[0],
                    persist_info.imported_headers[-1],
                    persist_info.elapsed_time,
                )
        except CheckpointsMustBeCanonical as err:
            self.logger.warning("Attempted to fill gap with invalid header: %s", err)
            self.manager.cancel()


class SequentialHeaderChainGapSyncer(Service):
    def __init__(self,
                 chain: AsyncChainAPI,
                 db: BaseAsyncChainDB,
                 peer_pool: ETHPeerPool) -> None:

        self.logger = get_logger('trinity.sync.header.chain.SequentialHeaderChainGapSyncer')
        self._chain = chain
        self._db = db
        self._peer_pool = peer_pool
        self._pauser = Pauser()
        self._max_backfill_header_at_once = MAX_BACKFILL_HEADERS_AT_ONCE

    def pause(self) -> None:
        """
        Pause the sync after the current operation has finished.
        """
        # We just switch the toggle but let the sync finish the current segment. It will wait for
        # the resume call before it starts a new segment.
        self._pauser.pause()
        self.logger.debug2(
            "Pausing SequentialHeaderChainGapSyncer after current operation finishs"
        )

    def resume(self) -> None:
        """
        Resume the sync.
        """
        self._pauser.resume()
        self.logger.debug2("SequentialHeaderChainGapSyncer resumed")

    async def run(self) -> None:
        while self.manager.is_running:
            if self._pauser.is_paused:
                await self._pauser.await_resume()

            gaps, _ = self._db.get_header_chain_gaps()
            if len(gaps) < 1:
                self.logger.info("No more gaps to fill. Exiting")
                self.manager.cancel()
                return
            else:
                self.logger.debug(f"Starting gap sync at {gaps[0]}")
                syncer = HeaderChainGapSyncer(
                    self._chain,
                    self._db,
                    self._peer_pool,
                    max_headers=self._max_backfill_header_at_once,
                )
            async with background_asyncio_service(syncer) as manager:
                await manager.wait_finished()
