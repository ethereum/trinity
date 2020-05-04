import asyncio
from async_service import Service

from trinity.chains.base import AsyncChainAPI
from trinity.db.eth1.chain import BaseAsyncChainDB
from trinity.protocol.eth.peer import ETHPeerPool
from trinity.protocol.eth.sync import ETHHeaderChainSyncer
from trinity._utils.logging import get_logger
from trinity.sync.common.checkpoint import Checkpoint
from trinity.sync.common.headers import persist_headers
from trinity.sync.common.strategies import (
    FromCheckpointLaunchStrategy,
    FromGenesisLaunchStrategy,
    SyncLaunchStrategyAPI,
)


class HeaderChainSyncer(Service):
    def __init__(self,
                 chain: AsyncChainAPI,
                 db: BaseAsyncChainDB,
                 peer_pool: ETHPeerPool,
                 checkpoint: Checkpoint = None) -> None:
        self.logger = get_logger('trinity.sync.header.chain.HeaderChainSyncer')
        self._db = db
        self._checkpoint = checkpoint

        if checkpoint is None:
            self._launch_strategy: SyncLaunchStrategyAPI = FromGenesisLaunchStrategy(
                db,
                chain
            )
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

        self.manager.run_daemon_child_service(self._header_syncer)
        self.manager.run_daemon_task(self._persist_headers)
        # run sync until cancelled
        await self.manager.wait_finished()

    async def _persist_headers(self) -> None:
        await persist_headers(self.logger, self._db, self._header_syncer)
