from async_service import Service

from trinity.chains.base import AsyncChainAPI
from trinity.db.eth1.header import BaseAsyncHeaderDB
from trinity.protocol.les.peer import LESPeerPool
from trinity.protocol.les.sync import LightHeaderChainSyncer
from trinity._utils.timer import Timer
from trinity._utils.logging import get_logger


class LightChainSyncer(Service):
    def __init__(self, chain: AsyncChainAPI, db: BaseAsyncHeaderDB, peer_pool: LESPeerPool) -> None:
        self.logger = get_logger('trinity.sync.light.chain.LightChainSyncer')
        self._db = db
        self._header_syncer = LightHeaderChainSyncer(chain, db, peer_pool)

    async def run(self) -> None:
        head = await self._db.coro_get_canonical_head()
        self.logger.info("Starting light sync; current head: %s", head)

        self.manager.run_daemon_child_service(self._header_syncer)
        self.manager.run_daemon_task(self._persist_headers)
        # run sync until cancelled
        await self.manager.wait_finished()

    async def _persist_headers(self) -> None:
        async for headers in self._header_syncer.new_sync_headers():

            self._header_syncer._chain.validate_chain_extension(headers)

            timer = Timer()
            await self._db.coro_persist_header_chain(headers)

            head = await self._db.coro_get_canonical_head()
            self.logger.info(
                "Imported %d headers in %0.2f seconds, new head: %s",
                len(headers), timer.elapsed, head)
