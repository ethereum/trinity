from eth_utils import get_extended_debug_logger
from p2p.service import Service

from trinity.chains.base import AsyncChainAPI
from trinity.db.eth1.header import BaseAsyncHeaderDB
from trinity.protocol.les.peer import LESPeerPool
from trinity.protocol.les.sync import LightHeaderChainSyncer
from trinity._utils.timer import Timer


class LightChainSyncer(Service):
    logger = get_extended_debug_logger('trinity.sync.light.ChainSyncer')

    def __init__(self,
                 chain: AsyncChainAPI,
                 db: BaseAsyncHeaderDB,
                 peer_pool: LESPeerPool) -> None:
        self._db = db
        self._header_syncer = LightHeaderChainSyncer(chain, db, peer_pool)

    async def run(self) -> None:
        self.manager.run_daemon_child_service(self._header_syncer)
        self.manager.run_daemon_task(self._persist_headers)
        # run sync until cancelled
        await self.manager.wait_forever()

    async def _persist_headers(self) -> None:
        async for headers in self._header_syncer.new_sync_headers():
            timer = Timer()
            await self._db.coro_persist_header_chain(headers)

            head = await self._db.coro_get_canonical_head()
            self.logger.info(
                "Imported %d headers in %0.2f seconds, new head: %s",
                len(headers), timer.elapsed, head)
