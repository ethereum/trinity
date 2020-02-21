from lahja import EndpointAPI
from cancel_token import CancelToken

from eth_typing import BlockNumber

from eth.abc import AtomicDatabaseAPI

from p2p.service import BaseService

from trinity.chains.base import AsyncChainAPI
from trinity.db.eth1.chain import BaseAsyncChainDB
from trinity.protocol.eth.peer import ETHPeerPool
from trinity.sync.beam.constants import (
    ESTIMATED_BEAMABLE_BLOCKS,
    PREDICTED_BLOCK_TIME,
)
from trinity.sync.common.checkpoint import Checkpoint

from .chain import BeamSyncer


class BeamSyncService(BaseService):

    def __init__(
            self,
            chain: AsyncChainAPI,
            chaindb: BaseAsyncChainDB,
            base_db: AtomicDatabaseAPI,
            peer_pool: ETHPeerPool,
            event_bus: EndpointAPI,
            checkpoint: Checkpoint = None,
            force_beam_block_number: BlockNumber = None,
            token: CancelToken = None) -> None:
        super().__init__(token)
        self.chain = chain
        self.chaindb = chaindb
        self.base_db = base_db
        self.peer_pool = peer_pool
        self.event_bus = event_bus
        self.checkpoint = checkpoint
        self.force_beam_block_number = force_beam_block_number

    async def _run(self) -> None:
        head = await self.wait(self.chaindb.coro_get_canonical_head())

        if self.checkpoint is not None:
            self.logger.info(
                "Initializing beam-sync; current head: %s, using checkpoint: %s",
                head,
                self.checkpoint,
            )
        else:
            self.logger.info("Initializing beam-sync; current head: %s", head)

        await self._pivot_loop()

    async def _pivot_loop(self) -> None:
        while self.is_operational:
            beam_syncer = BeamSyncer(
                self.chain,
                self.base_db,
                self.chaindb,
                self.peer_pool,
                self.event_bus,
                self.checkpoint,
                self.force_beam_block_number,
                token=self.cancel_token,
            )
            self.run_child_service(beam_syncer)
            do_pivot = await self._monitor_for_pivot(beam_syncer)
            if do_pivot:
                self.logger.info("Pivoting Beam Sync to a newer header...")
            else:
                self.logger.info("No pivot requested. Leaving Beam Syncer closed...")
                break

    async def _monitor_for_pivot(self, beam_syncer: BeamSyncer) -> bool:
        """
        :return: True if Beam Sync should be restarted on exit
        """
        while self.is_operational:
            await self.sleep(PREDICTED_BLOCK_TIME)
            if not beam_syncer.is_operational:
                # If the syncer exits normally, do not pivot
                return False
            else:
                lag = beam_syncer.get_block_count_lag()
                if lag > ESTIMATED_BEAMABLE_BLOCKS:
                    self.logger.warning(
                        "Beam Sync is lagging by %d blocks. Pivoting...",
                        lag,
                    )
                    beam_syncer.cancel_nowait()
                    return True
                else:
                    if lag >= ESTIMATED_BEAMABLE_BLOCKS * 0.8:
                        # Start showing the lag in info, if lagging behind a lot
                        logger = self.logger.info
                    else:
                        logger = self.logger.debug
                    logger("Beam Sync is lagging behind the latest known header by %d blocks", lag)

                    # Keep monitoring
                    continue

        # If the service exits, do not pivot
        return False
