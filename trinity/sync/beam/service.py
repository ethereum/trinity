from lahja import EndpointAPI

from eth_typing import BlockNumber
from eth_utils import get_extended_debug_logger

from eth.abc import AtomicDatabaseAPI

from p2p.service import Service

from trinity.chains.base import AsyncChainAPI
from trinity.db.eth1.chain import BaseAsyncChainDB
from trinity.protocol.eth.peer import ETHPeerPool
from trinity.sync.common.checkpoint import Checkpoint

from .chain import BeamSyncer


class BeamSyncService(Service):
    logger = get_extended_debug_logger('trinity.sync.beam.BeamSync')

    def __init__(
            self,
            chain: AsyncChainAPI,
            chaindb: BaseAsyncChainDB,
            base_db: AtomicDatabaseAPI,
            peer_pool: ETHPeerPool,
            event_bus: EndpointAPI,
            checkpoint: Checkpoint = None,
            force_beam_block_number: BlockNumber = None) -> None:
        self.chain = chain
        self.chaindb = chaindb
        self.base_db = base_db
        self.peer_pool = peer_pool
        self.event_bus = event_bus
        self.checkpoint = checkpoint
        self.force_beam_block_number = force_beam_block_number

    async def run(self) -> None:
        head = await self.chaindb.coro_get_canonical_head()

        if self.checkpoint is not None:
            self.logger.info(
                "Initializing beam-sync; current head: %s, using checkpoint: %s",
                head,
                self.checkpoint,
            )
        else:
            self.logger.info("Initializing beam-sync; current head: %s", head)

        beam_syncer = BeamSyncer(
            self.chain,
            self.base_db,
            self.chaindb,
            self.peer_pool,
            self.event_bus,
            self.checkpoint,
            self.force_beam_block_number,
        )
        manager = self.manager.run_child_service(beam_syncer)
        await manager.wait_stopped()
