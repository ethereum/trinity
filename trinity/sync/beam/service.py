import asyncio
from typing import (
    List,
    Tuple,
    cast,
)

from async_service import Service
from cached_property import cached_property
from lahja import EndpointAPI
from pyformance import MetricsRegistry

from eth_typing import BlockNumber, Hash32

from eth.abc import AtomicDatabaseAPI, DatabaseAPI

from eth_utils import ExtendedDebugLogger, humanize_hash

from trinity.chains.base import AsyncChainAPI
from trinity.components.builtin.metrics.registry import NoopMetricsRegistry
from trinity.components.builtin.metrics.sync_metrics_registry import SyncMetricsRegistry
from trinity.db.eth1.chain import BaseAsyncChainDB
from trinity.exceptions import WitnessHashesUnavailable
from trinity.protocol.eth.peer import ETHPeerPool, ETHPeer
from trinity.protocol.wit.db import AsyncWitnessDB
from trinity.sync.beam.constants import (
    MAX_BEAM_SYNC_LAG,
    PREDICTED_BLOCK_TIME,
)
from trinity.sync.common.checkpoint import Checkpoint
from trinity.sync.common.events import (
    BlockWitnessResult,
    CollectMissingTrieNodes,
    FetchBlockWitness,
)
from trinity._utils.logging import get_logger

from .chain import BeamSyncer


class BeamSyncService(Service):

    def __init__(
            self,
            chain: AsyncChainAPI,
            chaindb: BaseAsyncChainDB,
            base_db: AtomicDatabaseAPI,
            peer_pool: ETHPeerPool,
            event_bus: EndpointAPI,
            checkpoint: Checkpoint = None,
            force_beam_block_number: BlockNumber = None,
            enable_header_backfill: bool = False,
            sync_metrics_registry: SyncMetricsRegistry = None) -> None:
        self.logger = get_logger('trinity.sync.beam.service.BeamSyncService')
        self.chain = chain
        self.chaindb = chaindb
        self.base_db = base_db
        self.peer_pool = peer_pool
        self.event_bus = event_bus
        self.checkpoint = checkpoint
        self.force_beam_block_number = force_beam_block_number
        self.enable_header_backfill = enable_header_backfill
        self.sync_metrics_registry = sync_metrics_registry

    @cached_property
    def metrics_registry(self) -> MetricsRegistry:
        if self.sync_metrics_registry:
            return self.sync_metrics_registry.metrics_service.registry
        else:
            return NoopMetricsRegistry()

    async def run(self) -> None:
        # Run this first so that we start fetching witnesses ASAP.
        self.manager.run_daemon_task(self._witness_fetcher)

        head = await self.chaindb.coro_get_canonical_head()

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
        while self.manager.is_running:
            beam_syncer = BeamSyncer(
                self.chain,
                self.base_db,
                self.chaindb,
                self.peer_pool,
                self.event_bus,
                self.metrics_registry,
                self.checkpoint,
                self.force_beam_block_number,
                self.enable_header_backfill,
            )
            self.manager.run_child_service(beam_syncer)
            do_pivot = await self._monitor_for_pivot(beam_syncer)
            if do_pivot:
                self.logger.info("Pivoting Beam Sync to a newer header...")
                if self.sync_metrics_registry:
                    latest_block = beam_syncer._body_syncer._latest_block_number
                    await self.sync_metrics_registry.record_pivot(latest_block)
            else:
                self.logger.info("No pivot requested. Leaving Beam Syncer closed...")
                break

    async def _monitor_for_pivot(self, beam_syncer: BeamSyncer) -> bool:
        """
        :return: True if Beam Sync should be restarted on exit
        """
        while self.manager.is_running:
            await asyncio.sleep(PREDICTED_BLOCK_TIME)
            if not beam_syncer.get_manager().is_running:
                # If the syncer exits normally, do not pivot
                return False
            else:
                lag = beam_syncer.get_block_count_lag()
                if self.sync_metrics_registry:
                    self.sync_metrics_registry.record_lag(lag)
                if lag > MAX_BEAM_SYNC_LAG:
                    self.logger.warning(
                        "Beam Sync is lagging by %d blocks. Pivoting...",
                        lag,
                    )
                    beam_syncer.get_manager().cancel()
                    return True
                else:
                    if lag >= MAX_BEAM_SYNC_LAG * 0.8:
                        # Start showing the lag in info, if lagging behind a lot
                        logger = self.logger.info
                    else:
                        logger = self.logger.debug
                    logger(
                        (
                            "Beam Sync is lagging behind the latest known header by %d blocks,"
                            " will pivot above %d"
                        ),
                        lag,
                        MAX_BEAM_SYNC_LAG,
                    )

                    # Keep monitoring
                    continue

        # If the service exits, do not pivot
        return False

    async def _witness_fetcher(self) -> None:
        async for event in self.event_bus.stream(FetchBlockWitness):
            # FIXME: We regularly get NewBlockHashes msgs for the same blocks from multiple peers,
            # almost simultaneously, so we end up running concurrent instances of the task below
            # trying to fetch the same witness hashes, which is wasteful.
            self.manager.run_task(self._ensure_witness, event)

    async def _ensure_witness(self, event: FetchBlockWitness) -> None:
        block_hash = event.hash
        block_number = event.number
        block_str = f"Block #{block_number}-0x{humanize_hash(block_hash)}"
        self.logger.debug("Attempting to fetch witness for %s", block_str)
        try:
            existing_wit = AsyncWitnessDB(self.base_db).get_witness_hashes(block_hash)
        except WitnessHashesUnavailable:
            pass
        else:
            self.logger.debug(
                "Already have witness hashes for %s, not fetching again", block_str)
            await self.event_bus.broadcast(
                BlockWitnessResult(existing_wit), event.broadcast_config())
            return

        witness_hashes: Tuple[Hash32, ...] = tuple()
        if event.preferred_peer is not None:
            try:
                preferred_peer = cast(ETHPeer, self.peer_pool.connected_nodes[event.preferred_peer])
            except KeyError:
                # This means the peer has disconnected since we fired the FetchBlockWitness event.
                pass
            else:
                if hasattr(preferred_peer, 'wit_api'):
                    witness_hashes = await _fetch_witness(
                        preferred_peer, block_hash, block_number, self.event_bus, self.base_db,
                        self.metrics_registry, self.logger)
                else:
                    self.logger.debug(
                        "%s does not support the wit protocol, can't fetch witness for %s",
                        preferred_peer, block_str)
        else:
            queried_peers: List[ETHPeer] = []
            while not witness_hashes:
                pending_peers = [
                    peer for peer in self.peer_pool.connected_nodes.values()
                    if hasattr(peer, 'wit_api') and peer not in queried_peers
                ]
                if not pending_peers:
                    self.logger.debug("None of our peers have witness hashes for %s", block_str)
                    break
                peer = cast(ETHPeer, pending_peers[0])
                queried_peers.append(peer)
                witness_hashes = await _fetch_witness(
                    peer, block_hash, block_number, self.event_bus, self.base_db,
                    self.metrics_registry, self.logger)

        await self.event_bus.broadcast(
            BlockWitnessResult(witness_hashes), event.broadcast_config())


async def _fetch_witness(
        peer: ETHPeer,
        block_hash: Hash32,
        block_number: BlockNumber,
        event_bus: EndpointAPI,
        db: DatabaseAPI,
        metrics_registry: MetricsRegistry,
        logger: ExtendedDebugLogger,
) -> Tuple[Hash32, ...]:
    """
    Fetch witness hashes for the given block from the given peer, emit a CollectMissingTrieNodes
    event to trigger the download of the trie nodes referred by them and wait for the missing
    trie nodes to arrive.

    Returns the trie node hashes for the block witness, or an empty tuple if we cannot fetch them.
    """
    block_str = f"<Block #{block_number}-0x{humanize_hash(block_hash)}>"
    try:
        logger.debug("Asking %s for witness hashes for %s", peer, block_str)
        witness_hashes = await peer.wit_api.get_block_witness_hashes(block_hash)
    except asyncio.TimeoutError:
        logger.debug(
            "Timed out trying to fetch witness hashes for %s from %s", block_str, peer)
        return tuple()
    except Exception as err:
        logger.warning(
            "Error fetching witness hashes for %s from %s: %s", block_str, peer, err)
        return tuple()
    else:
        if witness_hashes:
            logger.debug(
                "Got witness hashes for %s, asking BeamSyncer to fetch trie nodes", block_str)
            # XXX: Consider using urgent=False if the new block is more than a couple blocks ahead
            # of our tip, as otherwise when beam sync start to falls behind it may be more
            # difficult to catch up.
            urgent = True
            try:
                # These events are handled by BeamSyncer, which gets restarted whenever we pivot,
                # so we sometimes have to wait a bit before we can fire those events. And we use
                # a long timeout because we want to be sure we fetch the witness once we have the
                # node hashes for it.
                await asyncio.wait_for(
                    event_bus.wait_until_any_endpoint_subscribed_to(CollectMissingTrieNodes),
                    timeout=5,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "No subscribers for CollectMissingTrieNodes, cannot fetch witness for %s",
                    block_str,
                )
                return witness_hashes
            wit_db = AsyncWitnessDB(db)
            wit_db.persist_witness_hashes(block_hash, witness_hashes)
            result = await event_bus.request(
                CollectMissingTrieNodes(witness_hashes, urgent, block_number))
            logger.debug(
                "Collected %d missing trie nodes from %s witness",
                result.num_nodes_collected,
                block_str,
            )
        else:
            logger.debug("Got empty witness hashes for %s from %s", block_str, peer)
        return witness_hashes
