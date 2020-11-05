import asyncio
import collections
import math
import pathlib
import random
from typing import (
    DefaultDict,
    Iterable,
    List,
    Tuple,
)

from async_service import (
    Service,
    background_trio_service,
)
from eth.abc import (
    BlockAPI,
    BlockHeaderAPI,
)
from eth_typing import Hash32
from eth_utils import (
    ExtendedDebugLogger,
    ValidationError,
    to_tuple,
)
from lahja import EndpointAPI
import trio

from p2p.abc import SessionAPI
from trinity.boot_info import BootInfo
from trinity.constants import TO_NETWORKING_BROADCAST_CONFIG
from trinity.db.manager import DBClient
from trinity.extensibility import TrioIsolatedComponent
from trinity.protocol.eth.events import NewBlockEvent
from trinity.protocol.eth.payloads import (
    BlockFields,
    NewBlockHash,
    NewBlockPayload,
)
from trinity.protocol.eth.peer import (
    ETHProxyPeerPool,
    ETHProxyPeer,
)
from trinity.protocol.wit.db import AsyncWitnessDB
from trinity.sync.common.events import CollectMissingTrieNodes, NewBlockImported, NewBlockMined
from trinity._utils.connect import get_eth1_chain_with_remote_db
from trinity._utils.logging import get_logger


class NewBlockComponent(TrioIsolatedComponent):
    """
    Propogate newly received and imported blocks to peers, according to devp2p rules.
    https://github.com/ethereum/devp2p/blob/master/caps/eth.md#block-propagation
    """
    name = "NewBlockComponent"

    @property
    def is_enabled(self) -> bool:
        return True

    async def do_run(self, event_bus: EndpointAPI) -> None:
        proxy_peer_pool = ETHProxyPeerPool(event_bus, TO_NETWORKING_BROADCAST_CONFIG)
        async with background_trio_service(proxy_peer_pool):
            service = NewBlockService(event_bus, proxy_peer_pool, self._boot_info)
            async with background_trio_service(service) as manager:
                await manager.wait_finished()


class NewBlockService(Service):

    logger = get_logger('trinity.components.new_block.NewBlockService')

    def __init__(self,
                 event_bus: EndpointAPI,
                 peer_pool: ETHProxyPeerPool,
                 boot_info: BootInfo) -> None:
        self._event_bus = event_bus
        self._peer_pool = peer_pool
        # TODO: old blocks need to be pruned to avoid unbounded growth of tracker
        self._peer_block_tracker: DefaultDict[bytes, List[str]] = collections.defaultdict(list)
        self._boot_info = boot_info

    async def run(self) -> None:
        self.manager.run_daemon_task(self._handle_imported_blocks)
        self.manager.run_daemon_task(self._handle_mined_blocks)

        async for event in self._event_bus.stream(NewBlockEvent):
            self.manager.run_task(self._handle_new_block, event.session, event.command.payload)

    async def _handle_mined_blocks(self) -> None:
        async for event in self._event_bus.stream(NewBlockMined):
            block = event.block
            self.logger.debug("NewBlockMined: %s", block)
            block_fields = BlockFields(block.header, block.transactions, block.uncles)
            await self._broadcast_new_block(block_fields, event.total_difficulty)

    async def _handle_imported_blocks(self) -> None:
        async for event in self._event_bus.stream(NewBlockImported):
            block = event.block
            self.logger.debug("NewBlockImported: %s", block)
            await self._broadcast_new_block_hashes(block)

    async def _handle_new_block(self, sender: SessionAPI, payload: NewBlockPayload) -> None:
        header = payload.block.header
        sender_peer = ETHProxyPeer.from_session(
            sender,
            self._event_bus,
            TO_NETWORKING_BROADCAST_CONFIG
        )
        sender_peer_str = str(sender_peer)

        # Add peer to tracker if we've seen this block before
        if header.hash in self._peer_block_tracker:
            if sender_peer_str not in self._peer_block_tracker[header.hash]:
                self._peer_block_tracker[header.hash].append(sender_peer_str)
        else:
            # Verify the validity of block, add to tracker and broadcast to eligible peers
            with get_eth1_chain_with_remote_db(self._boot_info, self._event_bus) as chain:
                try:
                    chain.validate_seal(header)
                except ValidationError as exc:
                    self.logger.info(
                        "Received invalid block from peer: %s. %s",
                        sender_peer_str, exc,
                    )
                else:
                    self.manager.run_task(self._fetch_witnesses, sender_peer, header)
                    self._peer_block_tracker[header.hash] = [sender_peer_str]
                    # Here we only broadcast a NewBlock msg to a subset of our peers, and once the
                    # block is imported into our chain a NewBlockImported event will be generated
                    # and we'll announce it to the remaining ones, as per the spec.
                    await self._broadcast_new_block(payload.block, payload.total_difficulty)

    async def _broadcast_new_block_hashes(self, block: BlockAPI) -> None:
        """
        Send `NewBlockHashes` msgs to all peers that haven't heard about the given block yet.
        """
        all_peers = await self._peer_pool.get_peers()
        eligible_peers = self._filter_eligible_peers(all_peers, block.hash)
        new_block_hash = NewBlockHash(hash=block.hash, number=block.number)
        for peer in eligible_peers:
            self.logger.debug("Sending NewBlockHashes(%s) to %s", block.header, peer)
            target_peer = await self._peer_pool.ensure_proxy_peer(peer.session)
            target_peer.eth_api.send_new_block_hashes((new_block_hash,))
            self._peer_block_tracker[block.hash].append(str(target_peer))
            # add checkpoint here to guarantee the event loop is released per iteration
            await trio.sleep(0)

    async def _broadcast_new_block(self, block_fields: BlockFields, total_difficulty: int) -> None:
        """
        Send `NewBlock` msgs to a subset of our peers.
        """
        all_peers = await self._peer_pool.get_peers()
        eligible_peers = self._filter_eligible_peers(all_peers, block_fields.header.hash)
        number_of_broadcasts = int(math.sqrt(len(all_peers)))
        sample_size = min(len(eligible_peers), number_of_broadcasts)
        broadcast_peers = random.sample(eligible_peers, sample_size)

        for peer in broadcast_peers:
            target_peer = await self._peer_pool.ensure_proxy_peer(peer.session)
            self.logger.debug("Sending NewBlock(%s) to %s", block_fields.header, target_peer)
            target_peer.eth_api.send_new_block(block_fields, total_difficulty)
            self._peer_block_tracker[block_fields.header.hash].append(str(target_peer))

    @to_tuple
    def _filter_eligible_peers(self,
                               all_peers: Tuple[ETHProxyPeer],
                               block_hash: bytes) -> Iterable[ETHProxyPeer]:
        """
        Filter and return peers who have not seen the given block hash.
        """
        for peer in all_peers:
            if str(peer) not in self._peer_block_tracker[block_hash]:
                yield peer

    async def _fetch_witnesses(self, peer: ETHProxyPeer, header: BlockHeaderAPI) -> None:
        await fetch_witnesses(
            peer, header, self._event_bus, self._boot_info.trinity_config.database_ipc_path,
            self.logger)


# XXX: Should probably be moved to trinity/sync/beam/ as we'll want to use this in the block
# previewer.
async def fetch_witnesses(
        peer: ETHProxyPeer,
        header: BlockHeaderAPI,
        event_bus: EndpointAPI,
        database_ipc_path: pathlib.Path,
        logger: ExtendedDebugLogger,
) -> Tuple[Hash32, ...]:
    """
    Fetch witness hashes for the given block from the given peer and emit a
    CollectMissingTrieNodes event to trigger the download of the trie nodes referred by them.
    """
    try:
        logger.debug(
            "Attempting to fetch witness hashes for block %s from %s", header.hash, peer)
        witness_hashes = await peer.wit_api.get_block_witness_hashes(header.hash)
    except asyncio.TimeoutError:
        logger.debug(
            "Timed out trying to fetch witnesses for block %s from %s", header.hash, peer)
        return tuple()
    except Exception as err:
        logger.warning(
            "Error fetching witnesses for block %s from %s: %s", header.hash, peer, err)
        return tuple()
    else:
        if witness_hashes:
            logger.debug(
                "Got witness hashes for block %s, asking BeamSyncer to fetch them", header.hash)
            await event_bus.broadcast(
                CollectMissingTrieNodes(
                    witness_hashes, urgent=True, block_number=header.block_number))
            base_db = DBClient.connect(database_ipc_path)
            with base_db:
                wit_db = AsyncWitnessDB(base_db)
                wit_db.persist_witness_hashes(header.hash, witness_hashes)
        else:
            logger.debug(
                "%s announced block %s but doesn't have witness hashes for it", peer, header.hash)
        return witness_hashes
