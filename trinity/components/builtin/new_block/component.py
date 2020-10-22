import math
import random
from typing import (
    Dict,
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
from eth_utils import (
    ValidationError,
    to_tuple,
)
from lahja import EndpointAPI
import trio

from p2p.abc import SessionAPI
from trinity.boot_info import BootInfo
from trinity.constants import TO_NETWORKING_BROADCAST_CONFIG
from trinity.extensibility import TrioIsolatedComponent
from trinity.protocol.eth.events import NewBlockEvent
from trinity.protocol.eth.payloads import (
    NewBlockHash,
    NewBlockPayload,
)
from trinity.protocol.eth.peer import (
    ETHProxyPeerPool,
    ETHProxyPeer,
)
from trinity.sync.common.events import NewBlockImported
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
        # tracks which peers have seen a block
        # todo: old blocks need to be pruned to avoid unbounded growth of tracker
        self._peer_block_tracker: Dict[bytes, List[str]] = {}
        self._boot_info = boot_info

    async def run(self) -> None:
        self.manager.run_daemon_task(self._handle_imported_blocks)

        async for event in self._event_bus.stream(NewBlockEvent):
            self.manager.run_task(self._handle_new_block, event.session, event.command.payload)

    async def _handle_imported_blocks(self) -> None:
        async for event in self._event_bus.stream(NewBlockImported):
            await self._broadcast_imported_block(event.block)

    async def _handle_new_block(self, sender: SessionAPI, block: NewBlockPayload) -> None:
        header = block.block.header
        sender_peer_str = str(ETHProxyPeer.from_session(
            sender,
            self._event_bus,
            TO_NETWORKING_BROADCAST_CONFIG
        ))

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
                    self._peer_block_tracker[header.hash] = [sender_peer_str]
                    await self._broadcast_newly_seen_block(header)

    async def _broadcast_imported_block(self, block: BlockAPI) -> None:
        """
        Broadcast `NewBlockHashes` for newly imported block to eligible peers,
        aka those that haven't seen the block.
        """
        all_peers = await self._peer_pool.get_peers()
        if block.hash in self._peer_block_tracker:
            eligible_peers = self._filter_eligible_peers(all_peers, block.hash)
        else:
            self._peer_block_tracker[block.hash] = []
            eligible_peers = all_peers

        new_block_hash = NewBlockHash(hash=block.hash, number=block.number)
        for peer in eligible_peers:
            target_peer = await self._peer_pool.ensure_proxy_peer(peer.session)
            target_peer.eth_api.send_new_block_hashes((new_block_hash,))
            self._peer_block_tracker[block.hash].append(str(target_peer))
            # add checkpoint here to guarantee the event loop is released per iteration
            await trio.sleep(0)

    async def _broadcast_newly_seen_block(self, header: BlockHeaderAPI) -> None:
        """
        Broadcast `NewBlock` for newly received block to square root of
        total # of connected peers, aka those that haven't seen the block.
        """
        all_peers = await self._peer_pool.get_peers()
        eligible_peers = self._filter_eligible_peers(all_peers, header.hash)
        number_of_broadcasts = int(math.sqrt(len(all_peers)))
        sample_size = min(len(eligible_peers), number_of_broadcasts)
        broadcast_peers = random.sample(eligible_peers, sample_size)

        for peer in broadcast_peers:
            target_peer = await self._peer_pool.ensure_proxy_peer(peer.session)
            target_peer.eth_api.send_block_headers((header,))
            self._peer_block_tracker[header.hash].append(str(target_peer))

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
