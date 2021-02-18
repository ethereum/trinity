from typing import Any, FrozenSet, Type, Tuple, AsyncIterator

from eth.abc import BlockHeaderAPI
from eth.exceptions import HeaderNotFound
from async_service import Service
from eth_typing import BlockNumber, Hash32
from eth_utils import get_extended_debug_logger

from p2p.abc import CommandAPI
from p2p.commands import BaseCommand
from p2p.disconnect import DisconnectReason
from p2p.peer import PeerSubscriber
from p2p.peer_pool import BasePeerPool
from p2p import p2p_proto as p2p_commands

from trinity._utils.headers import sequence_builder
from trinity.db.eth1.header import BaseAsyncHeaderDB
from trinity.protocol.common.payloads import BlockHeadersQuery
from trinity.protocol.eth.peer import ETHPeer
from trinity.protocol.eth import commands as eth_commands


IGNORE_COMMANDS = {
    p2p_commands.Disconnect,
    p2p_commands.Ping,
    p2p_commands.Pong,
    eth_commands.Status,
    eth_commands.StatusV63,
    eth_commands.BlockHeadersV65,
}

GOSSIP_COMMANDS = {
    eth_commands.NewBlock,
    eth_commands.NewBlockHashes,
    eth_commands.Transactions,
    eth_commands.NewPooledTransactionHashes,
}

DISCONNECT_COMMANDS = {
    eth_commands.GetBlockBodiesV65,
    eth_commands.GetNodeDataV65,
    eth_commands.GetReceiptsV65,
}


class RequestHandler(Service, PeerSubscriber):
    def __init__(self, peer_pool: BasePeerPool, db: BaseAsyncHeaderDB) -> None:
        self.logger = get_extended_debug_logger('trinity.proxy.RequestHandler')
        self.peer_pool = peer_pool
        self.db = db

    subscription_msg_types: FrozenSet[Type[CommandAPI[Any]]] = frozenset({BaseCommand})

    msg_queue_maxsize = 1024

    async def run(self) -> None:
        self.logger.info("Starting....")
        try:
            self.peer_pool.subscribe(self)

            while self.manager.is_running:
                peer, command = await self.msg_queue.get()
                self.manager.run_task(self._handle_peer_message, peer, command)
                self.msg_queue.task_done()
        finally:
            self.peer_pool.unsubscribe(self)

    #
    # Message Handlers
    #
    async def _handle_peer_message(self, peer: ETHPeer, command: BaseCommand) -> None:
        cmd_type = type(command)
        if cmd_type in IGNORE_COMMANDS:
            self.logger.debug("Ignoring: peer=%s  command=%s", peer, cmd_type)
        elif cmd_type in GOSSIP_COMMANDS:
            self.logger.debug2("Gossip: peer=%s  command=%s", peer, cmd_type)
        elif cmd_type in DISCONNECT_COMMANDS:
            self.logger.info("Disconnecting: peer=%s  command=%s", peer, cmd_type)
            peer.disconnect_nowait(DisconnectReason.USELESS_PEER)
        elif cmd_type is eth_commands.GetBlockHeadersV65:
            self.logger.info("Proxying: peer=%s  command=%s", peer, cmd_type)
            await self._handle_get_block_headers(peer, command)
        elif cmd_type is eth_commands.GetPooledTransactionsV65:
            self.logger.info("Proxying: peer=%s  command=%s", peer, cmd_type)
            await self._handle_get_pooled_transactions(peer, command)
        else:
            self.logger.info("Unhandled: peer=%s  command=%s", peer, command)

    async def _handle_get_block_headers(self,
                                        peer: ETHPeer,
                                        command: eth_commands.GetBlockHeadersV65) -> None:
        headers = await self.lookup_headers(command.payload)
        peer.eth_api.send_block_headers(headers)

    async def _handle_get_pooled_transactions(self,
                                              peer: ETHPeer,
                                              command: eth_commands.GetPooledTransactionsV65,
                                              ) -> None:
        peer.eth_api.send_pooled_transactions(())

    #
    # Utils
    #
    async def lookup_headers(self,
                             query: BlockHeadersQuery) -> Tuple[BlockHeaderAPI, ...]:
        """
        Lookup :max_headers: headers starting at :block_number_or_hash:, skipping :skip: items
        between each, in reverse order if :reverse: is True.
        """
        try:
            block_numbers = await self._get_block_numbers_for_query(query)
        except HeaderNotFound:
            self.logger.debug(
                "Peer requested starting header %r that is unavailable, returning nothing",
                query.block_number_or_hash)
            return tuple()

        headers: Tuple[BlockHeaderAPI, ...] = tuple([
            header
            async for header
            in self._generate_available_headers(block_numbers)
        ])
        return headers

    async def _get_block_numbers_for_query(self,
                                           query: BlockHeadersQuery,
                                           ) -> Tuple[BlockNumber, ...]:
        """
        Generate the block numbers for a given `HeaderRequest`.
        """
        if isinstance(query.block_number_or_hash, bytes):
            header = await self.db.coro_get_block_header_by_hash(Hash32(query.block_number_or_hash))
            start_number = header.block_number
        elif isinstance(query.block_number_or_hash, int):
            start_number = query.block_number_or_hash
        else:
            actual_type = type(query.block_number_or_hash)
            raise TypeError(f"Invariant: unexpected type for 'block_number_or_hash': {actual_type}")

        return sequence_builder(
            start_number=start_number,
            max_length=query.max_headers,
            skip=query.skip,
            reverse=query.reverse,
        )

    async def _generate_available_headers(
            self, block_numbers: Tuple[BlockNumber, ...]) -> AsyncIterator[BlockHeaderAPI]:
        """
        Generates the headers requested, halting on the first header that is not locally available.
        """
        for count, block_num in enumerate(block_numbers):
            try:
                yield await self.db.coro_get_canonical_block_header_by_number(block_num)
            except HeaderNotFound:
                self.logger.debug(
                    "Peer requested header number %s that is unavailable, stopping search.",
                    block_num,
                )
                break

            if count >= 10:
                break
