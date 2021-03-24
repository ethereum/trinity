from typing import (
    Sequence,
    Tuple,
)

from eth.abc import (
    BlockAPI,
    BlockHeaderAPI,
    ReceiptAPI,
    SignedTransactionAPI,
)
from eth_typing import (
    BlockIdentifier,
    Hash32,
)
from lahja import (
    BroadcastConfig,
    EndpointAPI,
)

from p2p.abc import SessionAPI

from trinity._utils.errors import SupportsError
from trinity._utils.logging import get_logger
from trinity.protocol.common.typing import (
    BlockBodyBundles,
    NodeDataBundles,
    ReceiptsBundles,
)
from trinity.rlp.block_body import BlockBody
from trinity.rlp.sedes import (
    UninterpretedTransaction,
    deinterpret_receipt_bundles,
)

from .commands import (
    BlockBodiesV65,
    BlockHeadersV65,
    NewBlock,
    NewBlockHashes,
    NodeDataV65,
    ReceiptsV65,
    Transactions,
    PooledTransactionsV65,
)
from .events import (
    GetBlockBodiesRequest,
    GetBlockHeadersRequest,
    GetNodeDataRequest,
    GetReceiptsRequest,
    SendBlockBodiesEvent,
    SendBlockHeadersEvent,
    SendNewBlockEvent,
    SendNewBlockHashesEvent,
    SendNodeDataEvent,
    SendReceiptsEvent,
    SendTransactionsEvent,
    GetPooledTransactionsRequest,
    SendPooledTransactionsEvent,
)
from .payloads import BlockFields, NewBlockHash, NewBlockPayload


class ProxyETHAPI:
    """
    An ``ETHAPI`` that can be used outside of the process that runs the peer pool. Any
    action performed on this class is delegated to the process that runs the peer pool.
    """
    logger = get_logger('trinity.protocol.eth.proxy.ProxyETHAPI')

    def __init__(self,
                 session: SessionAPI,
                 event_bus: EndpointAPI,
                 broadcast_config: BroadcastConfig):
        self.session = session
        self._event_bus = event_bus
        self._broadcast_config = broadcast_config

    def raise_if_needed(self, value: SupportsError) -> None:
        if value.error is not None:
            self.logger.warning(
                "Raised %s while fetching from peer %s", value.error, self.session,
            )
            raise value.error

    async def get_block_headers(self,
                                block_number_or_hash: BlockIdentifier,
                                max_headers: int = None,
                                skip: int = 0,
                                reverse: bool = True,
                                timeout: float = None) -> Tuple[BlockHeaderAPI, ...]:

        response = await self._event_bus.request(
            GetBlockHeadersRequest(
                self.session,
                block_number_or_hash,
                max_headers,
                skip,
                reverse,
                timeout,
            ),
            self._broadcast_config
        )

        self.raise_if_needed(response)

        self.logger.debug2(
            "ProxyETHExchangeHandler returning %s block headers from %s",
            len(response.headers),
            self.session
        )

        return tuple(response.headers)

    async def get_block_bodies(self,
                               headers: Sequence[BlockHeaderAPI],
                               timeout: float = None) -> BlockBodyBundles:

        response = await self._event_bus.request(
            GetBlockBodiesRequest(
                self.session,
                headers,
                timeout,
            ),
            self._broadcast_config
        )

        self.raise_if_needed(response)

        self.logger.debug2(
            "ProxyETHExchangeHandler returning %s block bodies from %s",
            len(response.bundles),
            self.session
        )

        return response.bundles

    async def get_node_data(self,
                            node_hashes: Sequence[Hash32],
                            timeout: float = None) -> NodeDataBundles:

        response = await self._event_bus.request(
            GetNodeDataRequest(
                self.session,
                node_hashes,
                timeout,
            ),
            self._broadcast_config
        )

        self.raise_if_needed(response)

        self.logger.debug2(
            "ProxyETHExchangeHandler returning %s node bundles from %s",
            len(response.bundles),
            self.session
        )

        return response.bundles

    async def get_receipts(self,
                           headers: Sequence[BlockHeaderAPI],
                           timeout: float = None) -> ReceiptsBundles:

        response = await self._event_bus.request(
            GetReceiptsRequest(
                self.session,
                headers,
                timeout,
            ),
            self._broadcast_config
        )

        self.raise_if_needed(response)

        self.logger.debug2(
            "ProxyETHExchangeHandler returning %s receipt bundles from %s",
            len(response.bundles),
            self.session
        )

        return response.bundles

    async def get_pooled_transactions(self,
                                      tx_hashes: Sequence[Hash32],
                                      timeout: float = None) -> Sequence[SignedTransactionAPI]:

        response = await self._event_bus.request(
            GetPooledTransactionsRequest(
                self.session,
                tx_hashes,
                timeout
            ),
            self._broadcast_config
        )

        self.raise_if_needed(response)

        self.logger.debug2(
            "ProxyETHExchangeHandler returning %s pooled transactions from %s",
            len(response.transactions),
            self.session
        )

        return response.transactions

    def send_transactions(self,
                          txns: Sequence[UninterpretedTransaction]) -> None:
        command = Transactions(tuple(txns))
        self._event_bus.broadcast_nowait(
            SendTransactionsEvent(self.session, command),
            self._broadcast_config,
        )

    def send_pooled_transactions(self,
                                 txns: Sequence[SignedTransactionAPI]) -> None:
        command = PooledTransactionsV65(tuple(txns))
        self._event_bus.broadcast_nowait(
            SendPooledTransactionsEvent(self.session, command),
            self._broadcast_config,
        )

    def send_block_headers(self, headers: Sequence[BlockHeaderAPI]) -> None:
        command = BlockHeadersV65(tuple(headers))
        self._event_bus.broadcast_nowait(
            SendBlockHeadersEvent(self.session, command),
            self._broadcast_config,
        )

    def send_new_block_hashes(self, new_block_hashes: Sequence[NewBlockHash]) -> None:
        command = NewBlockHashes(tuple(new_block_hashes))
        self._event_bus.broadcast_nowait(
            SendNewBlockHashesEvent(self.session, command),
            self._broadcast_config,
        )

    def send_new_block(self, block_fields: BlockFields, total_difficulty: int) -> None:
        command = NewBlock(NewBlockPayload(block_fields, total_difficulty))
        self._event_bus.broadcast_nowait(
            SendNewBlockEvent(self.session, command),
            self._broadcast_config,
        )

    def send_block_bodies(self, blocks: Sequence[BlockAPI]) -> None:
        block_bodies = tuple(
            BlockBody(block.transactions, block.uncles)
            for block in blocks
        )
        command = BlockBodiesV65(block_bodies)
        self._event_bus.broadcast_nowait(
            SendBlockBodiesEvent(self.session, command),
            self._broadcast_config,
        )

    def send_receipts(self, receipts: Sequence[Sequence[ReceiptAPI]]) -> None:
        command = ReceiptsV65(deinterpret_receipt_bundles(receipts))
        self._event_bus.broadcast_nowait(
            SendReceiptsEvent(self.session, command),
            self._broadcast_config,
        )

    def send_node_data(self, nodes: Sequence[bytes]) -> None:
        command = NodeDataV65(tuple(nodes))
        self._event_bus.broadcast_nowait(
            SendNodeDataEvent(self.session, command),
            self._broadcast_config,
        )
