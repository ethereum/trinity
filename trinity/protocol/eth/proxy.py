import asyncio
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
    BaseEvent,
    BaseRequestResponseEvent,
)
from lahja.base import TResponse

from p2p.abc import SessionAPI

from trinity._utils.errors import SupportsError
from trinity._utils.logging import get_logger
from trinity.exceptions import ExpectedSubscribersMissing
from trinity.protocol.common.typing import (
    BlockBodyBundles,
    NodeDataBundles,
    ReceiptsBundles,
)
from trinity.rlp.block_body import BlockBody

from .commands import (
    BlockBodies,
    BlockHeaders,
    NodeData,
    Receipts,
    Transactions,
    PooledTransactions,
)
from .events import (
    GetBlockBodiesRequest,
    GetBlockHeadersRequest,
    GetNodeDataRequest,
    GetReceiptsRequest,
    SendBlockBodiesEvent,
    SendBlockHeadersEvent,
    SendNodeDataEvent,
    SendReceiptsEvent,
    SendTransactionsEvent,
    GetPooledTransactionsRequest,
    SendPooledTransactionsEvent,
)

# Maximum number of seconds to wait for event / request subscribers
MAX_SUBSCRIBER_WAIT_SECONDS = 2


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

        response = await self._safe_request(
            GetBlockBodiesRequest(
                self.session,
                headers,
                timeout,
            )
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

        response = await self._safe_request(
            GetNodeDataRequest(
                self.session,
                node_hashes,
                timeout,
            ),
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

        response = await self._safe_request(
            GetReceiptsRequest(
                self.session,
                headers,
                timeout,
            )
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

        response = await self._safe_request(
            GetPooledTransactionsRequest(
                self.session,
                tx_hashes,
                timeout
            )
        )

        self.raise_if_needed(response)

        self.logger.debug2(
            "ProxyETHExchangeHandler returning %s pooled transactions from %s",
            len(response.transactions),
            self.session
        )

        return response.transactions

    async def send_transactions(self,
                                txns: Sequence[SignedTransactionAPI]) -> None:
        command = Transactions(tuple(txns))
        await self._safe_broadcast(SendTransactionsEvent(self.session, command))

    async def send_pooled_transactions(self,
                                       txns: Sequence[SignedTransactionAPI]) -> None:
        command = PooledTransactions(tuple(txns))
        await self._safe_broadcast(SendPooledTransactionsEvent(self.session, command))

    async def send_block_headers(self, headers: Sequence[BlockHeaderAPI]) -> None:
        command = BlockHeaders(tuple(headers))
        await self._safe_broadcast(SendBlockHeadersEvent(self.session, command))

    async def send_block_bodies(self, blocks: Sequence[BlockAPI]) -> None:
        block_bodies = tuple(
            BlockBody(block.transactions, block.uncles)
            for block in blocks
        )
        command = BlockBodies(block_bodies)
        await self._safe_broadcast(SendBlockBodiesEvent(self.session, command))

    async def send_receipts(self, receipts: Sequence[Sequence[ReceiptAPI]]) -> None:
        command = Receipts(tuple(map(tuple, receipts)))
        await self._safe_broadcast(SendReceiptsEvent(self.session, command))

    async def send_node_data(self, nodes: Sequence[bytes]) -> None:
        command = NodeData(tuple(nodes))
        await self._safe_broadcast(SendNodeDataEvent(self.session, command))

    async def _safe_broadcast(self, event: BaseEvent) -> None:
        """
        Make a broadcast only after it was validated that is has subscribers,
        otherwise raise exception.
        """
        event_type = type(event)
        try:
            await asyncio.wait_for(
                self._event_bus.wait_until_any_endpoint_subscribed_to(event_type),
                MAX_SUBSCRIBER_WAIT_SECONDS,
            )
        except asyncio.TimeoutError:
            raise ExpectedSubscribersMissing(f"No subscribers for event type: {event_type}")
        else:
            self._event_bus.broadcast_nowait(
                event,
                self._broadcast_config,
            )

    async def _safe_request(self, request: BaseRequestResponseEvent[TResponse]) -> TResponse:
        """
        Make a request only after it was validated that it has subscribers,
        otherwise raise an exception.
        """
        request_type = type(request)
        try:
            await asyncio.wait_for(
                self._event_bus.wait_until_any_endpoint_subscribed_to(request_type),
                MAX_SUBSCRIBER_WAIT_SECONDS,
            )
        except asyncio.TimeoutError:
            raise ExpectedSubscribersMissing(f"No subscribers for request type: {request_type}")
        else:
            return await self._event_bus.request(request, self._broadcast_config)
