import asyncio
from abc import (
    abstractmethod,
)
import concurrent
from typing import (
    Awaitable,
    Tuple,
    TypeVar,
)
import typing_extensions

from lahja import (
    BroadcastConfig,
    Endpoint,
)
from p2p.peer import (
    IdentifiablePeer,
)
from eth.rlp.headers import (
    BlockHeader,
)
from eth_typing import (
    BlockIdentifier,
    Hash32,
)


from trinity.protocol.common.handlers import (
    BaseChainExchangeHandler,
)
from trinity.protocol.common.types import (
    BlockBodyBundles,
    NodeDataBundles,
    ReceiptsBundles,
)
from trinity.protocol.eth.events import (
    GetBlockHeadersRequest,
    GetBlockBodiesRequest,
    GetNodeDataRequest,
    GetReceiptsRequest,
)

from .exchanges import (
    GetBlockBodiesExchange,
    GetBlockHeadersExchange,
    GetNodeDataExchange,
    GetReceiptsExchange,
)


class ETHExchangeHandlerLike(typing_extensions.Protocol):
    """
    Define the ETHExchangeHandler protocol that streamlines peer communication
    into request/response pairs
    """

    @abstractmethod
    async def get_block_headers(self,
                                block_number_or_hash: BlockIdentifier,
                                max_headers: int = None,
                                skip: int = 0,
                                reverse: bool = True,
                                timeout: float = None) -> Tuple[BlockHeader, ...]:
        pass

    @abstractmethod
    async def get_block_bodies(self,
                               headers: Tuple[BlockHeader, ...],
                               timeout: float = None) -> BlockBodyBundles:
        pass

    @abstractmethod
    async def get_node_data(self,
                            node_hashes: Tuple[Hash32, ...],
                            timeout: float = None) -> NodeDataBundles:
        pass

    @abstractmethod
    async def get_receipts(self,
                           headers: Tuple[BlockHeader, ...],
                           timeout: float = None) -> ReceiptsBundles:
        pass


class ETHExchangeHandler(BaseChainExchangeHandler):
    _exchange_config = {
        'get_block_bodies': GetBlockBodiesExchange,
        'get_block_headers': GetBlockHeadersExchange,
        'get_node_data': GetNodeDataExchange,
        'get_receipts': GetReceiptsExchange,
    }

    # These are needed only to please mypy.
    get_block_bodies: GetBlockBodiesExchange
    get_node_data: GetNodeDataExchange
    get_receipts: GetReceiptsExchange


class ProxyETHExchangeHandler:
    """
    An ``ETHExchangeHandler`` that can be used outside of the process that runs the peer pool. Any
    action performed on this class is delegated to the process that runs the peer pool.
    """

    def __init__(self,
                 dto_peer: IdentifiablePeer,
                 event_bus: Endpoint,
                 broadcast_config: BroadcastConfig):
        self._dto_peer = dto_peer
        self._event_bus = event_bus
        self._broadcast_config = broadcast_config

    T = TypeVar('T')

    async def wait_for(self, awaitable: Awaitable[T], timeout: float) -> T:
        # FIXME: Just a crutch to make it work for now
        try:
            return await asyncio.wait_for(awaitable, timeout=timeout)
        except concurrent.futures.TimeoutError as e:
            raise TimeoutError(e) from e

    async def get_block_headers(self,
                                block_number_or_hash: BlockIdentifier,
                                max_headers: int = None,
                                skip: int = 0,
                                reverse: bool = True,
                                timeout: float = None) -> Tuple[BlockHeader, ...]:

        response = await self.wait_for(self._event_bus.request(
            GetBlockHeadersRequest(
                self._dto_peer,
                block_number_or_hash,
                max_headers,
                skip,
                reverse,
                timeout,
            ),
            self._broadcast_config
        ), timeout=timeout)

        return response.headers

    async def get_block_bodies(self,
                               headers: Tuple[BlockHeader, ...],
                               timeout: float = None) -> BlockBodyBundles:

        response = await self.wait_for(self._event_bus.request(
            GetBlockBodiesRequest(
                self._dto_peer,
                headers,
                timeout,
            ),
            self._broadcast_config
        ), timeout=timeout)

        return response.bundles

    async def get_node_data(self,
                            node_hashes: Tuple[Hash32, ...],
                            timeout: float = None) -> NodeDataBundles:

        response = await self.wait_for(self._event_bus.request(
            GetNodeDataRequest(
                self._dto_peer,
                node_hashes,
                timeout,
            ),
            self._broadcast_config
        ), timeout=timeout)

        return response.bundles

    async def get_receipts(self,
                           headers: Tuple[BlockHeader, ...],
                           timeout: float = None) -> ReceiptsBundles:

        response = await self.wait_for(self._event_bus.request(
            GetReceiptsRequest(
                self._dto_peer,
                headers,
                timeout,
            ),
            self._broadcast_config
        ), timeout=timeout)

        return response.bundles
