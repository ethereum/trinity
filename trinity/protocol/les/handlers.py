import logging
from typing import (
    cast,
    Tuple,
)

from lahja import (
    BroadcastConfig,
    EndpointAPI,
)

from eth_typing import BlockIdentifier

from eth.rlp.headers import BlockHeader
from eth.tools.logging import (
    ExtendedDebugLogger,
)

from p2p.abc import NodeAPI

from trinity.protocol.common.handlers import (
    BaseChainExchangeHandler,
)
from trinity.protocol.les.events import (
    GetBlockHeadersRequest,
)
from trinity._utils.errors import (
    SupportsError,
)

from .exchanges import GetBlockHeadersExchange


class LESExchangeHandler(BaseChainExchangeHandler):
    _exchange_config = {
        'get_block_headers': GetBlockHeadersExchange,
    }


class ProxyLESExchangeHandler:
    """
    An ``LESExchangeHandler`` that can be used outside of the process that runs the peer pool. Any
    action performed on this class is delegated to the process that runs the peer pool.
    """

    def __init__(self,
                 remote: NodeAPI,
                 event_bus: EndpointAPI,
                 broadcast_config: BroadcastConfig):
        self.remote = remote
        self._event_bus = event_bus
        self._broadcast_config = broadcast_config
        self.logger = cast(
            ExtendedDebugLogger,
            logging.getLogger('trinity.protocol.les.handlers.ProxyLESExchangeHandler')
        )

    def raise_if_needed(self, value: SupportsError) -> None:
        if value.error is not None:
            self.logger.warning(
                "Raised %s while fetching from peer %s", value.error, self.remote.uri
            )
            raise value.error

    async def get_block_headers(self,
                                block_number_or_hash: BlockIdentifier,
                                max_headers: int = None,
                                skip: int = 0,
                                reverse: bool = True,
                                timeout: float = None) -> Tuple[BlockHeader, ...]:
        response = await self._event_bus.request(
            GetBlockHeadersRequest(
                self.remote,
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
            self.remote
        )

        return response.headers
