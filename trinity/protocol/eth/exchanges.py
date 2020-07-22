from typing import (
    Sequence,
    Tuple,
)

from eth_typing import (
    BlockIdentifier,
    Hash32,
)
from eth.abc import BlockHeaderAPI, SignedTransactionAPI

from p2p.exchange import (
    BaseExchange,
    noop_payload_validator,
)
from p2p.exchange.normalizers import DefaultNormalizer
from trinity.protocol.common.payloads import (
    BlockHeadersQuery,
)
from trinity.protocol.common.typing import (
    BlockBodyBundles,
    NodeDataBundles,
    ReceiptsBundles,
)

from .commands import (
    BlockBodiesV65,
    BlockHeadersV65,
    GetBlockBodiesV65,
    GetBlockHeadersV65,
    GetNodeDataV65,
    GetReceiptsV65,
    NodeDataV65,
    ReceiptsV65,
    GetPooledTransactionsV65,
    PooledTransactionsV65,
)
from .normalizers import (
    GetBlockBodiesNormalizer,
    GetNodeDataNormalizer,
    ReceiptsNormalizer,
)
from .trackers import (
    GetBlockHeadersTracker,
    GetBlockBodiesTracker,
    GetNodeDataTracker,
    GetReceiptsTracker,
    GetPooledTransactionsTracker,
)
from .validators import (
    GetBlockBodiesValidator,
    GetBlockHeadersValidator,
    GetNodeDataValidator,
    ReceiptsValidator,
    GetPooledTransactionsValidator,
)

BaseGetBlockHeadersV65Exchange = BaseExchange[
    GetBlockHeadersV65,
    BlockHeadersV65,
    Tuple[BlockHeaderAPI, ...],
]


class GetBlockHeadersV65Exchange(BaseGetBlockHeadersV65Exchange):
    _normalizer = DefaultNormalizer(BlockHeadersV65, tuple)
    tracker_class = GetBlockHeadersTracker

    _request_command_type = GetBlockHeadersV65
    _response_command_type = BlockHeadersV65

    async def __call__(  # type: ignore
            self,
            block_number_or_hash: BlockIdentifier,
            max_headers: int = None,
            skip: int = 0,
            reverse: bool = True,
            timeout: float = None) -> Tuple[BlockHeaderAPI, ...]:

        original_request_args = (block_number_or_hash, max_headers, skip, reverse)
        validator = GetBlockHeadersValidator(*original_request_args)
        request = GetBlockHeadersV65(BlockHeadersQuery(*original_request_args))

        return await self.get_result(
            request,
            self._normalizer,
            validator,
            noop_payload_validator,
            timeout,
        )


class GetNodeDataV65Exchange(BaseExchange[GetNodeDataV65, NodeDataV65, NodeDataBundles]):
    _normalizer = GetNodeDataNormalizer()
    tracker_class = GetNodeDataTracker

    _request_command_type = GetNodeDataV65
    _response_command_type = NodeDataV65

    async def __call__(self,  # type: ignore
                       node_hashes: Sequence[Hash32],
                       timeout: float = None) -> NodeDataBundles:
        validator = GetNodeDataValidator(node_hashes)
        request = GetNodeDataV65(tuple(node_hashes))
        return await self.get_result(
            request,
            self._normalizer,
            validator,
            noop_payload_validator,
            timeout,
        )


class GetReceiptsV65Exchange(BaseExchange[GetReceiptsV65, ReceiptsV65, ReceiptsBundles]):
    _normalizer = ReceiptsNormalizer()
    tracker_class = GetReceiptsTracker

    _request_command_type = GetReceiptsV65
    _response_command_type = ReceiptsV65

    async def __call__(self,  # type: ignore
                       headers: Sequence[BlockHeaderAPI],
                       timeout: float = None) -> ReceiptsBundles:
        validator = ReceiptsValidator(headers)

        block_hashes = tuple(header.hash for header in headers)
        request = GetReceiptsV65(block_hashes)

        return await self.get_result(
            request,
            self._normalizer,
            validator,
            noop_payload_validator,
            timeout,
        )


BaseGetBlockBodiesV65Exchange = BaseExchange[
    GetBlockBodiesV65,
    BlockBodiesV65,
    BlockBodyBundles,
]


class GetBlockBodiesV65Exchange(BaseGetBlockBodiesV65Exchange):
    _normalizer = GetBlockBodiesNormalizer()
    tracker_class = GetBlockBodiesTracker

    _request_command_type = GetBlockBodiesV65
    _response_command_type = BlockBodiesV65

    async def __call__(self,  # type: ignore
                       headers: Sequence[BlockHeaderAPI],
                       timeout: float = None) -> BlockBodyBundles:
        validator = GetBlockBodiesValidator(headers)

        block_hashes = tuple(header.hash for header in headers)
        request = GetBlockBodiesV65(block_hashes)

        return await self.get_result(
            request,
            self._normalizer,
            validator,
            noop_payload_validator,
            timeout,
        )


BasePooledTransactionsV65Exchange = BaseExchange[
    GetPooledTransactionsV65,
    PooledTransactionsV65,
    Tuple[SignedTransactionAPI, ...]
]


class GetPooledTransactionsV65Exchange(BasePooledTransactionsV65Exchange):
    _normalizer = DefaultNormalizer(PooledTransactionsV65, tuple)
    tracker_class = GetPooledTransactionsTracker

    _request_command_type = GetPooledTransactionsV65
    _response_command_type = PooledTransactionsV65

    async def __call__(self,  # type: ignore
                       transaction_hashes: Sequence[Hash32],
                       timeout: float = None) -> Tuple[SignedTransactionAPI, ...]:
        validator = GetPooledTransactionsValidator(transaction_hashes)
        request = GetPooledTransactionsV65(tuple(transaction_hashes))
        return await self.get_result(
            request,
            self._normalizer,
            validator,
            noop_payload_validator,
            timeout,
        )
