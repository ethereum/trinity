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
from trinity._utils.requests import gen_request_id
from trinity.protocol.common.payloads import (
    BlockHeadersQuery, Hash32TupleQueryPayload, BlockHeadersQueryPayload,
)
from trinity.protocol.common.validators import match_payload_request_id
from trinity.protocol.common.typing import (
    BlockBodyBundles,
    NodeDataBundles,
    ReceiptsBundles,
)

from .commands import (
    BlockBodiesV65,
    BlockHeadersV65,
    BlockHeadersV66,
    GetBlockBodiesV65,
    GetBlockHeadersV65,
    GetBlockHeadersV66,
    GetNodeDataV65,
    GetReceiptsV65,
    NodeDataV65,
    ReceiptsV65,
    GetPooledTransactionsV65,
    PooledTransactionsV65,
    GetBlockBodiesV66, BlockBodiesV66, GetNodeDataV66, NodeDataV66, GetReceiptsV66, ReceiptsV66,
    PooledTransactionsV66, GetPooledTransactionsV66)
from .normalizers import (
    GetBlockBodiesNormalizer,
    GetNodeDataNormalizer,
    ReceiptsNormalizer,
    GetBlockBodiesV66Normalizer, GetNodeDataV66Normalizer, GetReceiptsV66Normalizer)

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
    _normalizer = DefaultNormalizer(BlockHeadersV65, Tuple[BlockHeaderAPI, ...])
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


BaseGetBlockHeadersV66Exchange = BaseExchange[
    GetBlockHeadersV66,
    BlockHeadersV66,
    Tuple[BlockHeaderAPI, ...],
]


class GetBlockHeadersV66Exchange(BaseGetBlockHeadersV66Exchange):
    _normalizer = DefaultNormalizer(
        BlockHeadersV66,
        Tuple[BlockHeaderAPI, ...],
        normalize_fn=lambda res: res.payload.result
    )
    tracker_class = GetBlockHeadersTracker

    _request_command_type = GetBlockHeadersV66
    _response_command_type = BlockHeadersV66

    async def __call__(  # type: ignore
            self,
            block_number_or_hash: BlockIdentifier,
            max_headers: int = None,
            skip: int = 0,
            reverse: bool = True,
            timeout: float = None) -> Tuple[BlockHeaderAPI, ...]:

        original_request_args = (block_number_or_hash, max_headers, skip, reverse)
        validator = GetBlockHeadersValidator(*original_request_args)

        query = BlockHeadersQuery(
            block_number_or_hash=block_number_or_hash,
            max_headers=max_headers,
            skip=skip,
            reverse=reverse,
        )
        payload = BlockHeadersQueryPayload(
            request_id=gen_request_id(),
            query=query,
        )
        request = GetBlockHeadersV66(payload)

        return tuple(await self.get_result(
            request,
            self._normalizer,
            validator,
            match_payload_request_id,
            timeout,
        ))


BaseNodeDataExchange = BaseExchange[GetNodeDataV65, NodeDataV65, NodeDataBundles]


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


class GetNodeDataV66Exchange(BaseExchange[GetNodeDataV66, NodeDataV66, NodeDataBundles]):
    _normalizer = GetNodeDataV66Normalizer()
    tracker_class = GetNodeDataTracker

    _request_command_type = GetNodeDataV66
    _response_command_type = NodeDataV66

    async def __call__(self,  # type: ignore
                       node_hashes: Sequence[Hash32],
                       timeout: float = None) -> NodeDataBundles:
        validator = GetNodeDataValidator(node_hashes)

        request = GetNodeDataV66(Hash32TupleQueryPayload(
            request_id=gen_request_id(),
            query=tuple(node_hashes)
        ))

        return await self.get_result(
            request,
            self._normalizer,
            validator,
            match_payload_request_id,
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


class GetReceiptsV66Exchange(BaseExchange[GetReceiptsV66, ReceiptsV66, ReceiptsBundles]):
    _normalizer = GetReceiptsV66Normalizer()
    tracker_class = GetReceiptsTracker

    _request_command_type = GetReceiptsV66
    _response_command_type = ReceiptsV66

    async def __call__(self,  # type: ignore
                       headers: Sequence[BlockHeaderAPI],
                       timeout: float = None) -> ReceiptsBundles:
        validator = ReceiptsValidator(headers)

        block_hashes = tuple(header.hash for header in headers)

        request = GetReceiptsV66(Hash32TupleQueryPayload(
            request_id=gen_request_id(),
            query=block_hashes
        ))

        return await self.get_result(
            request,
            self._normalizer,
            validator,
            match_payload_request_id,
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


BaseGetBlockBodiesV66Exchange = BaseExchange[
    GetBlockBodiesV66,
    BlockBodiesV66,
    BlockBodyBundles,
]


class GetBlockBodiesV66Exchange(BaseGetBlockBodiesV66Exchange):
    _normalizer = GetBlockBodiesV66Normalizer()
    tracker_class = GetBlockBodiesTracker

    _request_command_type = GetBlockBodiesV66
    _response_command_type = BlockBodiesV66

    async def __call__(self,  # type: ignore
                       headers: Sequence[BlockHeaderAPI],
                       timeout: float = None) -> BlockBodyBundles:

        validator = GetBlockBodiesValidator(headers)
        block_hashes = tuple(header.hash for header in headers)

        request = GetBlockBodiesV66(Hash32TupleQueryPayload(
            request_id=gen_request_id(),
            query=block_hashes
        ))

        return tuple(await self.get_result(
            request,
            self._normalizer,
            validator,
            match_payload_request_id,
            timeout,
        ))


BasePooledTransactionsV65Exchange = BaseExchange[
    GetPooledTransactionsV65,
    PooledTransactionsV65,
    Tuple[SignedTransactionAPI, ...]
]


class GetPooledTransactionsV65Exchange(BasePooledTransactionsV65Exchange):
    _normalizer = DefaultNormalizer(PooledTransactionsV65, Tuple[SignedTransactionAPI, ...])
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


BasePooledTransactionsV66Exchange = BaseExchange[
    GetPooledTransactionsV66,
    PooledTransactionsV66,
    Tuple[SignedTransactionAPI, ...]
]


class GetPooledTransactionsV66Exchange(BasePooledTransactionsV66Exchange):
    _normalizer = DefaultNormalizer(
        PooledTransactionsV66,
        Tuple[SignedTransactionAPI, ...],
        normalize_fn=lambda res: res.payload.result
    )
    tracker_class = GetPooledTransactionsTracker

    _request_command_type = GetPooledTransactionsV66
    _response_command_type = PooledTransactionsV66

    async def __call__(self,  # type: ignore
                       transaction_hashes: Sequence[Hash32],
                       timeout: float = None) -> Tuple[SignedTransactionAPI, ...]:
        validator = GetPooledTransactionsValidator(transaction_hashes)

        request = GetPooledTransactionsV66(Hash32TupleQueryPayload(
            request_id=gen_request_id(),
            query=tuple(transaction_hashes)
        ))

        return await self.get_result(
            request,
            self._normalizer,
            validator,
            match_payload_request_id,
            timeout,
        )
