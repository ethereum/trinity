from typing import (
    Optional,
    Tuple,
    Sequence)

from eth.abc import BlockHeaderAPI, SignedTransactionAPI

from p2p.exchange import BasePerformanceTracker

from trinity.protocol.common.typing import (
    BlockBodyBundles,
    NodeDataBundles,
    ReceiptsBundles,
)
from trinity._utils.headers import sequence_builder

from .commands import (
    GetBlockBodiesV65,
    GetBlockHeadersV65,
    GetNodeDataV65,
    GetReceiptsV65,
    GetPooledTransactionsV65,
)


BaseGetBlockHeadersTracker = BasePerformanceTracker[
    GetBlockHeadersV65,
    Tuple[BlockHeaderAPI, ...],
]


class GetBlockHeadersTracker(BaseGetBlockHeadersTracker):
    def _get_request_size(self, request: GetBlockHeadersV65) -> int:
        payload = request.payload
        if isinstance(payload.block_number_or_hash, int):
            return len(sequence_builder(
                start_number=payload.block_number_or_hash,
                max_length=payload.max_headers,
                skip=payload.skip,
                reverse=payload.reverse,
            ))
        else:
            return None

    def _get_result_size(self, result: Tuple[BlockHeaderAPI, ...]) -> Optional[int]:
        return len(result)

    def _get_result_item_count(self, result: Tuple[BlockHeaderAPI, ...]) -> int:
        return len(result)


class GetBlockBodiesTracker(BasePerformanceTracker[GetBlockBodiesV65, BlockBodyBundles]):
    def _get_request_size(self, request: GetBlockBodiesV65) -> Optional[int]:
        return len(request.payload)

    def _get_result_size(self, result: BlockBodyBundles) -> int:
        return len(result)

    def _get_result_item_count(self, result: BlockBodyBundles) -> int:
        return sum(
            len(body.uncles) + len(body.transactions)
            for body, trie_data, uncles_hash
            in result
        )


class GetReceiptsTracker(BasePerformanceTracker[GetReceiptsV65, ReceiptsBundles]):
    def _get_request_size(self, request: GetReceiptsV65) -> Optional[int]:
        return len(request.payload)

    def _get_result_size(self, result: ReceiptsBundles) -> int:
        return len(result)

    def _get_result_item_count(self, result: ReceiptsBundles) -> int:
        return sum(
            len(receipts)
            for receipts, trie_data
            in result
        )


class GetNodeDataTracker(BasePerformanceTracker[GetNodeDataV65, NodeDataBundles]):
    def _get_request_size(self, request: GetNodeDataV65) -> Optional[int]:
        return len(request.payload)

    def _get_result_size(self, result: NodeDataBundles) -> int:
        return len(result)

    def _get_result_item_count(self, result: NodeDataBundles) -> int:
        return len(result)


BaseGetPooledTransactionsTracker = BasePerformanceTracker[
    GetPooledTransactionsV65,
    Tuple[SignedTransactionAPI, ...]
]


class GetPooledTransactionsTracker(BaseGetPooledTransactionsTracker):

    def _get_request_size(self, request: GetPooledTransactionsV65) -> Optional[int]:
        return len(request.payload)

    def _get_result_size(self, result: Sequence[SignedTransactionAPI]) -> int:
        return len(result)

    def _get_result_item_count(self, result: Sequence[SignedTransactionAPI]) -> int:
        return len(result)
