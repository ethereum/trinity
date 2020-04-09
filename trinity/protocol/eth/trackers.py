from typing import (
    Optional,
    Tuple,
    Sequence)

from eth.abc import BlockHeaderAPI, SignedTransactionAPI
from eth_typing import Hash32

from p2p.exchange import BasePerformanceTracker

from trinity.protocol.common.payloads import (
    BlockHeadersQuery,
    get_cmd_payload,
)
from trinity.protocol.common.typing import (
    BlockBodyBundles,
    NodeDataBundles,
    ReceiptsBundles,
)

from trinity._utils.headers import sequence_builder

from .commands import (
    AnyGetBlockHeaders,
    AnyGetBlockBodies,
    AnyGetNodeData,
    AnyGetReceipts,
    AnyGetPooledTransactions,
)


BaseGetBlockHeadersTracker = BasePerformanceTracker[
    AnyGetBlockHeaders,
    Tuple[BlockHeaderAPI, ...],
]


class GetBlockHeadersTracker(BaseGetBlockHeadersTracker):

    def _get_request_size(self, request: AnyGetBlockHeaders) -> int:
        payload: BlockHeadersQuery = get_cmd_payload(request)

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


class GetBlockBodiesTracker(BasePerformanceTracker[AnyGetBlockBodies, BlockBodyBundles]):
    def _get_request_size(self, request: AnyGetBlockBodies) -> Optional[int]:
        payload: Tuple[Hash32, ...] = get_cmd_payload(request)
        return len(payload)

    def _get_result_size(self, result: BlockBodyBundles) -> int:
        return len(result)

    def _get_result_item_count(self, result: BlockBodyBundles) -> int:
        return sum(
            len(body.uncles) + len(body.transactions)
            for body, trie_data, uncles_hash
            in result
        )


class GetReceiptsTracker(BasePerformanceTracker[AnyGetReceipts, ReceiptsBundles]):
    def _get_request_size(self, request: AnyGetReceipts) -> Optional[int]:
        payload: Tuple[Hash32, ...] = get_cmd_payload(request)
        return len(payload)

    def _get_result_size(self, result: ReceiptsBundles) -> int:
        return len(result)

    def _get_result_item_count(self, result: ReceiptsBundles) -> int:
        return sum(
            len(receipts)
            for receipts, trie_data
            in result
        )


class GetNodeDataTracker(BasePerformanceTracker[AnyGetNodeData, NodeDataBundles]):
    def _get_request_size(self, request: AnyGetNodeData) -> Optional[int]:
        payload: Tuple[Hash32, ...] = get_cmd_payload(request)
        return len(payload)

    def _get_result_size(self, result: NodeDataBundles) -> int:
        return len(result)

    def _get_result_item_count(self, result: NodeDataBundles) -> int:
        return len(result)


BaseGetPooledTransactionsTracker = BasePerformanceTracker[
    AnyGetPooledTransactions,
    Tuple[SignedTransactionAPI, ...]
]


class GetPooledTransactionsTracker(BaseGetPooledTransactionsTracker):

    def _get_request_size(self, request: AnyGetPooledTransactions) -> Optional[int]:
        payload: Tuple[Hash32, ...] = get_cmd_payload(request)
        return len(payload)

    def _get_result_size(self, result: Sequence[SignedTransactionAPI]) -> int:
        return len(result)

    def _get_result_item_count(self, result: Sequence[SignedTransactionAPI]) -> int:
        return len(result)
