from typing import (
    Sequence,
    Tuple,
)

from eth_typing import (
    Hash32,
)
from eth_utils import (
    ValidationError,
)
from eth.abc import BlockHeaderAPI, SignedTransactionAPI

from p2p.exchange import ValidatorAPI

from trinity._utils.logging import get_logger
from trinity.protocol.common.validators import (
    BaseBlockHeadersValidator,
)
from trinity.protocol.common.typing import (
    BlockBodyBundles,
    NodeDataBundles,
    ReceiptsBundles,
)

from . import constants


class GetBlockHeadersValidator(BaseBlockHeadersValidator):
    protocol_max_request_size = constants.MAX_HEADERS_FETCH


class GetNodeDataValidator(ValidatorAPI[NodeDataBundles]):
    def __init__(self, node_hashes: Sequence[Hash32]) -> None:
        self.logger = get_logger("trinity.protocol.eth.validators.GetNodeDataValidator")
        self.node_hashes = node_hashes

        # Check for uniqueness
        num_requested = len(node_hashes)
        num_unique = len(set(node_hashes))
        if num_requested != num_unique:
            self.logger.warning(
                "GetNodeData: Asked peer for %d trie nodes, but %d were duplicates",
                num_requested,
                num_requested - num_unique,
            )

    def validate_result(self, response: NodeDataBundles) -> None:
        if not response:
            # an empty response is always valid
            return

        node_keys = tuple(node_key for node_key, node in response)
        node_key_set = set(node_keys)

        if len(node_keys) != len(node_key_set):
            raise ValidationError("Response may not contain duplicate nodes")

        unexpected_keys = node_key_set.difference(self.node_hashes)

        if unexpected_keys:
            raise ValidationError(f"Response contains {len(unexpected_keys)} unexpected nodes")


class ReceiptsValidator(ValidatorAPI[ReceiptsBundles]):
    def __init__(self, headers: Sequence[BlockHeaderAPI]) -> None:
        self.headers = headers

    def validate_result(self, result: ReceiptsBundles) -> None:
        if not result:
            # empty result is always valid.
            return

        expected_receipt_roots = set(header.receipt_root for header in self.headers)
        actual_receipt_roots = set(
            root_hash
            for receipt, (root_hash, trie_data)
            in result
        )

        unexpected_roots = actual_receipt_roots.difference(expected_receipt_roots)

        if unexpected_roots:
            raise ValidationError(f"Got {len(unexpected_roots)} unexpected receipt roots")


class GetBlockBodiesValidator(ValidatorAPI[BlockBodyBundles]):
    def __init__(self, headers: Sequence[BlockHeaderAPI]) -> None:
        self.headers = headers

    def validate_result(self, response: BlockBodyBundles) -> None:
        expected_keys = {
            (header.transaction_root, header.uncles_hash)
            for header in self.headers
        }
        actual_keys = {
            (txn_root, uncles_hash)
            for body, (txn_root, trie_data), uncles_hash
            in response
        }
        unexpected_keys = actual_keys.difference(expected_keys)
        if unexpected_keys:
            raise ValidationError(f"Got {len(unexpected_keys)} unexpected block bodies")


class GetPooledTransactionsValidator(ValidatorAPI[Tuple[SignedTransactionAPI, ...]]):
    def __init__(self, transaction_hashes: Sequence[Hash32]) -> None:
        self.transaction_hashes = transaction_hashes

    def validate_result(self, response: Tuple[SignedTransactionAPI, ...]) -> None:
        if not response:
            # an empty response is always valid
            return

        tx_hashes = tuple(tx.hash for tx in response)
        tx_hash_set = set(tx_hashes)

        if len(tx_hashes) != len(tx_hash_set):
            raise ValidationError("Response may not contain duplicate tx hashes")

        unexpected_hashs = tx_hash_set.difference(self.transaction_hashes)

        if unexpected_hashs:
            raise ValidationError(
                f"Response contains {len(unexpected_hashs)} unexpected transactions"
            )
