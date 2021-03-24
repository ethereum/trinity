from typing import (
    Iterable,
)

from eth_utils import (
    to_tuple,
)

from eth_hash.auto import keccak
import rlp

from p2p.exchange import BaseNormalizer

from trinity._utils.trie import make_trie_root_and_nodes
from trinity.protocol.common.typing import (
    BlockBodyBundle,
    BlockBodyBundles,
    NodeDataBundles,
    ReceiptsBundles,
)

from .commands import (
    BlockBodiesV65,
    NodeDataV65,
    ReceiptsV65,
)


class GetNodeDataNormalizer(BaseNormalizer[NodeDataV65, NodeDataBundles]):
    is_normalization_slow = True

    def normalize_result(self, cmd: NodeDataV65) -> NodeDataBundles:
        node_keys = map(keccak, cmd.payload)
        result = tuple(zip(node_keys, cmd.payload))
        return result


class ReceiptsNormalizer(BaseNormalizer[ReceiptsV65, ReceiptsBundles]):
    is_normalization_slow = True

    def normalize_result(self, cmd: ReceiptsV65) -> ReceiptsBundles:
        trie_roots_and_data = map(make_trie_root_and_nodes, cmd.payload)
        return tuple(zip(cmd.payload, trie_roots_and_data))


class GetBlockBodiesNormalizer(BaseNormalizer[BlockBodiesV65, BlockBodyBundles]):
    is_normalization_slow = True

    @to_tuple
    def normalize_result(self, cmd: BlockBodiesV65) -> Iterable[BlockBodyBundle]:
        for body in cmd.payload:
            uncle_hashes = keccak(rlp.encode(body.uncles))
            transaction_root_and_nodes = make_trie_root_and_nodes(body.transactions)
            yield body, transaction_root_and_nodes, uncle_hashes
