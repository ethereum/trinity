from typing import (
    Iterable,
)

from eth.db.trie import _make_trie_root_and_nodes, TrieRootAndData
import rlp

from trinity.rlp.sedes import DecodedZeroOrOneLayerRLP


def make_trie_root_and_nodes(uninterpreted: Iterable[DecodedZeroOrOneLayerRLP]) -> TrieRootAndData:
    """
    Make a trie root, and get the trie nodes, for loose data.

    This is currently used for transactions or receipts. It could be used for
    any object that is either bytes or a list of bytes, which should be
    rlp-encoded before insertion to a trie.
    """
    def encode_serialized(serialized_transaction: DecodedZeroOrOneLayerRLP) -> bytes:
        if isinstance(serialized_transaction, bytes):
            return serialized_transaction
        else:
            return rlp.encode(serialized_transaction)

    encoded_items = tuple(encode_serialized(item) for item in uninterpreted)
    return _make_trie_root_and_nodes(encoded_items)
