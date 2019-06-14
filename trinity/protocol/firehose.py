import asyncio
import contextlib
from datetime import datetime
import enum
from functools import reduce
import itertools
import struct
from typing import (
    Any,
    cast,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    Set,
)
import secrets
import queue
import pickle
import logging

import plyvel
import time

from cancel_token import CancelToken, OperationCancelled

from eth.db.atomic import AtomicDB
from eth.db.chain import BaseChainDB, ChainDB
from eth.db.backends.base import BaseAtomicDB
from eth.db.header import BaseHeaderDB
from eth.rlp.headers import BlockHeader
from eth.rlp.sedes import trie_root
from eth.rlp.receipts import Receipt

from eth_hash.auto import keccak

from eth_keys import datatypes
from eth_utils import (
    big_endian_to_int,
    to_tuple,
)

from eth_utils.toolz import (
    first,
    compose,
    groupby,
)

from eth_typing import (
    BlockNumber,
    Hash32,
)

import rlp
import rlp.codec
from rlp import sedes
from rlp.sedes import (
    BigEndianInt,
)

from trie.constants import (
    NODE_TYPE_BLANK,
    NODE_TYPE_BRANCH,
    NODE_TYPE_EXTENSION,
    NODE_TYPE_LEAF,
)

from trie.hexary import HexaryTrie

from trie.utils.nibbles import (
    encode_nibbles,
    decode_nibbles,
    nibbles_to_bytes,
    add_nibbles_terminator,
)

from trie.utils.nodes import (
    get_common_prefix_length,
    decode_node,
    extract_key,
    get_node_type,
    compute_leaf_key,
    compute_extension_key,
)

from p2p.auth import (
    decode_authentication,
    HandshakeResponder,
    HandshakeInitiator,
    _handshake
)
from p2p.constants import (
    DEFAULT_PEER_BOOT_TIMEOUT,
    ENCRYPTED_AUTH_MSG_LEN,
    HASH_LEN,
    REPLY_TIMEOUT,
)
from p2p import ecies
from p2p.exceptions import HandshakeFailure, PeerConnectionLost, DecryptionError
from p2p.kademlia import Address, Node
from p2p.p2p_proto import DisconnectReason
from p2p.peer import BasePeer, BasePeerFactory, PeerSubscriber
from p2p.peer_pool import BasePeerPool
from p2p.protocol import (
    BaseRequest,
    Command,
    _DecodedMsgType,
    PayloadType,
    Protocol,
)
from p2p.service import BaseService
from p2p.transport import Transport

from trinity.protocol.common.exchanges import BaseExchange
from trinity.protocol.common.handlers import BaseExchangeHandler
from trinity.protocol.common.normalizers import BaseNormalizer, NoopNormalizer
from trinity.protocol.common.servers import BaseRequestServer
from trinity.protocol.common.trackers import BasePerformanceTracker
from trinity.protocol.common.validators import BaseValidator, noop_payload_validator

from trinity.protocol.common.managers import ExchangeManager

from trinity.sync.full.hexary_trie import _get_children

from trinity.rlp.block_body import BlockBody


# trinity.db.eth1.chain


"""
TODO: Full Sync trinity for 1000 blocks and Full Sync geth for 1000 blocks, then commit
both databases to the repository. Once those are in write some tests which assert that all
these methods give the same answers.
"""


class GethHeaderDB(BaseHeaderDB):
    """
    An implemention of HeaderDB which can read from Geth's database format
    """
    # from https://github.com/ethereum/go-ethereum/blob/v1.8.27/core/rawdb/schema.go
    LAST_BLOCK = b'LastBlock'

    HEADER_PREFIX = b'h'
    HEADER_HASH_SUFFIX = b'n'
    HEADER_NUMBER_PREFIX = b'H'
    HEADER_TD_SUFFIX = b't'

    ### Helpers

    @staticmethod
    def _encode_block_number(num: int) -> bytes:
        # big-endian 8-byte unsigned int
        return struct.pack('>Q', num)

    @staticmethod
    def _decode_block_number(num: bytes) -> int:
        # big-endian 8-byte unsigned int
        return struct.unpack('>Q', num)[0]

    @classmethod
    def _header_key(cls, encoded_block_number: bytes, block_hash: Hash32) -> bytes:
        # https://github.com/ethereum/go-ethereum/blob/v1.8.27/core/rawdb/schema.go#L81
        return cls.HEADER_PREFIX + encoded_block_number + block_hash

    @classmethod
    def _block_number_key(cls, block_hash: Hash32) -> bytes:
        return cls.HEADER_NUMBER_PREFIX + block_hash

    def _number_for_block(self, block_hash: Hash32) -> bytes:
        key = self._block_number_key(block_hash)
        return self.db.get(key)

    ### Canonical Chain API

    def get_canonical_block_hash(self, block_number: BlockNumber) -> Hash32:
        # https://github.com/ethereum/go-ethereum/blob/v1.8.27/core/rawdb/schema.go#L91

        encoded_block_num = self._encode_block_number(block_number)
        return self.db.get(
            self.HEADER_PREFIX + encoded_block_num + self.HEADER_HASH_SUFFIX
        )

    def get_canonical_block_header_by_number(self, block_number: BlockNumber) -> BlockHeader:
        # https://github.com/ethereum/go-ethereum/blob/v1.8.27/core/rawdb/schema.go#L45
        block_hash = self.get_canonical_block_hash(block_number)

        encoded_block_number = self._encode_block_number(block_number)
        # TODO: rlp.decode this block header?
        encoded_header = self.db.get(self._header_key(encoded_block_number, block_hash))
        return rlp.decode(encoded_header, sedes=BlockHeader)

    def get_canonical_head(self) -> BlockHeader:
        last_head_hash = self.db.get(self.LAST_BLOCK)

        return self.get_block_header_by_hash(last_head_hash)

    ### Header API

    def get_block_header_by_hash(self, block_hash: Hash32) -> BlockHeader:
        # https://github.com/ethereum/go-ethereum/blob/v1.8.27/core/rawdb/schema.go#L48

        # TODO: does block_hash need to be encoded in some way?
        encoded_block_number = self._number_for_block(block_hash)
        block_key = self._header_key(encoded_block_number, block_hash)
        encoded_header = self.db.get(block_key)

        return rlp.decode(encoded_header, sedes=BlockHeader)

    def get_score(self, block_hash: Hash32) -> int:
        # https://github.com/ethereum/go-ethereum/blob/v1.8.27/core/rawdb/schema.go#L46

        block_num = self._number_for_block(block_hash)
        block_key = self._header_key(block_num, block_hash)

        td = self.db.get(block_key + self.HEADER_TD_SUFFIX)
        return rlp.decode(td, sedes=sedes.big_endian_int)

    def header_exists(self, block_hash: Hash32) -> bool:
        block_number_key = self._block_number_key(block_hash)
        if not self.db.exists(block_number_key):
            return False

        encoded_block_number = self._number_for_block(block_hash)
        block_key = self._header_key(encoded_block_number, block_hash)

        return self.db.exists(block_key)

    def persist_header(self,
                       header: BlockHeader
                       ) -> Tuple[Tuple[BlockHeader, ...], Tuple[BlockHeader, ...]]:
        raise NotImplementedError("Writing to Geth databases is not supported")

    def persist_header_chain(self,
                             headers: Iterable[BlockHeader]
                             ) -> Tuple[Tuple[BlockHeader, ...], Tuple[BlockHeader, ...]]:
        raise NotImplementedError("Writing to Geth databases is not supported")


class GethChainDB(GethHeaderDB, BaseChainDB):
    """
    An implementation of ChainDB which can read from Geth's database format
    """

    def __init__(self, db: BaseAtomicDB) -> None:
        self.db = db

    ### Helpers

    BLOCK_BODY_PREFIX = b'b'
    TX_LOOKUP_PREFIX = b'l'
    BLOCK_RECEIPTS_PREFIX = b'r'

    @classmethod
    def _block_body_key(cls, encoded_block_number: bytes, block_hash: Hash32) -> bytes:
        # https://github.com/ethereum/go-ethereum/blob/v1.8.27/core/rawdb/schema.go#L50
        return cls.BLOCK_BODY_PREFIX + encoded_block_number + block_hash

    def _get_block_body(self, block_hash: Hash32) -> BlockBody:
        block_number = self._number_for_block(block_hash)
        key = self._block_body_key(block_number, block_hash)
        encoded_body = self.db.get(key)
        return rlp.decode(encoded_body, sedes=BlockBody)

    def _get_block_transactions(self,
                                block_header: BlockHeader) -> Iterable['BaseTransaction']:
        body = self._get_block_body(block_hash)
        return body.transactions

    ### Header API

    def get_block_uncles(self, uncles_hash: Hash32) -> List[BlockHeader]:
        body = self._get_block_body(uncles_hash)
        return list(body.uncles)  # (it's naturally a tuple)

    ### Block API

    def persist_block(self,
                      block: 'BaseBlock'
                      ) -> Tuple[Tuple[Hash32, ...], Tuple[Hash32, ...]]:
        raise NotImplementedError("Writing to Geth databases is not supported")

    def persist_uncles(self, uncles: Tuple[BlockHeader]) -> Hash32:
        raise NotImplementedError("Writing to Geth databases is not supported")

    ### Transaction API

    def add_receipt(self,
                    block_header: BlockHeader,
                    index_key: int, receipt: Receipt) -> Hash32:
        raise NotImplementedError("Writing to Geth databases is not supported")

    def add_transaction(self,
                        block_header: BlockHeader,
                        index_key: int, transaction: 'BaseTransaction') -> Hash32:
        raise NotImplementedError("Writing to Geth databases is not supported")

    def get_block_transactions(
            self,
            block_header: BlockHeader,
            transaction_class: Type['BaseTransaction']) -> Iterable['BaseTransaction']:
        body = self._get_block_body(block_header.hash)

        encoded = [rlp.encode(txn) for txn in body.transactions]
        decoded = [rlp.decode(txn, sedes=transaction_class) for txn in encoded]

        return decoded

    def get_block_transaction_hashes(self, block_header: BlockHeader) -> Iterable[Hash32]:
        body = self._get_block_body(block_header.hash)
        return [txn.hash for txn in body.transactions]

    def get_receipt_by_index(self,
                             block_number: BlockNumber,
                             receipt_index: int) -> Receipt:

        # 1. Fetch the header from the database
        # 2. Read the requested receipt out of it

        # TODO: implement receipts

        raise NotImplementedError("ChainDB classes must implement this method")

    def get_receipts(self,
                     header: BlockHeader,
                     receipt_class: Type[Receipt]) -> Iterable[Receipt]:
        # https://github.com/ethereum/go-ethereum/blob/v1.8.27/core/rawdb/schema.go#L51

        # geth stores receipts with a custom RLP:

        # type receiptStorageRLP struct {
        #	PostStateOrStatus []byte
        #   CumulativeGasUsed uint64
        #   TxHash            common.Hash
        #   ContractAddress   common.Address
        #   Logs              []*LogForStorage
        #   GasUsed           uint64
        # }

        # TODO: implement receipts

        raise NotImplementedError("ChainDB classes must implement this method")

    def get_transaction_by_index(
            self,
            block_number: BlockNumber,
            transaction_index: int,
            transaction_class: Type['BaseTransaction']) -> 'BaseTransaction':

        block_header = self.get_canonical_block_header_by_number(block_number)
        txns = self.get_block_transactions(block_header, transaction_class)
        return txns[transaction_index]

    def get_transaction_index(self, transaction_hash: Hash32) -> Tuple[BlockNumber, int]:
        # https://github.com/ethereum/go-ethereum/blob/v1.8.27/core/rawdb/schema.go#L53

        block_hash = self.db.get(self.TX_LOOKUP_PREFIX + transaction_hash)
        # https://github.com/ethereum/go-ethereum/blob/f9aa1cd21f776a4d3267d9c89772bdc622468d6d/core/rawdb/accessors_indexes.go#L36
        # there was also a legacy thing which went here
        assert len(block_hash) == 32

        encoded_block_num = self._number_for_block(block_hash)
        block_num = self._decode_block_number(encoded_block_num)

        body = self._get_block_body(block_hash)
        for index, transaction in enumerate(body.transactions):
            if transaction.hash == transaction_hash:
                return block_num, index
        raise Exception('could not find transaction')

    ### Raw Database API

    def exists(self, key: bytes) -> bool:
        return self.db.exists(key)

    def get(self, key: bytes) -> bytes:
        return self.db[key]

    def persist_trie_data_dict(self, trie_data_dict: Dict[Hash32, bytes]) -> None:
        raise NotImplementedError("Writing to Geth databases is not supported")


# Trie Utils


Nibbles = Tuple[int, ...]


class NodeKind(enum.Enum):
    BLANK = NODE_TYPE_BLANK
    LEAF = NODE_TYPE_LEAF
    EXTENSION = NODE_TYPE_EXTENSION
    BRANCH = NODE_TYPE_BRANCH


class TrieNode(NamedTuple):
    kind: NodeKind
    rlp: bytes
    obj: List[bytes]  # this type is wrong but mypy doesn't support recursive types
    keccak: Hash32

    def __str__(self) -> str:
        if self.kind == NodeKind.EXTENSION:
            return (
                "TrieNode(Extension,"
                f" hash={self.keccak.hex()}"
                f" path={self.path_rest}"
                f" child={self.obj[1].hex()}"
                 ")"
            )
        if self.kind == NodeKind.LEAF:
            return (
                "TrieNode(Leaf,"
                f" hash={self.keccak.hex()}"
                f" path={self.path_rest[:10]}..."
                 ")"
            )
        return f"TrieNode(kind={self.kind.name} hash={self.keccak.hex()})"

    @property
    def path_rest(self) -> Nibbles:
        # careful: this doesn't make any sense for branches
        return cast(Nibbles, extract_key(self.obj))


def is_subtree(prefix: Nibbles, nibbles: Nibbles) -> bool:
    """
    Returns True if {nibbles} represents a subtree of {prefix}.
    """
    if len(nibbles) < len(prefix):
        # nibbles represents a bigger tree than prefix does
        return False
    return get_common_prefix_length(prefix, nibbles) == len(prefix)


@to_tuple
def _get_children_with_nibbles(node: TrieNode, prefix: Nibbles) -> Iterable[Tuple[Nibbles, Hash32]]:
    """
    Return the children of the given node at the given path, including their full paths
    """
    if node.kind == NodeKind.BLANK:
        return
    elif node.kind == NodeKind.LEAF:
        full_path = prefix + node.path_rest
        yield (full_path, cast(Hash32, node.obj[1]))
    elif node.kind == NodeKind.EXTENSION:
        full_path = prefix + node.path_rest
        # TODO: this cast to a Hash32 is not right, nodes smaller than 32 are inlined
        yield (full_path, cast(Hash32, node.obj[1]))
    elif node.kind == NodeKind.BRANCH:
        for i in range(17):
            full_path = prefix + (i,)
            yield (full_path, cast(Hash32, node.obj[i]))


def _get_node(db: ChainDB, node_hash: Hash32) -> TrieNode:
    if len(node_hash) < 32:
        node_rlp = node_hash
    else:
        node_rlp = db.get(node_hash)

    node = decode_node(node_rlp)
    node_type = get_node_type(node)

    return TrieNode(kind=NodeKind(node_type), rlp=node_rlp, obj=node, keccak=node_hash)


def _iterate_trie(db: ChainDB,
                  node: TrieNode,  # the node we should look at
                  sub_trie: Nibbles,  # which sub_trie to return nodes from
                  prefix: Nibbles,  # our current path in the trie
                  ) -> Iterable[Tuple[Nibbles, TrieNode]]:

    if node.kind == NodeKind.BLANK:
        return

    if node.kind == NodeKind.LEAF:
        full_path = prefix + node.path_rest

        if is_subtree(sub_trie, prefix) or is_subtree(sub_trie, full_path):
            # also check full_path because either the node or the item the node points to
            # might be part of the desired subtree
            yield (prefix, node)

        # there's no need to recur, this is a leaf
        return

    child_of_sub_trie = is_subtree(sub_trie, prefix)

    if child_of_sub_trie:
        # this node is part of the subtrie which should be returned
        yield (prefix, node)

    parent_of_sub_trie = is_subtree(prefix, sub_trie)

    if child_of_sub_trie or parent_of_sub_trie:
        for path, child_hash in _get_children_with_nibbles(node, prefix):
            child_node = _get_node(db, child_hash)
            yield from _iterate_trie(db, child_node, sub_trie, path)


def iterate_trie(db: ChainDB, root_hash: Hash32,
                 sub_trie: Nibbles = ()) -> Iterable[Tuple[Nibbles, TrieNode]]:

    root_node = _get_node(db, root_hash)

    yield from _iterate_trie(
        db, root_node, sub_trie,
        prefix=(),
    )


def iterate_leaves(db: ChainDB, root_hash: Hash32,
                   sub_trie: Nibbles = ()) -> Iterable[Tuple[Nibbles, bytes]]:
    """
    Rather than returning the raw nodes, this returns just the leaves (usually, accounts),
    along with their full paths
    """

    node_iterator = iterate_trie(db, root_hash, sub_trie)

    for path, node in node_iterator:
        if node.kind == NodeKind.LEAF:
            full_path = path + node.path_rest
            yield (full_path, node.obj[1])


def _iterate_node_chunk(db: ChainDB,
                        node: TrieNode,
                        sub_trie: Nibbles,
                        prefix: Nibbles,
                        target_depth: int) -> Iterable[Tuple[Nibbles, TrieNode]]:

    def recur(new_depth: int) -> Iterable[Tuple[Nibbles, TrieNode]]:
        for path, child_hash in _get_children_with_nibbles(node, prefix):
            child_node = _get_node(db, child_hash)
            yield from _iterate_node_chunk(db, child_node, sub_trie, path, new_depth)

    if node.kind == NodeKind.BLANK:
        return

    if node.kind == NodeKind.LEAF:
        full_path = prefix + node.path_rest

        if is_subtree(sub_trie, prefix) or is_subtree(sub_trie, full_path):
            yield (prefix, node)

        # there's no need to recur, this is a leaf
        return

    child_of_sub_trie = is_subtree(sub_trie, prefix)

    if child_of_sub_trie:
        # the node is part of the sub_trie which we want to return
        yield (prefix, node)

    if target_depth == 0:
        # there's no point in recursing
        return

    parent_of_sub_trie = is_subtree(prefix, sub_trie)

    if child_of_sub_trie:
        # if we're returning nodes start decrementing the count
        yield from recur(target_depth - 1)
    elif parent_of_sub_trie:
        # if we're still looking for the sub_trie just recur
        yield from recur(target_depth)


def iterate_node_chunk(db: ChainDB,
                       root_hash: Hash32,
                       sub_trie: Nibbles,
                       target_depth: int) -> Iterable[Tuple[Nibbles, TrieNode]]:
    """
    Get all the nodes up to {target_depth} deep from the given sub_trie.

    Does a truncated breadth-first search rooted at the given node and returns everything
    it finds.
    """
    # TODO: notice BLANK_NODE_HASH and fail fast?
    root_node = _get_node(db, root_hash)

    yield from _iterate_node_chunk(
        db, root_node, sub_trie, prefix=(), target_depth=target_depth,
    )


# Commands


class Status(Command):
    _cmd_id = 0
    structure = (
        ('protocol_version', sedes.big_endian_int),
    )


class GetLeafCount(Command):
    """
    {Get,}LeafCount will probably not be part of Firehose, but it's included here as an
    easy method to test.
    """
    _cmd_id = 1
    structure = (
        ('request_id', sedes.big_endian_int),
        ('state_root', trie_root),
        ('prefix', sedes.binary),  # encoded in the same way trie nibbles are
    )


class LeafCount(Command):
    # TODO: add a boolean, "more_than", to return when it's taking too long to iterate
    _cmd_id = 2
    structure = (
        ('request_id', sedes.big_endian_int),
        ('leaf_count', sedes.big_endian_int),
    )


class GetNodeChunk(Command):
    """
    Returns a few layers of nodes starting at the given one. If prefix is None this
    returns nodes from the first n levels in the same order that a breadth-first traversal
    would traverse them.
    """
    _cmd_id = 3
    structure = (
        ('request_id', sedes.big_endian_int),
        ('state_root', trie_root),
        ('prefix', sedes.binary),  # encoded in the same way trie nibbles are
    )


class NodeChunk(Command):
    # TODO: There's no way to statelessly validate this response. Maybe it should include
    # a proof of the requested bucket, that would make middleware easier to write?
    _cmd_id = 4
    structure = (
        ('request_id', sedes.big_endian_int),
        ('nodes', sedes.CountableList(sedes.binary)),  # a list of rlp-encoded nodes
    )


class GetLeaves(Command):
    """
    Return all the leaves under the given prefix, unless there are too many

    TODO: accept a list of prefixes and return the leaves for each
    TODO: Ask Jason what he calls these. They're not trie leaves! They're the things the
    trie leaves point to...
    """
    _cmd_id = 5
    structure = (
        ('request_id', sedes.big_endian_int),
        ('state_root', trie_root),
        ('prefix', sedes.binary),  # encoded in the same way trie nibbles are
    )


class Leaves(Command):
    """
    If the leaves for the requested prefix all fit into the max response size {prefix}
    will be the same as {GetLeaves.prefix}. If there were too many leaves then the largest
    prefix for which all nodes fit into the max response size will be included.
    """
    _cmd_id = 6
    structure = (
        ('request_id', sedes.big_endian_int),
        ('prefix', sedes.binary),  # encoded in the same way trie nibbles are

        # A list of (fullpath, leafrlp) pairs
        # TODO: blindly copied from Status.capabilities, is it correct?
        ('leaves', sedes.CountableList(sedes.List([sedes.binary, sedes.binary]))),

        # proves the returned prefix if it's different than the requested prefix
        ('proof', sedes.CountableList(sedes.binary)),
    )

    LeafType = List[Tuple[bytes, bytes]]


# not sure where to put this
class LeafResponse(NamedTuple):
    prefix: Nibbles
    leaves: Iterable[Tuple[Nibbles, bytes]]
    proof: Iterable[bytes]


# Requests


class GetLeafCountRequest(BaseRequest[BigEndianInt]):
    cmd_type = GetLeafCount
    response_type = LeafCount

    def __init__(self, request_id: int, state_root: Hash32, prefix: Nibbles) -> None:
        nibbles = encode_nibbles(prefix)
        self.command_payload = (
            request_id,
            state_root,
            nibbles,
        )


class GetNodeChunkRequest(BaseRequest[Tuple[int, Hash32, bytes]]):
    cmd_type = GetNodeChunk
    response_type = NodeChunk

    def __init__(self, request_id: int, state_root: Hash32, prefix: Nibbles) -> None:
        nibbles = cast(bytes, encode_nibbles(prefix))
        self.command_payload = (
            request_id,
            state_root,
            nibbles,
        )


class GetLeavesRequest(BaseRequest[Tuple[int, Hash32, bytes]]):
    cmd_type = GetLeaves
    response_type = Leaves

    def __init__(self, request_id: int, state_root: Hash32, prefix: Nibbles) -> None:
        nibbles = cast(bytes, encode_nibbles(prefix))
        self.command_payload = (
            request_id,
            state_root,
            nibbles,
        )


# Trackers


class GetLeafCountTracker(BasePerformanceTracker[GetLeafCountRequest, BigEndianInt]):
    def _get_request_size(self, request: GetLeafCountRequest) -> Optional[int]:
        return len(request.command_payload)

    def _get_result_size(self, result: BigEndianInt) -> int:
        return len(result)

    def _get_result_item_count(self, result: BigEndianInt) -> int:
        return len(result)


class GetNodeChunkTracker(BasePerformanceTracker[GetNodeChunkRequest, NodeChunk]):
    def _get_request_size(self, request: GetNodeChunkRequest) -> Optional[int]:
        return len(request.command_payload)

    def _get_result_size(self, result: NodeChunk) -> int:
        return 0

    def _get_result_item_count(self, result: NodeChunk) -> int:
        return 0


class GetLeavesTracker(BasePerformanceTracker[GetLeavesRequest, Leaves]):
    def _get_request_size(self, request: GetLeavesRequest) -> Optional[int]:
        return len(request.command_payload)

    def _get_result_size(self, result: Leaves) -> int:
        return 0

    def _get_result_item_count(self, result: Leaves) -> int:
        return 0


# Validators


class GetLeafCountValidator(BaseValidator[None]):
    def validate_result(self, response: None) -> None:
        return


def children_of(node_rlp: bytes) -> Tuple[Hash32, ...]:
    node_obj = decode_node(node_rlp)
    references, _leaves = _get_children(node_obj, depth=0)
    return tuple(node_hash for (_depth, node_hash) in references)


class GetNodeChunkValidator(BaseValidator[NodeChunk]):
    """
    Each node must be a direct child of a node which precedes it in the response.

    This does not fully validate the response. In order to do that we would need to know
    the expected hash of the root of the returned subtrie. Other parts of the code-base
    know the expected hash however it hasn't been threaded into here and keeping this
    class stateless sounds valuable. So, this only checks that the returned nodes form *a*
    valid sub-trie, and it's up to the requester to validate that that form the requested
    sub-trie (which they can do by checking the hash of the first node)
    """

    def validate_result(self, response: NodeChunk) -> None:
        """
        TODO: This does not check that the trie is in any way balanced, though maybe it
        should.
        """
        # TODO: response is a Dict, not a NodeChunk. Fixing this will take a while.

        if len(response['nodes']) == 0:  # type: ignore
            # the caller should figure this out
            return

        first, *rest = response['nodes']  # type: ignore
        expected_hashes = set(children_of(first))

        for node_rlp in rest:
            node_hash = keccak(node_rlp)
            if node_hash not in expected_hashes:
                raise Exception(f'Unexpected node: {node_rlp} hash: {node_hash.hex()}')

            expected_hashes.remove(node_hash)
            expected_hashes.update(children_of(node_rlp))


class GetLeavesValidator(BaseValidator[LeafResponse]):
    def validate_result(self, response: LeafResponse) -> None:
        return


# Normalizers


class GetNodeChunkNormalizer(BaseNormalizer[None, NodeChunk]):
    @staticmethod
    def normalize_result(message: None) -> NodeChunk:
        return message


class GetLeavesNormalizer(BaseNormalizer[Leaves, LeafResponse]):
    @staticmethod
    def normalize_result(message: Leaves) -> LeafResponse:
        return LeafResponse(
            prefix=decode_nibbles(message['prefix']),
            leaves=[
                (decode_nibbles(path), leaf_rlp) for path, leaf_rlp in message['leaves']
            ],
            proof=message['proof'],
        )


# Exchanges


class GetLeafCountExchange(BaseExchange[BigEndianInt, BigEndianInt, BigEndianInt]):
    _normalizer = NoopNormalizer[BigEndianInt]()
    request_class = GetLeafCountRequest
    tracker_class = GetLeafCountTracker

    async def __call__(self, state_root: Hash32, prefix: Nibbles,  # type: ignore
                       timeout: float = None) -> None:
        validator = GetLeafCountValidator()
        request = self.request_class(
            request_id=1,  # this will be replaced later
            state_root=state_root,
            prefix=prefix,
        )

        return await self.get_result(
            request,
            self._normalizer,
            validator,
            noop_payload_validator,
            timeout,
        )


class GetNodeChunkExchange(BaseExchange[Tuple[int, Hash32, bytes], None, NodeChunk]):
    _normalizer = GetNodeChunkNormalizer()
    request_class = GetNodeChunkRequest
    tracker_class = GetNodeChunkTracker

    async def __call__(self, state_root: Hash32, prefix: Nibbles,  # type: ignore
                       timeout: float = None) -> NodeChunk:
        # TODO: in order to validate we need to know the expected hash. However, all we
        # have here is the state_root and the requested prefix. How do we get the hash of
        # the node at said prefix? Maybe all responses should include a proof from the
        # root?
        validator = GetNodeChunkValidator()
        request = self.request_class(
            request_id=1,  # this will be replaced later
            state_root=state_root,
            prefix=prefix,
        )

        return await self.get_result(
            request,
            self._normalizer,
            validator,
            noop_payload_validator,
            timeout,
        )


class GetLeavesExchange(BaseExchange[Tuple[int, Hash32, bytes], None, Leaves]):
    _normalizer = GetLeavesNormalizer()
    request_class = GetLeavesRequest
    tracker_class = GetLeavesTracker

    async def __call__(self, state_root: Hash32, prefix: Nibbles,  # type: ignore
                       timeout: float = None) -> Leaves:
        validator = GetLeavesValidator()
        request = self.request_class(
            request_id=1,  # this will be replaced later
            state_root=state_root,
            prefix=prefix,
        )

        return await self.get_result(
            request,
            self._normalizer,
            validator,
            noop_payload_validator,
            timeout,
        )


# Handlers


class AFirehoseExchangeHandler(BaseExchangeHandler):
    _exchange_config = {
        'get_leaf_count': GetLeafCountExchange,
        'get_node_chunk': GetNodeChunkExchange,
        'get_leaves': GetLeavesExchange,
    }

    get_leaf_count: GetLeafCountExchange
    get_node_chunk: GetNodeChunkExchange
    get_leaves: GetLeavesExchange


class FirehoseExchangeManager(PeerSubscriber, BaseService):
    """
    A hacky minimal replacement for ExchangeManager
    """

    msg_queue_maxsize = 100

    def __init__(self, peer: BasePeer, msg_types: FrozenSet[Type[Command]]) -> None:
        self.peer = peer
        self.cancel_token = peer.cancel_token
        super().__init__(token=self.cancel_token)
        self._msg_types = msg_types

        self.pending_requests: Dict[int, asyncio.Future] = {}
        self.counter = itertools.count()

    @property
    def subscription_msg_types(self):
        return self._msg_types

    async def launch_service(self) -> None:
        self.peer.run_daemon(self)
        await self.events.started.wait()

    async def get_result(
            self,
            request: BaseRequest,
            normalizer: BaseNormalizer,
            validate_result: Callable,
            payload_validator: Callable,
            tracker: BasePerformanceTracker,
            timeout: float = None) -> Any:
        # TODO: play with a timeout_bucket? record a blacklist? use the tracker?

        # TODO: maybe the request_id belongs in a header? Wrap this message in another?
        request_id = next(self.counter)
        assert isinstance(request.command_payload, tuple)
        request.command_payload = (request_id,) + request.command_payload[1:]

        assert request_id not in self.pending_requests
        self.pending_requests[request_id] = future = asyncio.Future()

        self.peer.sub_proto.send_request(request)
        # self.logger.debug(f'sent message: {request_id}')

        try:
            payload = await self.wait(future, timeout=timeout)
        except TimeoutError as err:
            del self.pending_requests[request_id]
            tracker.record_timeout()
            raise

        payload_validator(payload)
        result = normalizer.normalize_result(payload)
        validate_result(result)

        return result

    async def _run(self) -> None:
        with self.subscribe_peer(self.peer):
            while self.is_operational:
                _peer, cmd, msg = await self.wait(self.msg_queue.get())

                request_id = msg['request_id']

                # TODO: log and continue, don't crash!
                assert request_id in self.pending_requests

                # self.logger.debug(f'received message: {request_id}')
                future = self.pending_requests[request_id]
                future.set_result(msg)

    def deregister_peer(self, peer: BasePeer) -> None:
        # notify all the pending futures, the response is never coming
        for future in self.pending_requests.values():
            # TODO: check that this unblocks the wait()ing coros
            future.set_exception(PeerConnectionLost(''))
        # TODO: stop the service, our work here is done


class FirehoseExchangeHandler:
    """
    A (hopefully simpler) replacement for BaseExchangeHandler
    """
    def __init__(self, peer: BasePeer) -> None:
        self.peer = peer

        self.manager = FirehoseExchangeManager(peer, frozenset((
            GetLeafCountExchange.response_cmd_type,
            GetNodeChunkExchange.response_cmd_type,
            GetLeavesExchange.response_cmd_type,
        )))

        self.get_leaf_count = GetLeafCountExchange(self.manager)
        self.get_node_chunk = GetNodeChunkExchange(self.manager)
        self.get_leaves = GetLeavesExchange(self.manager)

#        self.get_leaf_count = GetLeafCountExchange(
#            ExchangeManager(peer, GetLeafCountExchange.response_cmd_type, peer.cancel_token)
#        )
#        self.get_node_chunk = GetNodeChunkExchange(
#            ExchangeManager(peer, GetNodeChunkExchange.response_cmd_type, peer.cancel_token)
#        )

    def get_stats(self) -> Dict[str, str]:
        """
        Part of the BaseExchangeHandler interface
        """
        raise NotImplemented()


# Protocol


class FirehoseProtocol(Protocol):
    name = 'firehose'
    version = 1
    _commands = (
        Status,
        GetLeafCount, LeafCount,
        GetNodeChunk, NodeChunk,
        GetLeaves, Leaves,
    )
    cmd_length = 7

    peer: 'FirehosePeer'

    def send_handshake(self) -> None:
        resp = {
            'protocol_version': self.version,
        }
        cmd = Status(self.cmd_id_offset, self.snappy_support)
        self.transport.send(*cmd.encode(resp))

    def send_leaf_count(self, request_id: int, leaf_count: int) -> None:
        cmd = LeafCount(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode((request_id, leaf_count))
        self.transport.send(header, body)

    def send_node_chunk(self, request_id: int, nodes: List[bytes]) -> None:
        # TODO: what type should nodes have?
        cmd = NodeChunk(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode((request_id, nodes))
        self.transport.send(header, body)

    def send_leaves(self, request_id: int,
                    prefix: Nibbles,
                    leaves: Leaves.LeafType,
                    proof: Iterable[bytes]) -> None:
        # TODO: the reverse of this method is GetLeavesNormalizer, make that more explicit
        cmd = Leaves(self.cmd_id_offset, self.snappy_support)
        nibbles = cast(bytes, encode_nibbles(prefix))
        encoded_leaves = [
            (encode_nibbles(path), leaf_rlp)
            for path, leaf_rlp in leaves
        ]
        encoded_proof = [rlp.encode(node) for node in proof]
        header, body = cmd.encode((request_id, nibbles, encoded_leaves, encoded_proof))
        self.transport.send(header, body)


# Peer


class FirehosePeer(BasePeer):
    supported_sub_protocols = (FirehoseProtocol,)
    sub_proto: FirehoseProtocol = None

    _requests: FirehoseExchangeHandler = None

    @property
    def requests(self) -> FirehoseExchangeHandler:
        if self._requests is None:
            self._requests = FirehoseExchangeHandler(self)
        return self._requests

    async def send_sub_proto_handshake(self) -> None:
        self.sub_proto.send_handshake()

    async def process_sub_proto_handshake(self, cmd: Command, msg: PayloadType) -> None:
        if not isinstance(cmd, Status):
            await self.disconnect(DisconnectReason.subprotocol_error)
            raise HandshakeFailure(f"Expected a status msg, got {cmd}, disconnecting")

        # TODO: fail if the remote is using the wrong version


class FirehosePeerFactory(BasePeerFactory):
    peer_class = FirehosePeer


class FirehosePeerPool(BasePeerPool):
    peer_factory_class = FirehosePeerFactory


# Servers


def common_prefix(left: Nibbles, right: Nibbles) -> Nibbles:
    # TODO: test this more thoroughly

    result = []
    for left_nibble, right_nibble in zip(left, right):
        if left_nibble != right_nibble:
            return tuple(result)
        result.append(left_nibble)
    return min(left, right)  # TODO: more verbose code might be easier to read


def get_leaves_response(db: ChainDB,
                        state_root: Hash32,
                        nibbles: Nibbles,
                        max_leaves: int) -> Dict:
    """A pure method without any asyncio is much easier to use pdb with"""

    # Iterate over all leaves, rooted at the given prefix
    iterator = iterate_leaves(db, state_root, nibbles)

    # Don't return more than {max_leaves} leaves
    sliced = list(itertools.islice(iterator, max_leaves + 1))

    # If we ran out of leaves then {sliced} contains every leaf in the requested prefix.
    # It can be returned directly.
    if len(sliced) <= max_leaves:
        return {
            'prefix': nibbles,
            'leaves': sliced,
            'proof': tuple(),
        }

    # Return the largest subset of {sliced} which constitutes a full bucket.
    # This is the shortest prefix which not all nodes have in common.
    # e.g. if all nodes in {sliced} have a prefix of (1,) then we can't fit the entire
    #      (1,) bucket into a single response. Try to return a longer prefix instead.

    assert len(sliced) > 2  # {max_leaves} should be 2 or larger

    # It's cool that this is a single pass but there has to be a more understandable
    # way to do this.
    # - The first version of this had a bug which slipped through initial testing

    first, *rest = sliced
    first_path = first[0]

    # once we have all the nodes for a given bucket they're added here
    leaves_to_return = [first]

    # while we're still trying to fill the next-larger bucket nodes are queued in here
    leaves_which_might_fit = []

    # the bucket which we're trying to fill
    last_prefix = None
    current_prefix = None

    for path, leaf in rest:
        if current_prefix is None:
            # this is the second node
            current_prefix = common_prefix(first_path, path)

            # special logic in case the first node is the only thing which fits into the
            # requested {max_leaves}.
            assert len(current_prefix) > 0
            last_prefix = first_path[:len(current_prefix)+1]

            leaves_which_might_fit.append((path, leaf))
            continue

        diff = common_prefix(first_path, path)
        if diff == current_prefix:
            # this leaf is part of the bucket we're trying to fill
            leaves_which_might_fit.append((path, leaf))
            continue

        # this leaf is part of a new bucket!
        leaves_to_return.extend(leaves_which_might_fit)  # add this bucket to our output
        leaves_which_might_fit = []
        leaves_which_might_fit.append((path, leaf))  # add this leaf to the new bucket
        last_prefix = current_prefix
        current_prefix = diff
        continue

    # TODO: what if last_prefix was never set?

    # we're returning a smaller prefix than requested so we also need to prove the prefix
    # that we're returning

    trie = HexaryTrie(db.db, root_hash=state_root)
    trie_root = trie.get_node(trie.root_hash)
    proof = trie._get_proof(trie_root, last_prefix)

    # proof[-1] is the node at {last_prefix}, the caller can rederive it
    proof = proof[:-1]

    # proof also includes a proof of the node at {nibbles}. If the caller is asking for
    # {nibbles} they presumibly already have this part of the proof:
    proof = proof[len(nibbles):]

    assert len(leaves_to_return) <= max_leaves
    return {
        'prefix': last_prefix,
        'leaves': leaves_to_return,
        'proof': proof,
    }


class LeavesCache:
    """
    TODO: This needs to be async, blocking the entire server for this isn't reasonable.

    Assumes all requests are for the same state root.
    Also assumes that MAX_LEAVES never changes
    """

    def __init__(self, db_path: str):
        self.db = plyvel.DB(
            db_path,
            create_if_missing=True,
            error_if_exists=False,
            max_open_files=256,
        )

    @staticmethod
    def _next_bucket(previous: Nibbles) -> Nibbles:
        result = list(previous)
        result[-1] += 1
        return tuple(result)

    def save_response(self,
                      prefix: Nibbles,
                      leaves: Leaves.LeafType,
                      proof: Iterable[bytes]) -> None:
        assert len(prefix)

        key = bytes(prefix)
        assert self.db.get(key) is None
        self.db.put(key, pickle.dumps((leaves, proof)))

    def lookup_prefix(self, prefix: Nibbles) -> Optional[Dict]:
        assert len(prefix)

        it = self.db.iterator(
            start=bytes(prefix),
            stop=bytes(self._next_bucket(prefix)),
            include_start=True,
            include_stop=False,
        )
        try:
            key, value = next(it)
        except StopIteration:
            return None
        it.close()

        leaves, proof = pickle.loads(value)

        return {
            'prefix': tuple(key),
            'leaves': leaves,
            'proof': proof,
        }


class FirehoseRequestServer(BaseRequestServer):
    subscription_msg_types: FrozenSet[Type[Command]] = frozenset({
        GetLeafCount, GetNodeChunk, GetLeaves,
    })

    # TODO: think about what a good value for this would be
    MAX_LEAVES = 1000  # the maximum number of leaves to return in each request

    def __init__(self, db: ChainDB,
                 peer_pool: FirehosePeerPool,
                 cache: LeavesCache = None,
                 token: CancelToken = None) -> None:
        super().__init__(peer_pool, token)
        self.db = db

        self.cache = cache

    async def _handle_msg(self, base_peer: BasePeer, cmd: Command,
                          msg: _DecodedMsgType) -> None:
        peer = cast(FirehosePeer, base_peer)

        if isinstance(cmd, GetLeafCount):
            msg = cast(Dict[str, Any], msg)
            request_id = cast(int, msg['request_id'])
            state_root = cast(Hash32, msg['state_root'])  # TODO: is this cast correct?
            prefix = cast(bytes, msg['prefix'])
            await self.handle_get_leaf_count(peer, request_id, state_root, prefix)
        elif isinstance(cmd, GetNodeChunk):
            msg = cast(Dict[str, Any], msg)
            request_id = cast(int, msg['request_id'])
            state_root = cast(Hash32, msg['state_root'])
            prefix = cast(bytes, msg['prefix'])
            await self.handle_get_node_chunk(peer, request_id, state_root, prefix)
        elif isinstance(cmd, GetLeaves):
            msg = cast(Dict[str, Any], msg)
            request_id = cast(int, msg['request_id'])
            state_root = cast(Hash32, msg['state_root'])
            prefix = cast(bytes, msg['prefix'])
            await self.handle_get_leaves(peer, request_id, state_root, prefix)

    async def handle_get_leaf_count(self, peer: FirehosePeer, request_id: int,
                                    state_root: Hash32, prefix: bytes) -> None:
        # TODO: add some kind of validation here?
        nibbles = decode_nibbles(prefix)

        count = 0
        for _ in iterate_leaves(self.db, state_root, nibbles):
            # TODO: time out if this operation takes too long
            count += 1

        peer.sub_proto.send_leaf_count(request_id, leaf_count=count)

    async def handle_get_node_chunk(self, peer: FirehosePeer, request_id: int,
                                    state_root: Hash32, prefix: bytes) -> None:
        # TODO: You don't technically need to return every node. The nodes at the last
        # level (including "" (0x80) to represent NULL nodes) are sufficient for
        # reconstructing the higher levels. Over the course of an entire sync not sending
        # the extra nodes could lead to a reasonable savings.

        # How many nodes should be returned?
        # - each branch is 512 bytes (16*32)
        # - there are 256 nodes at depth 2
        # - a response with all depth-2 nodes consumes 128kb
        # - all the nodes up to depth-2 -> 136.5kb

        # TODO: figure out how to test this
        # TODO: ensure this doesn't take too long to generate, or return too much data
        # TODO: validate that "prefix" has the correct format

        nibbles = decode_nibbles(prefix)
        iterator = iterate_node_chunk(
            self.db, state_root, sub_trie=nibbles, target_depth=2,
        )
        nodes = [node.rlp for (_prefix, node) in iterator]
        peer.sub_proto.send_node_chunk(request_id, nodes)

    async def handle_get_leaves(self, peer: FirehosePeer, request_id: int,
                                state_root: Hash32, prefix: bytes) -> None:
        start = time.perf_counter()

        cache_lookup_time = None
        generate_response_time = None
        cache_save_time = None

        nibbles = decode_nibbles(prefix)

        response = None

        if self.cache and len(nibbles):
            t_cache_start = time.perf_counter()
            response = self.cache.lookup_prefix(nibbles)
            cache_lookup_time = time.perf_counter() - t_cache_start

        if response is None:
            cache_lookup_time = None  # don't fill the log with cache misses
            t_response_start = time.perf_counter()
            response = get_leaves_response(
                self.db, state_root, nibbles, self.MAX_LEAVES
            )
            t_response_end = time.perf_counter()
            generate_response_time = t_response_end - t_response_start
            # TODO: this means requests for () don't have their responses saved
            if self.cache and len(nibbles):
                self.cache.save_response(
                    response['prefix'], response['leaves'], response['proof']
                )
                cache_save_time = time.perf_counter() - t_response_end

        end = time.perf_counter()

        log_line = f'request={nibbles} response={response["prefix"]} total={end-start:.2}'

        if cache_lookup_time:
            log_line += f' cache={cache_lookup_time:.2}'

        if generate_response_time:
            log_line += f' response={generate_response_time:.2}'

        if cache_save_time:
            log_line += f' save={cache_save_time:.2}'

        self.logger.info(log_line)

        peer.sub_proto.send_leaves(
            request_id,
            response['prefix'],
            response['leaves'],
            response['proof'],
        )


class FirehoseListener(BaseService):

    def __init__(self,
                 privkey: datatypes.PrivateKey,
                 port: int,
                 peer_pool: FirehosePeerPool,
                 token: CancelToken):
        super().__init__(token)

        self.privkey = privkey
        self.port = port
        self.peer_pool = peer_pool

    async def _start_tcp_listener(self) -> None:
        self._tcp_listener = await asyncio.start_server(
            self.receive_handshake,
            host='127.0.0.1',
            port=self.port,
        )

    async def _close_tcp_listener(self) -> None:
        if self._tcp_listener:
            self._tcp_listener.close()
            await self._tcp_listener.wait_closed()

    async def _run(self) -> None:
        await self._start_tcp_listener()
        self.logger.info(
            "Starting firehose listener: enode://%s@127.0.0.1:%s",
            self.privkey.public_key.to_hex()[2:],
            self.port,
        )
        try:
            await self.cancel_token.wait()
        finally:
            await self._close_tcp_listener()

    async def receive_handshake(
            self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        expected_exceptions = (
            TimeoutError,
            PeerConnectionLost,
            HandshakeFailure,
            asyncio.IncompleteReadError,
        )

        def cleanup_reader_and_writer() -> None:
            if not reader.at_eof():
                reader.feed_eof()
            writer.close()

        try:
            await self._receive_handshake(reader, writer)
        except expected_exceptions as e:
            self.logger.debug("Could not complete handshake: %s", e)
            cleanup_reader_and_writer()
        except OperationCancelled:
            pass
        except Exception as e:
            self.logger.exception("Unexpected error handling handshake")
            cleanup_reader_and_writer()

    async def _receive_handshake(
            self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        # TODO: this belongs somewhere in the p2p module
        msg = await self.wait(
            reader.read(ENCRYPTED_AUTH_MSG_LEN),
            timeout=REPLY_TIMEOUT)

        peername = writer.get_extra_info("peername")
        if peername is None:
            socket = writer.get_extra_info("socket")
            sockname = writer.get_extra_info("sockname")
            raise HandshakeFailure(
                "Received incoming connection with no remote information:"
                f"socket={repr(socket)}  sockname={sockname}"
            )

        ip, socket, *_ = peername
        remote_address = Address(ip, socket)
        self.logger.debug("Receiving handshake from %s", remote_address)

        try:
            ephem_pubkey, initiator_nonce, initiator_pubkey = decode_authentication(
                msg, self.privkey)
        except DecryptionError as non_eip8_err:
            # Try to decode as EIP8
            got_eip8 = True
            msg_size = big_endian_to_int(msg[:2])
            remaining_bytes = msg_size - ENCRYPTED_AUTH_MSG_LEN + 2
            msg += await self.wait(
                reader.read(remaining_bytes),
                timeout=REPLY_TIMEOUT)
            try:
                ephem_pubkey, initiator_nonce, initiator_pubkey = decode_authentication(
                    msg, self.privkey)
            except DecryptionError as eip8_err:
                raise HandshakeFailure(
                    f"Failed to decrypt both EIP8 handshake: {eip8_err}  and "
                    f"non-EIP8 handshake: {non_eip8_err}"
                )
        else:
            got_eip8 = False

        initiator_remote = Node(initiator_pubkey, remote_address)
        responder = HandshakeResponder(initiator_remote, self.privkey, got_eip8, self.cancel_token)

        responder_nonce = secrets.token_bytes(HASH_LEN)
        auth_ack_msg = responder.create_auth_ack_message(responder_nonce)
        auth_ack_ciphertext = responder.encrypt_auth_ack_message(auth_ack_msg)

        # Use the `writer` to send the reply to the remote
        writer.write(auth_ack_ciphertext)
        await self.wait(writer.drain())

        # Call `HandshakeResponder.derive_shared_secrets()` and use return values to create `Peer`
        aes_secret, mac_secret, egress_mac, ingress_mac = responder.derive_secrets(
            initiator_nonce=initiator_nonce,
            responder_nonce=responder_nonce,
            remote_ephemeral_pubkey=ephem_pubkey,
            auth_init_ciphertext=msg,
            auth_ack_ciphertext=auth_ack_ciphertext
        )

        transport = Transport(
            remote=initiator_remote,
            private_key=self.privkey,
            reader=reader,
            writer=writer,
            aes_secret=aes_secret,
            mac_secret=mac_secret,
            egress_mac=egress_mac,
            ingress_mac=ingress_mac,
        )

        # Create and register peer in peer_pool
        peer = self.peer_pool.get_peer_factory().create_peer(
            transport=transport,
            inbound=True,
        )

        # TODO: maybe check whether the peer pool is full?

        # We use self.wait() here as a workaround for
        # https://github.com/ethereum/py-evm/issues/670.
        await self.wait(self.do_handshake(peer))

    async def do_handshake(self, peer: FirehosePeer) -> None:
        await peer.do_p2p_handshake()
        await peer.do_sub_proto_handshake()
        await self.peer_pool.start_peer(peer)


class MiniPeerPool(BaseService):
    def __init__(self,
                 privkey: datatypes.PrivateKey,
                 cancel_token: CancelToken) -> None:
        super().__init__(cancel_token)

        self.privkey = privkey
        self.connected_nodes: Dict[Node, BasePeer] = {}

        self._subscribers: List[PeerSubscriber] = list()

    def get_peer_factory(self) -> BasePeerFactory:
        return FirehosePeerFactory(
            privkey=self.privkey,
            context=None,
            event_bus=None,
            token=self.cancel_token,
        )

    async def start_peer(self, peer: FirehosePeer) -> None:
        self.run_child_service(peer)
        await self.wait(peer.events.started.wait(), timeout=1)
        if peer.is_operational:
            self._add_peer(peer)
        else:
            self.logger.debug("%s was cancelled immediately, not adding to pool", peer)

        try:
            await self.wait(
                peer.boot_manager.events.finished.wait(),
                timeout=DEFAULT_PEER_BOOT_TIMEOUT,
            )
        except TimeoutError as err:
            self.logger.debug('Timout waiting for peer to boot: %s', err)
            await peer.disconnect(DisconnectReason.timeout)
            return
        except HandshakeFailure as err:
            raise

        if not peer.is_operational:
            self.logger.debug('%s disconnected during boot-up', peer)

    async def _run(self) -> None:
        await self.cancel_token.wait()

    def _add_peer(self, peer: FirehosePeer) -> None:
        self.logger.info('Adding %s to pool', peer)
        self.connected_nodes[peer.remote] = peer
        peer.add_finished_callback(lambda _: self._peer_finished(peer))
        for subscriber in self._subscribers:
            subscriber.register_peer(peer)
            peer.add_subscriber(subscriber)

    def _peer_finished(self, peer: FirehosePeer) -> None:
        self.connected_nodes.pop(peer.remote)
        for subscriber in self._subscribers:
            subscriber.deregister_peer(peer)

    def subscribe(self, subscriber: PeerSubscriber) -> None:
        self._subscribers.append(subscriber)
        for peer in self.connected_nodes.values():
            subscriber.register_peer(peer)
            peer.add_subscriber(subscriber)

    def unsubscribe(self, subscriber: PeerSubscriber) -> None:
        self._subscribers.remove(subscriber)
        for peer in self.connected_nodes.values():
            peer.remove_subscriber(subscriber)


async def connect_to(pubkey: datatypes.PublicKey, host: str, port: int) -> FirehosePeer:
    # taken from test_server_incoming_connection
    use_eip8 = False
    token = CancelToken('initiator')

    # TODO: connect_to(Node) is probably the better interface
    receiver = Node(pubkey, Address(host, udp_port=port, tcp_port=port))

    privkey = ecies.generate_privkey()

    initiator = HandshakeInitiator(receiver, privkey, use_eip8, token)
    reader, writer = await initiator.connect()
    aes_secret, mac_secret, egress_mac, ingress_mac = await _handshake(
        initiator, reader, writer, token)

    transport = Transport(
        remote=receiver,
        private_key=initiator.privkey,
        reader=reader,
        writer=writer,
        aes_secret=aes_secret,
        mac_secret=mac_secret,
        egress_mac=egress_mac,
        ingress_mac=ingress_mac,
    )
    initiator_peer = FirehosePeer(
        transport=transport,
        context=None,
        inbound=True,
        token=token,
    )

    await initiator_peer.do_p2p_handshake()
    await initiator_peer.do_sub_proto_handshake()

    return initiator_peer


# Syncer


class MemoTrieNode:
    """
    Profiling showed that decode_node and get_node_type took up ~4% of processing time
    during get_leaves syncing, this is a stop-gap to test whether not calling decode_node
    will really speed things up. The next step is to refactor the syncer to not call
    make_node, it only really cares about (keccak, rlp).
    """
    def __init__(self, node_rlp) -> None:
        self.rlp = node_rlp

        self._type = None
        self._node = None
        self._hash = None
        self._kind = None

    @property
    def kind(self):
        if not self._kind:
            self._kind = NodeKind(self.node_type)
        return self._kind

    @property
    def keccak(self):
        if not self._hash:
            self._hash = keccak(self.rlp)
        return self._hash

    @property
    def obj(self):
        if not self._node:
            self._node = decode_node(self.rlp)
        return self._node

    @property
    def node_type(self):
        if not self._type:
            self._type = get_node_type(self.obj)
        return self._type

    @property
    def path_rest(self) -> Nibbles:
        # careful: this doesn't make any sense for branches
        return cast(Nibbles, extract_key(self.obj))


def make_node(node_rlp: bytes) -> TrieNode:
    # return MemoTrieNode(node_rlp)

    node = decode_node(node_rlp)
    node_type = get_node_type(node)
    node_hash = keccak(node_rlp)

    return TrieNode(
        kind=NodeKind(node_type),
        rlp=node_rlp,
        obj=node,
        keccak=node_hash
    )


async def simple_get_chunk_sync(db: AtomicDB, peer: FirehosePeer, state_root: Hash32) -> None:
    """
    Sends GetNodeChunk requests to the remote peer until we've finished syncing. This is
    not much faster than Fast Sync but it should still be some improvement: we can
    batch requests more intelligently.

    TODO: pipelining requests should improve performance by a lot

    Rough strategy:
    - You have a queue of unexplored hashes, which starts as just the state_root
    - fire off a GetNodeChunk request for the first unexplored hash
    - validate that the response was rooted in the sub-trie you asked for
    - insert the received nodes into the local database
      - add the un-returned children of these nodes to the unexplored hash queue
    - repeat until there are no more unexplored hashes

    A productionized strategy might use a priority queue rather than a queue, where
    priority is the key of the node. I think this would minimize the size of the queue and
    also allow a basic progress indicator.
    """

    unexplored_hashes: queue.Queue[Tuple[Nibbles, Hash32]] = queue.Queue()
    unexplored_hashes.put(((), state_root))

    while not unexplored_hashes.empty():
        path, requested_node_hash = unexplored_hashes.get()
        result = await peer.requests.get_node_chunk(
            state_root, path, timeout=1,
        )
        # TODO: result is a Dict, not a Command, this is likely what normalizers are for
        nodes = result['nodes']  # type: ignore
        assert len(nodes)  # TODO: do something smarter here

        expected_nodes: Dict[Hash32, Nibbles] = {requested_node_hash: path}

        for node_rlp in nodes:
            node = make_node(node_rlp)

            # if we weren't expecting this node the remote sent us bad data
            assert node.keccak in expected_nodes

            db.set(node.keccak, node.rlp)
            node_path = expected_nodes.pop(node.keccak)

            if node.kind == NodeKind.LEAF:
                continue

            children = _get_children_with_nibbles(node, node_path)
            for child_path, child_hash in children:
                if child_hash == b'':
                    continue
                expected_nodes[child_hash] = child_path

        for node_hash, node_path in expected_nodes.items():
            unexplored_hashes.put((node_path, node_hash))


class ParallelSimpleChunkSync:
    """
    A syncer which takes advantage of request pipelining
    """
    def __init__(self, db: AtomicDB, peer: FirehosePeer, state_root: Hash32, concurrency):
        self.db = db
        self.peer = peer
        self.state_root = state_root

        self.unexplored_hashes = queue.Queue()
        self.unexplored_hashes.put(((), state_root))

        self.max_concurrency = concurrency

        # If a fetch is running other coros will block until it is not running
        self.fetch_not_running = asyncio.Event()
        self.fetch_not_running.set()

    async def run(self):
        coros = [self._run() for _ in range(self.max_concurrency)]
        await asyncio.gather(*coros)

    async def _run(self):
        while True:
            while True:
                try:
                    path, node_hash = self.unexplored_hashes.get_nowait()
                    break
                except queue.Empty:
                    if self.fetch_not_running.is_set():
                        # the queue is empty and no other coros are working, we're done!
                        return
                    # the queue is empty but other coros are working, wait until they're done
                    await self.fetch_not_running.wait()

            self.fetch_not_running.clear()  # the fetch has begun!
            try:
                await self.fetch(path, node_hash)
            finally:
                self.fetch_not_running.set()

    async def fetch(self, path: Nibbles, requested_node_hash: Hash32):
        result = await self.peer.requests.get_node_chunk(
            self.state_root,
            path,
            timeout=1,
        )

        nodes = result['nodes']  # type: ignore
        assert len(nodes)  # TODO: do something smarter here

        expected_nodes: Dict[Hash32, Nibbles] = {requested_node_hash: path}

        for node_rlp in nodes:
            node = make_node(node_rlp)

            # if we weren't expecting this node the remote sent us bad data
            assert node.keccak in expected_nodes

            self.db.set(node.keccak, node.rlp)
            node_path = expected_nodes.pop(node.keccak)

            if node.kind == NodeKind.LEAF:
                continue

            children = _get_children_with_nibbles(node, node_path)
            for child_path, child_hash in children:
                if child_hash == b'':
                    continue
                expected_nodes[child_hash] = child_path

        for node_hash, node_path in expected_nodes.items():
            self.unexplored_hashes.put((node_path, node_hash))


def make_leaf(path: Nibbles, payload: bytes) -> TrieNode:

    ### key = compute_leaf_key(path)
    term = add_nibbles_terminator(path)
    key = encode_nibbles(term)  # %4.58 of runtime
    ###

    node_rlp = rlp.codec.encode_raw([key, payload])
    node = make_node(node_rlp)
    return node


def make_extension(path: Nibbles, ref: Hash32) -> TrieNode:
    key = compute_extension_key(path)
    node_rlp = rlp.codec.encode_raw([key, ref])
    node = make_node(node_rlp)
    return node


def nodes_from_leaves(prefix: Nibbles,
                      leaves: Leaves.LeafType) -> Iterable[Tuple[bytes, TrieNode]]:
    """
    Given a response from the server which only includes the leaves generate all the nodes
    rooted at the returned prefix.

    Returns them in an insertable order (highest to lowest)

    TODO: I believe these special cases can be avoided by first truncating {prefix} from
    the beginning of all these paths, then inserting them into the trie and reading out
    every node from it.
    """

    if len(leaves) == 1:
        # this requires a special case, the below code would create a trie which consists
        # of nothing but the one leaf. We need a trie *rooted at the given prefix* which
        # contains the one leaf. It can be built manually:

        original_path, original_payload = leaves[0]

        assert common_prefix(original_path, prefix) == prefix
        truncated_path = original_path[len(prefix):]

        node = make_leaf(truncated_path, original_payload)
        yield (prefix, node)
        return

    fake_db = dict()
    fake_trie = HexaryTrie(fake_db)

    for path, leaf in leaves:
        assert is_subtree(prefix, path)
        key = nibbles_to_bytes(path)
        fake_trie.set(key, leaf)

    # TODO: accept an expected hash for the node at prefix and verify it here

    nodes_to_return = iterate_trie(fake_db, fake_trie.root_hash, prefix)
    first_path, first_node = next(nodes_to_return)

    if first_path != prefix:
        assert is_subtree(prefix, first_path)  # sanity check

        # Alt: the first node in the trie is an extension node which points to {first_path}
        # instead of ignoring the first node we could inspect it and modify it if it
        # doesn't point to {prefix}. I think that would enable merging this branch with
        # the len(leaves) == 1 branch

        """
        The syncer only asked for this node because it definitely existed. The caller
        usually even knows what hash it's expecting. If we're about to return a node which
        isn't the node at the requested path, then the requested node is an extension node
        wich should be built and returned
        """

        path_rest = first_path[len(prefix):]
        node = make_extension(path_rest, first_node.keccak)
        yield (prefix, node)

    yield first_path, first_node
    yield from nodes_to_return


NodeGen = Iterable[Tuple[Nibbles, TrieNode]]


def nibbles_prepended(nibbles: Nibbles, nodes: NodeGen) -> NodeGen:
    for path, leaf in nodes:
        yield (
            nibbles + path,
            leaf
        )


def n_nibbles_dropped(n: int, nodes):
    return [
        (path[n:], leaf) for path, leaf in nodes
    ]


def fast_nodes_from_leaves(prefix: Nibbles,
                           leaves: Iterable[Leaves.LeafType]) -> Iterable[Tuple[Nibbles, TrieNode]]:

    # 1. truncate the prefixes
    truncated = n_nibbles_dropped(len(prefix), leaves)

    # 2. call the inner method
    builder = trie_builder(truncated)

    # 3. append the prefix back onto the returned nodes
    yield from nibbles_prepended(prefix, builder)


def trie_builder(leaves: Iterable[Leaves.LeafType]) -> Iterable[Tuple[Nibbles, TrieNode]]:
    """
    Go from a bunch of leaves to a bunch of insertable trie nodes.

    TODO: This code is horrible and I'm embarrassed to even commit it with the intent to
    refactor it soon.

    TODO: Much of the complication comes from the fact that while the uptimate caller
    cares about every subnode, each recursive call only cares about the first node which
    is returned. Maybe those two things should be returned via different mechanisms.
    """
    if len(leaves) == 1:
        # the base-case, build the leaf and send it back up
        # what path do we give the leaf? What does it mean to return a path?
        path, payload = first(leaves)
        yield ((), make_leaf(path, payload))
        return

    first_nibble_of_path = compose(first, first)
    leaf_groups = groupby(first_nibble_of_path, leaves)

    if len(leaf_groups) == 1:
        # build an extension node

        group_nibble = first(leaf_groups)
        group = leaf_groups[group_nibble]

        paths = map(first, group)
        max_common_prefix = reduce(common_prefix, paths)

        dropped = n_nibbles_dropped(len(max_common_prefix), group)
        subtrie = trie_builder(dropped)
        prepended = nibbles_prepended(max_common_prefix, subtrie)

        first_path, first_node = next(prepended)

        extension = make_extension(max_common_prefix, first_node.keccak)
        yield (), extension
        yield first_path, first_node
        yield from prepended
        return

    # build a branch node!
    node_groups = dict()
    for nibble, leaf_group in leaf_groups.items():
        dropped = n_nibbles_dropped(1, leaf_group)
        subtrie = trie_builder(dropped)
        node_groups[nibble] = nibbles_prepended((nibble,), subtrie)

    first_nodes = []

    branch = [b''] * 17
    for i in range(16):
        try:
            ith_group = node_groups[i]
        except KeyError:
            continue

        first_path, first_node = next(ith_group)
        first_nodes.append((first_path, first_node))

        branch[i] = first_node.keccak

    branch_rlp = rlp.codec.encode_raw(branch)
    branch_node = make_node(branch_rlp)

    yield (), branch_node

    for node in first_nodes:
        yield node

    for nibble, node_group in node_groups.items():
        yield from node_group


def get_trie_node_at(trie: HexaryTrie, prefix: Nibbles) -> bytes:  # TODO: it's not bytes
    encoded = nibbles_to_bytes(prefix)
    return trie.get_proof(encoded)[-1]


def check_proof(requested_hash: Hash32, proof: Iterable[bytes]) -> Set[Hash32]:
    """
    Verifies that the proof is a valid branch of the trie starting at {requested_hash}.

    Returns the set of nodes this proof proves.

    TODO: Also verify that the proof proves the specified prefix
    """
    expected_hashes = {requested_hash}
    for node_rlp in proof:
        assert keccak(node_rlp) in expected_hashes
        expected_hashes = {*children_of(node_rlp)}
    return expected_hashes


class TrieProgress:
    "Remembers completed work and reports on how much of the trie has been completed"

    def __init__(self) -> None:
        self.subtries = [None] * 16

        self.completed_subtries = 0
        self.completed = False

    def complete_bucket(self, nibbles: Nibbles) -> bool:
        """
        Returns whether this subtrie has been completed
        """
        assert not self.completed
        assert self.completed_subtries < 16

        if len(nibbles) == 0:
            self.completed = True
            return True

        first_nibble, *rest = nibbles

        if self.subtries[first_nibble] is None:
            subtrie = self.subtries[first_nibble] = TrieProgress()
        else:
            subtrie = self.subtries[first_nibble]

        subtrie_completed = subtrie.complete_bucket(rest)
        if subtrie_completed:
            self.completed_subtries += 1
            if self.completed_subtries == 16:
                self.completed = True
                self.subtries = [None] * 16  # save some space
                return True

        return False

    def progress(self) -> float:
        if self.completed:
            return 1.0

        progress = 0.0
        for subtrie in self.subtries:
            if subtrie is None:
                continue

            subprogress = subtrie.progress()
            progress += subprogress * (1/16)

        return progress


async def simple_get_leaves_sync(db: AtomicDB,
                                 peer: FirehosePeer,
                                 state_root: Hash32) -> None:
    """
    Sends GetLeaves requests to the remote peer until it's received the entire trie.
    """

    # when it asks for a prefix and is given a smaller prefix:
    # 1. verifies that the given proof matches the requested item
    # 2. generates the nodes for the given leaves
    # 3. inserts everything into the database, from parent down
    # 4. remembers which hashes need to be recursed into

    # keeps track of all the unexplored hashes (starting w/ just the root)
    unexplored_hashes: queue.Queue[Tuple[Nibbles, Hash32]] = queue.Queue()
    unexplored_hashes.put(((), state_root))

    progress_tracker = TrieProgress()

    logger = logging.getLogger('leaf-sync')

    while not unexplored_hashes.empty():
        path, requested_node_hash = unexplored_hashes.get()
        expected_nodes: Dict[Hash32, Nibbles] = {requested_node_hash: path}

        t_start = time.perf_counter()

        result = await peer.requests.get_leaves(
            state_root, path, timeout=60,
        )

        t_req = time.perf_counter()

        progress_tracker.complete_bucket(result.prefix)
        progress = f'{progress_tracker.progress()*100:2.6}'
        now = datetime.now().isoformat()
        self.logger.info(f'[{now}] req: {path} res: {result.prefix} leaves: {len(result.leaves)} progress: {progress}')

        proof_nodes_to_insert: List[TrieNode] = []

        # 1. check the proof

        if result.prefix != path:
            # the bucket we asked for held too many leaves

            assert is_subtree(path, result.prefix)
            proven_hashes = check_proof(requested_node_hash, result.proof)

            # TODO: check that the first generated node is part of {proven_hashes}
            #       that check probably belongs in the response Validator

            # TODO: this logic is nearly identical to simple_get_chunk_sync. Extract a
            #       method?

            for proof_node_rlp in result.proof:
                proof_node = make_node(proof_node_rlp)

                assert proof_node.keccak in expected_nodes
                proof_node_path = expected_nodes.pop(proof_node.keccak)

                proof_nodes_to_insert.append(proof_node)

                # leaves can't prove anything
                assert proof_node.kind in (NodeKind.EXTENSION, NodeKind.BRANCH)

                children = _get_children_with_nibbles(proof_node, proof_node_path)
                for child_path, child_hash in children:
                    if child_hash == b'':
                        continue
                    expected_nodes[child_hash] = child_path

        nodes_to_insert = fast_nodes_from_leaves(result.prefix, result.leaves)

        # 2. check that the proof proves the leaves we were given

        t_first_leaf = time.perf_counter()
        first_node_path, first_node = next(nodes_to_insert)
        t_first_leaf_e = time.perf_counter()

        if result.prefix != path:
            hexified = first_node.keccak.hex()
            assert first_node.keccak in proven_hashes
        else:
            fnk = first_node.keccak.hex()
            rnk = requested_node_hash.hex()
            assert first_node.keccak == requested_node_hash

        assert first_node.keccak in expected_nodes
        expected_nodes.pop(first_node.keccak)

        t_rest_leaves = time.perf_counter()

        # 3. insert the proof into the database
        for trie_node in proof_nodes_to_insert:
            db.set(trie_node.keccak, trie_node.rlp)

        t_rest_leaves_e = time.perf_counter()

        # 4. insert all the generated nodes
        db.set(first_node.keccak, first_node.rlp)
        for _node_path, node in nodes_to_insert:
            db.set(node.keccak, node.rlp)

        # 5. recurse on the proven nodes which weren't returned
        for node_hash, node_path in expected_nodes.items():
            unexplored_hashes.put((node_path, node_hash))

        t_end = time.perf_counter()

        total_time = t_end - t_start
        server_time = t_req - t_start
        process_time = t_end - t_req
        first_leaf_time = t_first_leaf_e - t_first_leaf
        rest_leaves_time = t_rest_leaves_e - t_rest_leaves
        all_leaf_time = first_leaf_time + rest_leaves_time

        logger.info(
            f't: {total_time} s: {server_time} p: {process_time} l: {all_leaf_time}'
        )


class ParallelGetLeavesSync:

    logger = logging.getLogger('leaf-sync')

    def __init__(self, db: AtomicDB, peer: FirehosePeer, state_root: Hash32,
                 concurrency: int = 1, target: float = 1) -> None:
        self.db = db
        self.peer = peer
        self.state_root = state_root
        self.max_concurrency = concurrency
        self.target = target

        self.unexplored_hashes = queue.Queue()
        self.unexplored_hashes.put(((), state_root))

        self.running_fetches = 0
        self.fetch_finished = asyncio.Event()

        self.progress_tracker = TrieProgress()

    async def run(self):
        self.spawn_fetches()

        while self.running_fetches > 0:
            await self.fetch_finished.wait()

            if self.progress_tracker.progress() > self.target:
                self.logger.info('sync reached target, exiting')
                break

            self.fetch_finished.clear()
            self.spawn_fetches()

        # wait for all the outstanding fetches to finish
        while self.running_fetches > 0:
            await self.fetch_finished.wait()
            self.fetch_finished.clear()

    def spawn_fetches(self):
        while self.running_fetches < self.max_concurrency:
            try:
                self.spawn_fetch()
            except queue.Empty:
                return

    def spawn_fetch(self):
        def fetch_finished(_fut):
            self.running_fetches -= 1
            self.fetch_finished.set()

        next_fetch = self.unexplored_hashes.get_nowait()
        self.running_fetches += 1
        # self.logger.debug(f'[{self.running_fetches}] spawning fetch: {next_fetch[0]}')
        task = asyncio.create_task(self.fetch(*next_fetch))
        task.add_done_callback(fetch_finished)

    def check_proof(self, path: Nibbles, expected_hash: Hash32, result):
        assert result.prefix != path

        # result must be proving something we know how to verify the proof of
        assert is_subtree(path, result.prefix)

        expected_nodes: Dict[Hash32, Nibbles] = {expected_hash: path}

        for proof_node_rlp in result.proof:
            proof_node = make_node(proof_node_rlp)

            assert proof_node.keccak in expected_nodes
            proof_node_path = expected_nodes.pop(proof_node.keccak)

            # we've verified that this node belongs in the database
            self.db.set(proof_node.keccak, proof_node.rlp)

            # leaves can't prove anything
            assert proof_node.kind in (NodeKind.EXTENSION, NodeKind.BRANCH)

            children = _get_children_with_nibbles(proof_node, proof_node_path)
            for child_path, child_hash in children:
                if child_hash == b'':
                    continue
                expected_nodes[child_hash] = child_path

        return expected_nodes

    def process_no_proof(self, expected_hash, result):
        nodes_to_insert = fast_nodes_from_leaves(result.prefix, result.leaves)

        first_node_path, first_node = next(nodes_to_insert)
        assert first_node.keccak == expected_hash

        self.db.set(first_node.keccak, first_node.rlp)
        for _path, node in nodes_to_insert:
            self.db.set(node.keccak, node.rlp)

    async def fetch(self, path: Nibbles, requested_node_hash: Hash32):
        """
        Something which might be simpler to code, but maybe not simpler to profile, would
        be to structure this as a generator which returned a stream of 
           REF: (path, hash) | NODE: (keccak, rlp)
        we could collect the stream and insert all the nodes and remember all the
        references
        """

        t_start = time.perf_counter()
        result = await self.peer.requests.get_leaves(
            self.state_root, path, timeout=60,
        )
        t_end = time.perf_counter()
        server_time = t_end - t_start

        self.progress_tracker.complete_bucket(result.prefix)
        progress = f'{self.progress_tracker.progress():.6}'

        now = datetime.now().isoformat()
        self.logger.info(f'time={now} req={path} res={result.prefix} progress={progress}')

        # If there's no proof it's easy, just generate all the nodes and insert them
        if result.prefix == path:
            self.process_no_proof(requested_node_hash, result)
            # self.logger.debug(f'[{self.running_fetches}] fetch finishing: {path}')
            return

        expected_nodes = self.check_proof(path, requested_node_hash, result)
        nodes_to_insert = fast_nodes_from_leaves(result.prefix, result.leaves)

        # 2. check that the proof proves the leaves we were given
        first_node_path, first_node = next(nodes_to_insert)
        assert first_node.keccak in expected_nodes
        expected_nodes.pop(first_node.keccak)

        # 3. insert all the generated nodes
        self.db.set(first_node.keccak, first_node.rlp)
        for _node_path, node in nodes_to_insert:
            self.db.set(node.keccak, node.rlp)

        # 4. recurse on the proven nodes which weren't returned
        for node_hash, node_path in expected_nodes.items():
            self.unexplored_hashes.put((node_path, node_hash))

        # self.logger.debug(f'[{self.running_fetches}] fetch finishing: {path}')
