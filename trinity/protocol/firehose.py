import enum
from typing import (
    Any,
    cast,
    Dict,
    FrozenSet,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Type,
)

from cancel_token import CancelToken

from eth.db.chain import ChainDB
from eth.rlp.sedes import trie_root

from eth_hash.auto import keccak

from eth_utils import (
    to_tuple,
)

from eth_typing import (
    Hash32,
)

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

from trie.utils.nibbles import (
    encode_nibbles,
    decode_nibbles,
)

from trie.utils.nodes import (
    get_common_prefix_length,
    decode_node,
    extract_key,
    get_node_type,
)

from p2p.exceptions import HandshakeFailure
from p2p.p2p_proto import DisconnectReason
from p2p.peer import BasePeer, BasePeerFactory
from p2p.peer_pool import BasePeerPool
from p2p.protocol import (
    BaseRequest,
    Command,
    _DecodedMsgType,
    PayloadType,
    Protocol,
)

from trinity.protocol.common.exchanges import BaseExchange
from trinity.protocol.common.handlers import BaseExchangeHandler
from trinity.protocol.common.normalizers import BaseNormalizer, NoopNormalizer
from trinity.protocol.common.servers import BaseRequestServer
from trinity.protocol.common.trackers import BasePerformanceTracker
from trinity.protocol.common.validators import BaseValidator, noop_payload_validator

from trinity.sync.full.hexary_trie import _get_children


# Trie Utils


Nibbles = Tuple[int, ...]


class NodeKind(enum.Enum):
    BLANK = NODE_TYPE_BLANK
    LEAF = NODE_TYPE_LEAF
    EXTENSION = NODE_TYPE_EXTENSION
    BRANCH = NODE_TYPE_BRANCH


class Node(NamedTuple):
    kind: NodeKind
    rlp: bytes
    obj: List[bytes]  # this type is wrong but mypy doesn't support recursive types
    keccak: bytes

    def __str__(self) -> str:
        return f"Node(kind={self.kind.name} hash={self.keccak.hex()})"

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
def _get_children_with_nibbles(node: Node, prefix: Nibbles) -> Iterable[Tuple[Nibbles, Hash32]]:
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


def _get_node(db: ChainDB, node_hash: Hash32) -> Node:
    if len(node_hash) < 32:
        node_rlp = node_hash
    else:
        node_rlp = db.get(node_hash)

    node = decode_node(node_rlp)
    node_type = get_node_type(node)

    return Node(kind=NodeKind(node_type), rlp=node_rlp, obj=node, keccak=node_hash)


def _iterate_trie(db: ChainDB,
                  node: Node,  # the node we should look at
                  sub_trie: Nibbles,  # which sub_trie to return nodes from
                  prefix: Nibbles,  # our current path in the trie
                  ) -> Iterable[Tuple[Nibbles, Node]]:

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
                 sub_trie: Nibbles = ()) -> Iterable[Tuple[Nibbles, Node]]:

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
                        node: Node,
                        sub_trie: Nibbles,
                        prefix: Nibbles,
                        target_depth: int) -> Iterable[Tuple[Nibbles, Node]]:

    def recur(new_depth: int) -> Iterable[Tuple[Nibbles, Node]]:
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
                       target_depth: int) -> Iterable[Tuple[Nibbles, Node]]:
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


# Validators


class GetLeafCountValidator(BaseValidator[None]):
    def validate_result(self, response: None) -> None:
        return


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
        print(f'response: {type(response)}')

        def children_of(node_rlp: bytes) -> Tuple[Hash32, ...]:
            node_obj = decode_node(node_rlp)
            references, _leaves = _get_children(node_obj, depth=0)
            return tuple(node_hash for (_depth, node_hash) in references)

        # TODO: response is a Dict, not a NodeChunk. Fixing this will take a while.
        first, *rest = response['nodes']  # type: ignore
        expected_hashes = set(children_of(first))

        for node_rlp in rest:
            node_hash = keccak(node_rlp)
            if node_hash not in expected_hashes:
                raise Exception(f'Unexpected node: {node_rlp} hash: {node_hash.hex()}')

            expected_hashes.remove(node_hash)
            expected_hashes.update(children_of(node_rlp))


# Normalizers


class GetNodeChunkNormalizer(BaseNormalizer[None, NodeChunk]):
    @staticmethod
    def normalize_result(message: None) -> NodeChunk:
        return message


# Exchanges


class GetLeafCountExchange(BaseExchange[BigEndianInt, BigEndianInt, BigEndianInt]):
    _normalizer = NoopNormalizer[BigEndianInt]()
    request_class = GetLeafCountRequest
    tracker_class = GetLeafCountTracker

    async def __call__(self, state_root: Hash32, prefix: Nibbles,  # type: ignore
                       timeout: float = None) -> None:
        validator = GetLeafCountValidator()
        request = self.request_class(
            request_id=1,
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
            request_id=1,
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


class FirehoseExchangeHandler(BaseExchangeHandler):
    _exchange_config = {
        'get_leaf_count': GetLeafCountExchange,
        'get_node_chunk': GetNodeChunkExchange,
    }

    get_leaf_count: GetLeafCountExchange
    get_node_chunk: GetNodeChunkExchange


# Protocol


class FirehoseProtocol(Protocol):
    name = 'firehose'
    version = 1
    _commands = (
        Status,
        GetLeafCount, LeafCount,
        GetNodeChunk, NodeChunk,
    )
    cmd_length = 5

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


class FirehoseRequestServer(BaseRequestServer):
    subscription_msg_types: FrozenSet[Type[Command]] = frozenset({
        GetLeafCount, GetNodeChunk,
    })

    def __init__(self, db: ChainDB,
                 peer_pool: FirehosePeerPool, token: CancelToken = None) -> None:
        super().__init__(peer_pool, token)
        self.db = db

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
