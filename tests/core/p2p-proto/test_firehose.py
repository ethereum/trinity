import asyncio
import random

import pytest
from hypothesis import (
    given,
    strategies as st,
)

from cancel_token import CancelToken
from eth.db.atomic import AtomicDB
from eth.db.chain import ChainDB
from trie.constants import BLANK_NODE_HASH
from trie.hexary import HexaryTrie
from trie.utils.nibbles import bytes_to_nibbles

from p2p import ecies
from p2p.tools.paragon.helpers import (
    get_directly_linked_peers,
)
from trinity.protocol import firehose


# Trie tests (move to a different file eventually)


# TODO: this doesn't give an even distribution of node keys
def many_nodes(min_nodes: int = 1, max_nodes: int = 100):
    node_key = st.binary(min_size=32, max_size=32)
    node_value = st.just(bytes(0x1))
    node = st.tuples(node_key, node_value)
    return st.lists(
        node, min_size=min_nodes, max_size=max_nodes,
        unique_by=lambda x: x[0]
    )


# TODO: this is by far the most expensive operation. Batch trie creation would be useful
def create_trie(nodes):
    db = dict()
    trie = HexaryTrie(db)

    for key, value in nodes:
        trie.set(key, value)

    return trie


@given(many_nodes(max_nodes=1000))
def test_trie_iteration(random_nodes):
    # create a trie with a bunch of leaves in random places
    trie = create_trie(random_nodes)
    assert trie.root_hash != BLANK_NODE_HASH

    atomic = AtomicDB(trie.db)
    chaindb = ChainDB(atomic)

    # every node is returned
    node_iterator = firehose.iterate_leaves(chaindb, trie.root_hash)
    node_count = sum(1 for _ in node_iterator)
    assert node_count == len(random_nodes)


# TODO: this is slow when the minimum node count is raised but currently it isn't a very
# thorough test (it never really tests counts above 20)
@given(many_nodes(max_nodes=1000))
def test_iterate_catches_all(random_nodes):
    trie = create_trie(random_nodes)
    atomic = AtomicDB(trie.db)
    chaindb = ChainDB(atomic)

    # nodes are returned in ascending order and with the correct keys
    sorted_random_nodes = sorted(random_nodes, key=lambda x: x[0])
    node_iterator = firehose.iterate_leaves(chaindb, trie.root_hash)

    for left, right in zip(sorted_random_nodes, node_iterator):
        left_path, left_value = left
        right_path, right_value = right
        assert bytes_to_nibbles(left_path) == right_path


def test_is_subtree():
    func = firehose.is_subtree
    assert func((), (0,))
    assert func((), ())
    assert func((0,), (0, 1))
    assert not func((0,), (1,))


def many_prefixes():
    indexes = st.integers(min_value=0, max_value=15)
    prefixes = st.lists(indexes, min_size=0, max_size=2)
    return prefixes


# TODO: hypothesis complains when min_nodes is bumped all the way up to 200
# Maybe generating random tries directly would be better?
@given(many_nodes(max_nodes=1000), many_prefixes())
def test_trie_iteration_from_random_start(random_nodes, prefix):
    nibbles = tuple(prefix)

    trie = create_trie(random_nodes)
    atomic = AtomicDB(trie.db)
    chaindb = ChainDB(atomic)

    within_prefix = sum([
        firehose.is_subtree(nibbles, bytes_to_nibbles(key))
        for key, value in random_nodes
    ])

    # Now that we have a random trie and a random prefix, try iterating just that prefix!
    returned_nodes = list(firehose.iterate_leaves(chaindb, trie.root_hash, nibbles))
    assert len(returned_nodes) == within_prefix


# the above tests use hypothesis which doesn't provide an even distribution
# the below tests use a much larger and balanced trie


def make_random_trie(leaf_count=1000):
    db = dict()
    trie = HexaryTrie(db, prune=True)

    for _ in range(leaf_count):
        key = bytes([random.getrandbits(8) for _ in range(32)])
        trie.set(key, bytes(0x1))

    return trie


@pytest.mark.parametrize("leaf_count", [1, 10, 100, 1000])
def test_can_iterate_all_nodes(leaf_count):
    random.seed(1000)  # make this test reproducible
    trie = make_random_trie(leaf_count)

    atomic = AtomicDB(trie.db)
    chaindb = ChainDB(atomic)

    all_nodes = firehose.iterate_node_chunk(chaindb, trie.root_hash, (), target_depth=100)
    assert len(list(all_nodes)) == len(trie.db)


@pytest.mark.parametrize('chunk', [(0, 1), (5,), (1, 4, 2)])
def test_get_node_chunk_respects_chunk(chunk):
    random.seed(2000)  # make this test reproducible

    trie = make_random_trie(leaf_count=2000)

    atomic = AtomicDB(trie.db)
    chaindb = ChainDB(atomic)

    # iterate over every node and count how many are part of the given chunk
    all_nodes = firehose.iterate_node_chunk(
        chaindb, trie.root_hash, (), target_depth=100
    )
    nodes_in_chunk = [
        (prefix, node) for (prefix, node) in all_nodes
        if firehose.is_subtree(chunk, prefix)
    ]

    # iterate over just the specified chunk
    iterated = list(firehose.iterate_node_chunk(
        chaindb, trie.root_hash, chunk, target_depth=100
    ))

    # you better get the same answer!
    assert len(iterated) == len(nodes_in_chunk)


def test_get_node_chunk_respects_depth():
    """
    TODO: this test is actually incorrect (and raising the target depth to 5 will
    cause a failure).

    There are two different meanings of depth:
    - The length of the node's path  (what this test uses)
    - The distance from the root node  (what iterate_node_chunk uses)

    These are identical notions when there are only branch nodes, but extension and leaf
    nodes both cause parts of the path to be "skipped" during traversal. I'm not sure
    which notion of distance is better so I'll leave the tests as-is for now.
    """
    random.seed(3000)  # make this test reproducible

    trie = make_random_trie(leaf_count=2000)

    atomic = AtomicDB(trie.db)
    chaindb = ChainDB(atomic)

    all_nodes = list(firehose.iterate_node_chunk(
        chaindb, trie.root_hash, (), target_depth=100
    ))

    for depth in range(4):
        until_depth = len(list(firehose.iterate_node_chunk(
            chaindb, trie.root_hash, (), target_depth=depth
        )))

        naive_answer = len([
            node for (prefix, node) in all_nodes
            if len(prefix) <= depth
        ])

        assert until_depth == naive_answer


# Firehose tests


class MockPeerPool(firehose.FirehosePeerPool):
    def __init__(self, peers) -> None:
        super().__init__(privkey=None, context=None)
        for peer in peers:
            self.connected_nodes[peer.remote] = peer


@pytest.fixture
async def linked_peers(request, event_loop):
    cancel_token = CancelToken('test_firehose')

    alice_factory = firehose.FirehosePeerFactory(
        privkey=ecies.generate_privkey(),
        context=None,
        token=cancel_token,
    )

    bob_factory = firehose.FirehosePeerFactory(
        privkey=ecies.generate_privkey(),
        context=None,
        token=cancel_token,
    )

    alice, bob = await get_directly_linked_peers(
        request, event_loop, alice_factory, bob_factory
    )

    yield alice, bob, cancel_token

    cancel_token.trigger()


@pytest.mark.asyncio
async def test_firehose(linked_peers):
    # TODO: consider using chaindb_1000, we're building a database with fake data
    alice, bob, cancel_token = linked_peers

    random.seed(4000)  # make the test deterministic
    trie = make_random_trie(2000)

    atomic = AtomicDB(trie.db)
    chaindb = ChainDB(atomic)

    alice_peer_pool = MockPeerPool([alice])
    request_server = firehose.FirehoseRequestServer(
        db=chaindb,
        peer_pool=alice_peer_pool,
        token=cancel_token,
    )

    asyncio.ensure_future(request_server.run())

    state_root = trie.root_hash

    result = await bob.requests.get_leaf_count(
        state_root,
        prefix=(),
        timeout=1,
    )

    leaf_count = 0
    for _ in firehose.iterate_leaves(chaindb, state_root):
        leaf_count += 1

    assert result['leaf_count'] == leaf_count

    # Also check that we can fetch the leaf count for a given bucket

    result = await bob.requests.get_leaf_count(
        state_root,
        prefix=(0,),
        timeout=1,
    )

    leaf_count = 0
    for path, _leaf in firehose.iterate_leaves(chaindb, state_root):
        if path[0] == 0:
            leaf_count += 1

    assert result['leaf_count'] == leaf_count

    # Check that we can extract a chunk of nodes

    result = await bob.requests.get_node_chunk(
        state_root,
        prefix=(0,),
        timeout=1,
    )

    assert len(result['nodes']) == 122  # TODO: check that this number is correct

    await request_server.cancel()


@pytest.mark.asyncio
async def test_get_chunk_sync(linked_peers):
    alice, bob, cancel_token = linked_peers

    # 1. Create a database with many nodes

    random.seed(5000)
    trie = make_random_trie(1000)

    atomic = AtomicDB(trie.db)
    chaindb = ChainDB(atomic)

    # 2. Sit a request server atop it

    alice_peer_pool = MockPeerPool([alice])
    request_server = firehose.FirehoseRequestServer(
        db=chaindb,
        peer_pool=alice_peer_pool,
        token=cancel_token,
    )

    asyncio.ensure_future(request_server.run())

    await asyncio.sleep(0)

    # 3. Start a syncer and watch it go

    db = dict()
    bob_atomic = AtomicDB(db)

    await firehose.simple_get_chunk_sync(
        bob_atomic, bob, state_root=trie.root_hash
    )

    # 4. Check that the final result matches the original database

    assert len(trie.db) == len(db)
    for key in trie.db.keys():
        assert key in db
        assert db[key] == trie.db[key]
