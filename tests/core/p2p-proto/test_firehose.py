import asyncio

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

from tests.core.integration_test_helpers import (
    FakeAsyncChainDB,
)


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
    node_iterator = firehose.iterate_trie(chaindb, trie.root_hash)
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
    node_iterator = firehose.iterate_trie(chaindb, trie.root_hash)

    for left, right in zip(sorted_random_nodes, node_iterator):
        left_path, left_value = left
        right_path, right_value = right
        assert bytes_to_nibbles(left_path) == right_path


def test_is_nibbles_within_prefix():
    func = firehose.is_nibbles_within_prefix
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
        firehose.is_nibbles_within_prefix(nibbles, bytes_to_nibbles(key))
        for key, value in random_nodes
    ])

    # Now that we have a random trie and a random prefix, try iterating just that prefix!
    returned_nodes = list(firehose.iterate_trie(chaindb, trie.root_hash, nibbles))
    assert len(returned_nodes) == within_prefix


# Firehose tests


class MockPeerPool(firehose.FirehosePeerPool):
    def __init__(self, peers) -> None:
        super().__init__(privkey=None, context=None)
        for peer in peers:
            self.connected_nodes[peer.remote] = peer


@pytest.mark.asyncio
async def test_firehose(request, event_loop, chaindb_fresh):
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

    asyncchaindb = FakeAsyncChainDB(chaindb_fresh.db)
    chaindb = asyncchaindb.db

    alice_peer_pool = MockPeerPool([alice])
    request_server = firehose.FirehoseRequestServer(
        db=chaindb,
        peer_pool=alice_peer_pool,
        token=cancel_token,
    )

    asyncio.ensure_future(request_server.run())

    head = asyncchaindb.get_canonical_head()
    state_root = head.state_root

    result = await bob.requests.get_leaf_count(
        state_root,
        prefix=(),
        timeout=1,
    )

    leaf_count = 0
    for _ in firehose.iterate_trie(chaindb, state_root):
        leaf_count += 1

    assert result['leaf_count'] == leaf_count

    # Also check that we can fetch the leaf count for a given bucket

    result = await bob.requests.get_leaf_count(
        state_root,
        prefix=(0,),
        timeout=1,
    )

    leaf_count = 0
    for path, _leaf in firehose.iterate_trie(chaindb, state_root):
        if path[0] == 0:
            leaf_count += 1

    assert result['leaf_count'] == leaf_count

    await request_server.cancel()
