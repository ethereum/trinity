import asyncio
import random
import time

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
from trie.utils.nibbles import bytes_to_nibbles, nibbles_to_bytes, decode_nibbles

from eth_hash.auto import keccak

from p2p import ecies
from p2p.kademlia import Node, Address
from p2p.auth import HandshakeInitiator, _handshake
from p2p.tools.factories import get_open_port
from p2p.tools.paragon.helpers import (
    get_directly_linked_peers,
)
from p2p.transport import Transport

from trinity.protocol import firehose

import rlp


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
async def test_get_leaves(linked_peers):
    # TODO: the peer pool and server should also be broken out into a fixture
    alice, bob, cancel_token = linked_peers

    # 1. Create a database with some nodes

    random.seed(8000)
    trie = make_random_trie(100)

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

    # 3. Make a request!

    request_server.MAX_LEAVES = 1000
    result = await bob.requests.get_leaves(
        trie.root_hash,
        prefix=(),
        timeout=1,
    )
    assert len(result.leaves) == 100
    assert len(result.proof) == 0  # everything was returned, no proof necessary

    request_server.MAX_LEAVES = 10
    result = await bob.requests.get_leaves(
        trie.root_hash,
        prefix=(),
        timeout=1,
    )
    assert result.prefix == (0,)
    assert len(result.leaves) == 9
    assert len(result.proof) == 1  # the top-level node should have been returned


@pytest.mark.asyncio
async def test_parallel_get_leaves_sync(linked_peers):
    alice, bob, cancel_token = linked_peers

    # 1. Create a database with many nodes

    random.seed(5000)
    trie = make_random_trie(100)

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

    request_server.MAX_LEAVES = 1000
    syncer = firehose.ParallelGetLeavesSync(
        bob_atomic, bob, state_root=trie.root_hash, concurrency=5,
    )
    await syncer.run()

    # 4. Check that the final result matches the original database

    assert len(trie.db) == len(db)
    for key in trie.db.keys():
        assert key in db
        assert db[key] == trie.db[key]




def run_trie_builder_comparison(seed, count, debug=False):
    random.seed(seed)
    trie = make_random_trie(count)

    atomic = AtomicDB(trie.db)
    chaindb = ChainDB(atomic)

    orig_nodes = list(firehose.iterate_trie(chaindb, trie.root_hash))
    leaves = list(firehose.iterate_leaves(chaindb, trie.root_hash))
    gen_nodes = list(firehose.trie_builder(leaves))

    if debug:
        print('\n===nodes===\n')
        for path, node in orig_nodes:
            print(bytes(path).hex(), node)

        print('\n===leaves===\n')
        for path, node in leaves:
            print(nibbles_to_bytes(path).hex(), node)

        print('\n===gen===\n')
        for path, node in gen_nodes:
            print(bytes(path).hex(), node)

    for path, node in orig_nodes:
        assert (path, node) in gen_nodes


@pytest.mark.parametrize("seed,count", [
    (8000, 2),  # the two nodes are parented by a branch node
    (8006, 2),  # the two leaves are parented by an extension node and a branch
])
def test_trie_builder_regression(seed, count):
    run_trie_builder_comparison(seed, count)


def test_trie_builder_many():
    for i in range(1000):
        run_trie_builder_comparison(i, 2)

    for i in range(100):
        run_trie_builder_comparison(i, 5)

    for i in range(10):
        run_trie_builder_comparison(i, 100)

    run_trie_builder_comparison(9000, 1000)


def test_single_leaf_response():
    # regression test, this failed during a sync test
    # (7, ) has more than 10 leaves
    # (7, 1) only has one leaf

    random.seed(5000)
    trie = make_random_trie(1000)

    atomic = AtomicDB(trie.db)
    chaindb = ChainDB(atomic)

    result = firehose.get_leaves_response(
        chaindb, trie.root_hash, nibbles=(7,), max_leaves=10,
    )

    generated_nodes = firehose.nodes_from_leaves(result['prefix'], result['leaves'])
    leaf = next(generated_nodes)
    assert leaf[0] == (7, 0)
    assert leaf[1].keccak in trie.db

    with pytest.raises(StopIteration):
        next(generated_nodes)


def test_nodes_from_leaves_creates_extension():
    # regression test, this failed during a sync test
    # if the returned leaves all belong to a deeper path than the requested one then an
    # extension node must be returned

    random.seed(5000)
    trie = make_random_trie(1000)

    atomic = AtomicDB(trie.db)
    chaindb = ChainDB(atomic)

    result = firehose.get_leaves_response(
        chaindb, trie.root_hash, nibbles=(13, 14), max_leaves=10,
    )

    generated_nodes = list(firehose.nodes_from_leaves(result['prefix'], result['leaves']))
    generated_hashes = [node.keccak for _, node in generated_nodes]

    for generated_hash in generated_hashes:
        assert generated_hash in trie.db

    extension_hash = bytes.fromhex(
        'c6061101906cd8f0e831b8103268b949af1b30842618e44eb51c834b23887827'
    )
    assert extension_hash in generated_hashes


def test_get_leaves_response():
    random.seed(8000)
    trie = make_random_trie(100)

    atomic = AtomicDB(trie.db)
    chaindb = ChainDB(atomic)

    # if we want to fetch more than exist.. we get all the leaves!
    result = firehose.get_leaves_response(
        chaindb, trie.root_hash, nibbles=(), max_leaves=1000,
    )
    assert len(result['leaves']) <= 100
    assert result['prefix'] == ()
    assert result['proof'] == ()

    result = firehose.get_leaves_response(
        chaindb, trie.root_hash, nibbles=(), max_leaves=10,
    )
    assert len(result['leaves']) <= 10

    assert firehose.is_subtree((), result['prefix'])
    for path, leaf in result['leaves']:
        assert firehose.is_subtree(result['prefix'], path)

    assert len(result['proof']) == len(result['prefix']) - 0

    # with random.seed(8000) there are 9 nodes with a prefix of (0,)

    result = firehose.get_leaves_response(
        chaindb, trie.root_hash, nibbles=(), max_leaves=9,
    )
    assert result['prefix'] == (0,)
    assert len(result['leaves']) == 9
    assert len(result['proof']) == len(result['prefix']) - 0

    assert firehose.is_subtree((), result['prefix'])
    for path, leaf in result['leaves']:
        assert firehose.is_subtree(result['prefix'], path)

    # with random.seed(8000) there are 2 node with a prefix of (0, 1)
    result = firehose.get_leaves_response(
        chaindb, trie.root_hash, nibbles=(), max_leaves=8,
    )
    assert result['prefix'] == (0, 1)
    assert len(result['leaves']) == 2
    assert len(result['proof']) == len(result['prefix']) - 0

    # assert that we can understand the response

    nodes = firehose.nodes_from_leaves(result['prefix'], result['leaves'])
    first_node_path, first_node = next(nodes)
    assert first_node_path == result['prefix']
    expected_first_node = firehose.get_trie_node_at(trie, (0, 1))
    assert first_node.obj == expected_first_node

    # even if we ask for a lot of nodes we can only fill one bucket
    # a better response format would include the other buckets we're also able to fill!
    result = firehose.get_leaves_response(
        chaindb, trie.root_hash, nibbles=(), max_leaves=90,
    )
    assert result['prefix'] == (0,)
    assert len(result['leaves']) == 9
    assert len(result['proof']) == len(result['prefix']) - 0

    # Make sure the proof is of the correct format

    result = firehose.get_leaves_response(
        chaindb, trie.root_hash, nibbles=(), max_leaves=8,
    )
    proof = result['proof']
    assert len(proof) == 2

    # TODO: the tests are made complicated by how many stages there are:
    #       unencoded response -> encoded response -> decoded response
    potential_first_node_hashes = firehose.check_proof(
        trie.root_hash, [rlp.encode(node) for node in result['proof']]
    )

    # the first element of the proof proves (0,)
    root = trie.get_node(trie.root_hash)
    assert root == proof[0]

    # the second element of the proof proves (0, 1)
    node_at_0 = trie.get_node(root[0])
    assert node_at_0 == proof[1]

    # the first node we derive from the provided leaves should be the node at (0, 1)
    nodes = firehose.nodes_from_leaves(result['prefix'], result['leaves'])
    first_node_path, first_node = next(nodes)
    assert node_at_0[1] == first_node.keccak

    assert first_node.keccak in potential_first_node_hashes


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



@pytest.mark.asyncio
async def test_parallel_get_chunk_sync(linked_peers):
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

    syncer = firehose.ParallelSimpleChunkSync(
        bob_atomic, bob, state_root=trie.root_hash, concurrency=2,
    )
    await syncer.run()

    # 4. Check that the final result matches the original database

    assert len(trie.db) == len(db)
    for key in trie.db.keys():
        assert key in db
        assert db[key] == trie.db[key]


@pytest.fixture
async def firehose_listener():
    cancel_token = CancelToken('listener')
    privkey = ecies.generate_privkey()

    peer_pool = firehose.MiniPeerPool(privkey, cancel_token)
    port = get_open_port()
    listener = firehose.FirehoseListener(
        privkey,
        port,
        peer_pool,
        cancel_token
    )

    asyncio.ensure_future(peer_pool.run())
    await peer_pool.events.started.wait()

    asyncio.ensure_future(listener.run())
    await listener.events.started.wait()

    await asyncio.sleep(0.05)  # give it just a little longer to call bind()

    yield listener

    cancel_token.trigger()
    await asyncio.wait_for(listener.events.finished.wait(), timeout=1)
    await asyncio.wait_for(peer_pool.events.finished.wait(), timeout=1)


@pytest.mark.asyncio
async def test_firehose_listener(firehose_listener):
    peer_pool = firehose_listener.peer_pool
    port = firehose_listener.port

    # 1. connect a peer to our pool

    pubkey = firehose_listener.privkey.public_key
    assert len(peer_pool.connected_nodes) == 0
    peer = await firehose.connect_to(pubkey, '127.0.0.1', port)

    # 2. check that the peer pool has our peer

    while len(peer_pool.connected_nodes) == 0:
        await asyncio.sleep(0.01)
    assert len(peer_pool.connected_nodes) == 1


@pytest.mark.asyncio
async def test_listener_server(firehose_listener):
    """
    Check that the request server subscribes to the pool
    """
    random.seed(6000)

    peer_pool = firehose_listener.peer_pool
    port = firehose_listener.port
    token = firehose_listener.cancel_token

    trie = make_random_trie(10)

    atomic = AtomicDB(trie.db)
    chaindb = ChainDB(atomic)

    server = firehose.FirehoseRequestServer(
        db=chaindb,
        peer_pool=peer_pool,
        token=token
    )
    asyncio.ensure_future(server.run())
    await asyncio.sleep(0)

    # 1. connect a peer to our pool

    pubkey = firehose_listener.privkey.public_key
    assert len(peer_pool.connected_nodes) == 0
    peer = await firehose.connect_to(pubkey, '127.0.0.1', port)

    asyncio.ensure_future(peer.run())
    await asyncio.wait_for(peer.events.started.wait(), timeout=1)

    # 2. check that the peer pool has our peer

    while len(peer_pool.connected_nodes) == 0:
        await asyncio.sleep(0.01)
    assert len(peer_pool.connected_nodes) == 1

    # 3. Can the peer make a request and get a response back?

    state_root = trie.root_hash
    result = await peer.requests.get_node_chunk(
        state_root, prefix=(0,), timeout=1
    )
    assert len(result['nodes']) == 1

    # Make sure everything continues to work when the peer leaves and another peer joins

    peer.cancel_token.trigger()
    await asyncio.wait_for(peer.events.finished.wait(), timeout=1)

    peer2 = await firehose.connect_to(pubkey, '127.0.0.1', port)
    asyncio.ensure_future(peer2.run())
    await asyncio.wait_for(peer2.events.started.wait(), timeout=1)

    result = await peer2.requests.get_node_chunk(
        state_root, prefix=(0,), timeout=1
    )
    assert len(result['nodes']) == 1

    peer2.cancel_token.trigger()
    await asyncio.wait_for(peer2.events.finished.wait(), timeout=1)

    while len(peer_pool.connected_nodes) != 0:
        await asyncio.sleep(0.01)
    assert len(peer_pool.connected_nodes) == 0


@pytest.mark.asyncio
async def test_simple_get_leaves_sync(linked_peers, tmpdir):
    alice, bob, cancel_token = linked_peers

    # 1. Create a database with many nodes

    random.seed(5000)
    trie = make_random_trie(1000)

    atomic = AtomicDB(trie.db)
    chaindb = ChainDB(atomic)

    # 2. Sit a request server atop it

    cache = firehose.LeavesCache(str(tmpdir))
    alice_peer_pool = MockPeerPool([alice])
    request_server = firehose.FirehoseRequestServer(
        db=chaindb,
        peer_pool=alice_peer_pool,
        cache=cache,
        token=cancel_token,
    )

    asyncio.ensure_future(request_server.run())

    await asyncio.sleep(0)

    request_server.MAX_LEAVES=10000  # all leaves are returned in single request
    # 3. Start a syncer and watch it go

    db = dict()
    bob_atomic = AtomicDB(db)

    await firehose.simple_get_leaves_sync(
        bob_atomic, bob, state_root=trie.root_hash
    )

    # 4. Check that the final result matches the original database

    assert len(trie.db) == len(db)
    for key in trie.db.keys():
        assert key in db
        assert db[key] == trie.db[key]

    request_server.MAX_LEAVES=100  # each bucket of len == 1 fits into one request
    # 3. Start a syncer and watch it go

    db = dict()
    bob_atomic = AtomicDB(db)

    await firehose.simple_get_leaves_sync(
        bob_atomic, bob, state_root=trie.root_hash
    )

    # 4. Check that the final result matches the original database

    assert len(trie.db) == len(db)
    for key in trie.db.keys():
        assert key in db
        assert db[key] == trie.db[key]

    request_server.MAX_LEAVES=10  # each request only fits a len(2) bucket
    # 3. Start a syncer and watch it go

    db = dict()
    bob_atomic = AtomicDB(db)

    await firehose.simple_get_leaves_sync(
        bob_atomic, bob, state_root=trie.root_hash
    )

    # 4. Check that the final result matches the original database

    assert len(trie.db) == len(db)
    for key in trie.db.keys():
        assert key in db
        assert db[key] == trie.db[key]


def test_progress():
    # TODO: test this more thoroughly 

    progress = firehose.TrieProgress()
    progress.complete_bucket((0, ))
    assert progress.progress() == (1/16)

    progress = firehose.TrieProgress()
    for i in range(16):
        progress.complete_bucket((0, i))
    assert progress.progress() == (1/16)


def test_next_nibble():
    func = firehose.LeavesCache._next_bucket

    assert func((0,)) == (1,)
    assert func((15,)) == (16,)
    assert func((1, 4, 15,)) == (1, 4, 16,)

