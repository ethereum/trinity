import asyncio
import pytest

from cancel_token import CancelToken

from p2p import ecies

from trinity.protocol import firehose

from tests.core.integration_test_helpers import (
    FakeAsyncChainDB,
)

from p2p.tools.paragon.helpers import (
    get_directly_linked_peers,
)

from trinity.sync.full.hexary_trie import trie_iterator


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
        prefix=(0,),
        timeout=1,
    )

    leaf_count = 0
    for _ in trie_iterator(chaindb, state_root):
        leaf_count += 1

    assert result['leaf_count'] == leaf_count

    await request_server.cancel()
