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

    chaindb = FakeAsyncChainDB(chaindb_fresh.db)

    alice_peer_pool = MockPeerPool([alice])
    request_server = firehose.FirehoseRequestServer(
        db=chaindb,
        peer_pool=alice_peer_pool,
        token=cancel_token,
    )

    asyncio.ensure_future(request_server.run())

    # 1. find the state root so we know what to ask for
    head = await chaindb.coro_get_canonical_head()
    state_root = head.state_root

    await bob.requests.get_state_data(
        state_root,
        prefix=(0,),
        timeout=1,
    )
    await request_server.cancel()
