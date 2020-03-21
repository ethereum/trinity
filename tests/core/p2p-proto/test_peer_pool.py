import asyncio
import pytest

from p2p.tools.paragon import (
    ParagonContext,
    ParagonPeerPool,
)
from p2p.tools.factories import (
    ParagonPeerPairFactory,
    PrivateKeyFactory,
)


@pytest.mark.asyncio
async def test_peer_removed_after_disconnect():
    alice_pk = PrivateKeyFactory()
    async with ParagonPeerPairFactory(alice_private_key=alice_pk) as (alice, bob):
        peer_pool = ParagonPeerPool(
            privkey=alice_pk,
            context=ParagonContext(),
        )
        assert len(peer_pool) == 0
        peer_pool._add_peer(bob, tuple())
        assert len(peer_pool) == 1

        await bob.cancel()
        assert len(peer_pool) == 0


@pytest.mark.asyncio
async def test_peer_removed_when_unresponsive():
    async with ParagonPeerPairFactory() as (peer, remote):
        peer_transport = peer.connection.get_multiplexer().get_transport()
        peer_transport.idle_timeout = 0.2
        # This is necessary to trigger another call to read() on peer's transport, using the
        # monkey-patched timeout.
        remote.p2p_api.send_ping()
        await asyncio.sleep(0.1)

        pool = ParagonPeerPool(privkey=PrivateKeyFactory(), context=ParagonContext())
        pool._add_peer(peer, tuple())
        assert len(pool) == 1

        # This is to force the remote to stop responding without closing the connection or sending
        # us a Disconnect msg, causing our transport's read() to timeout because it's been idle
        # too long. We cannot close the remote's transport here because that would cause an
        # IncompleteReadError on our (peer) side and it would cancel itself.
        remote_transport = remote.connection.get_multiplexer().get_transport()
        remote_transport.write = lambda data: None

        await asyncio.sleep(0.4)
        assert len(pool) == 0
