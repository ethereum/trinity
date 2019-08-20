import asyncio

import pytest

from p2p.tools.factories import ParagonPeerPairFactory
from p2p.p2p_proto import Ping, Pong


@pytest.mark.asyncio
async def test_connection_factory_with_ParagonPeer():
    async with ParagonPeerPairFactory() as (alice, bob):
        got_ping = asyncio.Event()
        got_pong = asyncio.Event()

        async def handle_ping(msg):
            got_ping.set()
            bob.base_protocol.send_pong()

        async def handle_pong(msg):
            got_pong.set()

        alice.add_command_handler(Pong, handle_pong)
        bob.add_command_handler(Ping, handle_ping)

        alice.base_protocol.send_ping()

        await asyncio.wait_for(got_ping.wait(), timeout=0.01)
        await asyncio.wait_for(got_pong.wait(), timeout=0.01)
