import asyncio

import pytest

from p2p.tools.factories import ParagonPeerPairFactory
from p2p.p2p_proto import Ping, Pong
from p2p import p2p_api


@pytest.mark.asyncio
async def test_connection_factory_with_ParagonPeer():
    async with ParagonPeerPairFactory() as (alice, bob):
        got_ping = asyncio.Event()
        got_pong = asyncio.Event()

        async def handle_ping(conn, msg):
            got_ping.set()
            bob.p2p_api.send_pong()

        async def handle_pong(conn, msg):
            got_pong.set()

        alice.connection.add_command_handler(Pong, handle_pong)
        bob.connection.add_command_handler(Ping, handle_ping)

        alice.p2p_api.send_ping()

        await asyncio.wait_for(got_ping.wait(), timeout=1)
        await asyncio.wait_for(got_pong.wait(), timeout=1)


@pytest.mark.asyncio
async def test_disconnect_on_cancellation(monkeypatch):
    disconnects_received = []

    orig_handle = p2p_api.CancelOnDisconnect.handle

    async def handle(self, connection, cmd) -> None:
        disconnects_received.append(connection)
        await orig_handle(self, connection, cmd)

    monkeypatch.setattr(p2p_api.CancelOnDisconnect, 'handle', handle)
    async with ParagonPeerPairFactory() as (alice, bob):
        await alice.cancel()
        await alice.events.cleaned_up.wait()
        await asyncio.sleep(0.1)
        assert len(disconnects_received) == 1
        assert disconnects_received[0] == bob.connection
