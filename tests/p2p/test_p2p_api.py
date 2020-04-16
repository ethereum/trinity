import asyncio

import pytest

from p2p.disconnect import DisconnectReason
from p2p.p2p_proto import Pong, Ping
from p2p.p2p_api import DisconnectIfIdle, P2PAPI

from p2p.tools.factories import ConnectionPairFactory


@pytest.fixture
async def alice_and_bob():
    pair_factory = ConnectionPairFactory(
        alice_client_version='alice',
        bob_client_version='bob',
    )
    async with pair_factory as (alice, bob):
        yield alice, bob


@pytest.fixture
def alice(alice_and_bob):
    alice, _ = alice_and_bob
    return alice


@pytest.fixture
def bob(alice_and_bob):
    _, bob = alice_and_bob
    return bob


@pytest.mark.asyncio
async def test_p2p_api_properties(bob, alice):
    async with P2PAPI().as_behavior().apply(alice):
        assert alice.has_logic('p2p')
        p2p_api = alice.get_logic('p2p', P2PAPI)

        assert p2p_api.client_version_string == 'bob'
        assert p2p_api.safe_client_version_string == 'bob'


@pytest.mark.asyncio
async def test_p2p_api_pongs_when_pinged(bob, alice):
    async with P2PAPI().as_behavior().apply(alice):
        got_pong = asyncio.Event()

        async def handle_pong(connection, msg):
            got_pong.set()
        bob.add_command_handler(Pong, handle_pong)
        bob.get_base_protocol().send(Ping(None))
        await asyncio.wait_for(got_pong.wait(), timeout=1)


@pytest.mark.asyncio
async def test_p2p_api_disconnect_fn(bob, alice):
    async with P2PAPI().as_behavior().apply(alice):
        async with P2PAPI().as_behavior().apply(bob):
            alice_p2p_api = alice.get_logic('p2p', P2PAPI)
            bob_p2p_api = bob.get_logic('p2p', P2PAPI)

            alice_p2p_api.disconnect(DisconnectReason.CLIENT_QUITTING)

            assert alice_p2p_api.local_disconnect_reason is DisconnectReason.CLIENT_QUITTING
            assert bob_p2p_api.local_disconnect_reason is None


@pytest.mark.asyncio
async def test_p2p_api_applies_DisconnectIfIdle(bob, alice):
    p2p_api = P2PAPI()
    for behavior in p2p_api._behaviors:
        if isinstance(behavior.logic, DisconnectIfIdle):
            break
    else:
        raise AssertionError("DisconnectIfIdle not among P2PAPI behaviors")


@pytest.mark.asyncio
async def test_DisconnectIfIdle_sends_ping_when_conn_idle(bob, alice):
    got_ping = asyncio.Event()

    async def _handle_ping(conn, cmd):
        got_ping.set()

    idle_timeout = 0.1
    async with DisconnectIfIdle(idle_timeout).apply(bob):
        alice.add_command_handler(Ping, _handle_ping)
        await asyncio.wait_for(got_ping.wait(), timeout=0.5)
        assert bob.manager.is_running
        assert alice.manager.is_running


@pytest.mark.asyncio
async def test_DisconnectIfIdle_cancels_after_idle_timeout(bob, alice, monkeypatch):
    idle_timeout = 0.1
    async with DisconnectIfIdle(idle_timeout).apply(bob):
        alice_transport = alice.get_multiplexer().get_transport()
        monkeypatch.setattr(alice_transport, 'write', lambda data: None)  # Mute alice.
        await asyncio.wait_for(bob.manager.wait_finished(), timeout=0.5)
        assert bob.manager.is_cancelled
