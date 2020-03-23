import asyncio

import pytest

from p2p.disconnect import DisconnectReason
from p2p.p2p_proto import Pong, Ping
from p2p.p2p_api import P2PAPI

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
