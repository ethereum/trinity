import asyncio
import contextlib
import pytest

from p2p.tools.factories import (
    MemoryTransportPairFactory,
    ProtocolFactory,
    DevP2PHandshakeParamsFactory,
)
from p2p.p2p_proto import P2PProtocolV4, P2PProtocolV5
from p2p.handshake import (
    negotiate_protocol_handshakes,
    DevP2PHandshakeParams,
)
from p2p.tools.handshake import NoopHandshaker


@contextlib.asynccontextmanager
async def _do_handshake(alice_handshakers, alice_version, bob_handshakers, bob_version):
    alice_transport, bob_transport = MemoryTransportPairFactory()

    alice_p2p_params = DevP2PHandshakeParamsFactory(
        client_version_string='alice-client',
        listen_port=alice_transport.remote.address.tcp_port,
        version=alice_version,
    )
    bob_p2p_params = DevP2PHandshakeParams(
        client_version_string='bob-client',
        listen_port=bob_transport.remote.address.tcp_port,
        version=bob_version,
    )

    alice_coro = negotiate_protocol_handshakes(
        alice_transport,
        alice_p2p_params,
        alice_handshakers,
    )
    bob_coro = negotiate_protocol_handshakes(
        bob_transport,
        bob_p2p_params,
        bob_handshakers,
    )

    alice_result, bob_result = await asyncio.gather(alice_coro, bob_coro)

    alice_multiplexer, alice_p2p_receipt, alice_receipts = alice_result
    bob_multiplexer, bob_p2p_receipt, bob_receipts = bob_result

    yield (alice_multiplexer, alice_p2p_receipt, alice_receipts,
           bob_multiplexer, bob_p2p_receipt, bob_receipts)

    # Need to manually stop the background streaming task started by
    # negotiate_protocol_handshakes() because we never use the multiplexers in a Connection, which
    # would do that for us.
    await alice_multiplexer.stop_streaming()
    await bob_multiplexer.stop_streaming()


@pytest.mark.asyncio
async def test_handshake_with_v4_and_v5_disables_snappy():
    protocol_class = ProtocolFactory()
    alice_handshakers = bob_handshakers = (NoopHandshaker(protocol_class),)
    alice_version = 5
    bob_version = 4
    async with _do_handshake(alice_handshakers, alice_version, bob_handshakers, bob_version) as (
        _, alice_p2p_receipt, _, _, bob_p2p_receipt, _
    ):
        alice_p2p_protocol = alice_p2p_receipt.protocol
        bob_p2p_protocol = bob_p2p_receipt.protocol

        assert isinstance(alice_p2p_protocol, P2PProtocolV5)
        assert alice_p2p_protocol.snappy_support is False

        assert isinstance(bob_p2p_protocol, P2PProtocolV4)
        assert bob_p2p_protocol.snappy_support is False


@pytest.mark.asyncio
async def test_handshake_with_single_protocol():
    protocol_class = ProtocolFactory()
    alice_handshakers = bob_handshakers = (NoopHandshaker(protocol_class),)
    alice_version = bob_version = 5
    async with _do_handshake(alice_handshakers, alice_version, bob_handshakers, bob_version) as (
        alice_multiplexer, alice_p2p_receipt, alice_receipts,
        bob_multiplexer, bob_p2p_receipt, bob_receipts
    ):
        assert len(alice_receipts) == 1
        assert len(bob_receipts) == 1

        alice_receipt = alice_receipts[0]
        bob_receipt = bob_receipts[0]

        assert alice_p2p_receipt.client_version_string == 'bob-client'
        assert bob_p2p_receipt.client_version_string == 'alice-client'

        assert isinstance(alice_receipt.protocol, protocol_class)
        assert isinstance(bob_receipt.protocol, protocol_class)

        assert isinstance(alice_multiplexer.get_base_protocol(), P2PProtocolV5)
        assert isinstance(alice_multiplexer.get_protocols()[0], P2PProtocolV5)
        assert isinstance(alice_multiplexer.get_protocols()[1], protocol_class)

        assert isinstance(bob_multiplexer.get_base_protocol(), P2PProtocolV5)
        assert isinstance(bob_multiplexer.get_protocols()[0], P2PProtocolV5)
        assert isinstance(bob_multiplexer.get_protocols()[1], protocol_class)

        alice_p2p_protocol = alice_p2p_receipt.protocol
        assert alice_p2p_protocol.snappy_support is True

        bob_p2p_protocol = bob_p2p_receipt.protocol
        assert bob_p2p_protocol.snappy_support is True


@pytest.mark.asyncio
async def test_handshake_with_multiple_protocols():
    A1 = ProtocolFactory(name='a', version=1)
    A2 = ProtocolFactory(name='a', version=2)
    A3 = ProtocolFactory(name='a', version=3)
    B5 = ProtocolFactory(name='b', version=5)
    B6 = ProtocolFactory(name='b', version=6)
    B7 = ProtocolFactory(name='b', version=7)
    C2 = ProtocolFactory(name='c', version=2)
    C3 = ProtocolFactory(name='c', version=3)

    alice_protos = (A1, A2, A3, B5, B7, C3)
    bob_protos = (A2, B6, C3, C2)
    # expected overlap A2, C3

    alice_handshakers = tuple(map(NoopHandshaker, alice_protos))
    bob_handshakers = tuple(map(NoopHandshaker, bob_protos))
    alice_version = bob_version = 5

    async with _do_handshake(alice_handshakers, alice_version, bob_handshakers, bob_version) as (
        alice_multiplexer, alice_p2p_receipt, alice_receipts,
        bob_multiplexer, bob_p2p_receipt, bob_receipts
    ):
        assert len(alice_receipts) == 2
        assert len(bob_receipts) == 2

        assert isinstance(alice_p2p_receipt.protocol, P2PProtocolV5)
        assert isinstance(alice_receipts[0].protocol, A2)
        assert isinstance(alice_receipts[1].protocol, C3)

        assert isinstance(alice_p2p_receipt.protocol, P2PProtocolV5)
        assert isinstance(bob_receipts[0].protocol, A2)
        assert isinstance(bob_receipts[1].protocol, C3)

        assert isinstance(alice_multiplexer.get_base_protocol(), P2PProtocolV5)
        assert isinstance(alice_multiplexer.get_protocols()[0], P2PProtocolV5)
        assert isinstance(alice_multiplexer.get_protocols()[1], A2)
        assert isinstance(alice_multiplexer.get_protocols()[2], C3)

        assert isinstance(bob_multiplexer.get_base_protocol(), P2PProtocolV5)
        assert isinstance(bob_multiplexer.get_protocols()[0], P2PProtocolV5)
        assert isinstance(bob_multiplexer.get_protocols()[1], A2)
        assert isinstance(bob_multiplexer.get_protocols()[2], C3)
