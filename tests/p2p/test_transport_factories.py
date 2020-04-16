import asyncio
import pytest

from rlp import sedes

from p2p.tools.factories import (
    TransportPairFactory,
    MemoryTransportPairFactory,
)
from p2p.commands import BaseCommand, RLPCodec


class CommandForTest(BaseCommand[bytes]):
    protocol_command_id = 0
    serialization_codec = RLPCodec(sedes=sedes.binary)


@pytest.fixture(params=('memory', 'real'))
async def transport_pair(request):
    if request.param == 'memory':
        return MemoryTransportPairFactory()
    elif request.param == 'real':
        return await TransportPairFactory()
    else:
        raise Exception(f"Unknown: {request.param}")


@pytest.mark.parametrize('snappy_support', (True, False))
@pytest.mark.asyncio
async def test_transport_pair_factories(transport_pair, snappy_support):
    alice_transport, bob_transport = transport_pair

    done = asyncio.Event()

    async def manage_bob(expected):
        for value in expected:
            msg = await bob_transport.recv()
            assert msg == value
        done.set()

    payloads = (
        b'unicorns',
        b'rainbows',
        b'',
        b'\x00' * 256,
        b'\x00' * 65536,
    )
    messages = tuple(
        CommandForTest(payload).encode(
            CommandForTest.protocol_command_id,
            snappy_support=snappy_support,
        ) for payload in payloads
    )
    asyncio.ensure_future(manage_bob(messages))
    for payload in payloads:
        command = CommandForTest(payload)
        message = command.encode(CommandForTest.protocol_command_id, snappy_support=snappy_support)
        alice_transport.send(message)

    await asyncio.wait_for(done.wait(), timeout=1)


@pytest.mark.asyncio
async def test_closing_one_end_does_not_close_the_other(transport_pair):
    # This is to ensure closing one end of a directly linked transport pair does not cause the
    # other end to be closed as well. That is a problem, for example, when a test (like in
    # tests/p2p/test_peer.py) stops one peer and wants to confirm that the other has received a
    # Disconnect msg, because as soon as the is_closing becomes true on the still-running peer,
    # one of its daemon tasks (Connection._feed_protocol_handlers) exits before the disconnect msg
    # has been received/processed, causing async-service to abort the service because a daemon
    # must not exit while its parent is still running.
    alice, bob = transport_pair
    assert not alice.is_closing
    assert not bob.is_closing
    await alice.close()
    assert alice.is_closing
    assert not bob.is_closing
