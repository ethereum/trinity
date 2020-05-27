import asyncio
import time

import pytest

from eth_utils import ValidationError

from p2p.exceptions import UnknownProtocol
from p2p.commands import BaseCommand, NoneSerializationCodec
from p2p.protocol import BaseProtocol
from p2p.p2p_proto import Ping, Pong, P2PProtocolV5

from p2p.tools.factories import MultiplexerPairFactory


DEFAULT_TIMEOUT = 1


class CommandA(BaseCommand):
    protocol_command_id = 0
    serialization_codec = NoneSerializationCodec()


class CommandB(BaseCommand):
    protocol_command_id = 1
    serialization_codec = NoneSerializationCodec()


class SecondProtocol(BaseProtocol):
    name = 'second'
    version = 1
    commands = (CommandA, CommandB)
    command_length = 2


class CommandC(BaseCommand):
    protocol_command_id = 0
    serialization_codec = NoneSerializationCodec()


class CommandD(BaseCommand):
    protocol_command_id = 1
    serialization_codec = NoneSerializationCodec()


class ThirdProtocol(BaseProtocol):
    name = 'third'
    version = 1
    commands = (CommandC, CommandD)
    command_length = 2


class UnsupportedProtocol(BaseProtocol):
    name = 'unknown'
    version = 1
    commands = (CommandC, CommandD)
    command_length = 2


@pytest.mark.asyncio
async def test_multiplexer_properties():
    multiplexer, _ = MultiplexerPairFactory(
        protocol_types=(SecondProtocol, ThirdProtocol),
    )
    transport = multiplexer.get_transport()

    base_protocol = multiplexer.get_protocol_by_type(P2PProtocolV5)
    second_protocol = multiplexer.get_protocol_by_type(SecondProtocol)
    third_protocol = multiplexer.get_protocol_by_type(ThirdProtocol)

    assert multiplexer.get_base_protocol() is base_protocol
    assert multiplexer.get_protocols() == (base_protocol, second_protocol, third_protocol)

    assert multiplexer.get_protocol_by_type(P2PProtocolV5) is base_protocol
    assert multiplexer.get_protocol_by_type(SecondProtocol) is second_protocol
    assert multiplexer.get_protocol_by_type(ThirdProtocol) is third_protocol

    assert multiplexer.has_protocol(P2PProtocolV5) is True
    assert multiplexer.has_protocol(SecondProtocol) is True
    assert multiplexer.has_protocol(ThirdProtocol) is True
    assert multiplexer.has_protocol(UnsupportedProtocol) is False

    assert multiplexer.has_protocol(base_protocol) is True
    assert multiplexer.has_protocol(second_protocol) is True
    assert multiplexer.has_protocol(third_protocol) is True
    assert multiplexer.has_protocol(UnsupportedProtocol(transport, 16, False)) is False

    assert multiplexer.remote is transport.remote


@pytest.mark.asyncio
async def test_stream_in_background():
    alice, _ = MultiplexerPairFactory()
    assert not alice.is_streaming
    assert not alice.is_closing
    with pytest.raises(Exception):
        await alice.wait_streaming()

    await alice.stream_in_background()

    assert alice.is_streaming

    await alice.stop_streaming()

    assert not alice.is_streaming
    assert alice.is_closing
    assert isinstance(alice.get_streaming_error(), asyncio.CancelledError)

    with pytest.raises(asyncio.CancelledError):
        await alice.wait_streaming_finished()
    with pytest.raises(asyncio.CancelledError):
        alice.raise_if_streaming_error()


@pytest.mark.asyncio
async def test_streaming_error(monkeypatch):
    alice, _ = MultiplexerPairFactory()

    msg = "Multiplexing error"
    exc = Exception(msg)

    async def multiplexing():
        alice._started_streaming.set()
        raise exc

    monkeypatch.setattr(alice, '_do_multiplexing', multiplexing)
    await alice.stream_in_background()

    with pytest.raises(Exception, match=msg):
        await alice.wait_streaming_finished()
    assert alice.get_streaming_error() == exc
    with pytest.raises(Exception, match=msg):
        alice.raise_if_streaming_error()


@pytest.mark.asyncio
async def test_multiplexer_only_p2p_protocol(request, event_loop):
    alice_multiplexer, bob_multiplexer = MultiplexerPairFactory()
    await run_multiplexers([alice_multiplexer, bob_multiplexer], request, event_loop)

    alice_stream = alice_multiplexer.stream_protocol_messages(P2PProtocolV5)
    bob_stream = bob_multiplexer.stream_protocol_messages(P2PProtocolV5)

    alice_p2p_protocol = alice_multiplexer.get_protocol_by_type(P2PProtocolV5)
    bob_p2p_protocol = bob_multiplexer.get_protocol_by_type(P2PProtocolV5)

    alice_p2p_protocol.send(Ping(None))
    cmd = await asyncio.wait_for(bob_stream.asend(None), timeout=DEFAULT_TIMEOUT)
    assert isinstance(cmd, Ping)

    bob_p2p_protocol.send(Pong(None))
    cmd = await asyncio.wait_for(alice_stream.asend(None), timeout=DEFAULT_TIMEOUT)


@pytest.mark.asyncio
async def test_multiplexer_p2p_and_paragon_protocol(request, event_loop):
    alice_multiplexer, bob_multiplexer = MultiplexerPairFactory(
        protocol_types=(SecondProtocol,),
    )
    await run_multiplexers([alice_multiplexer, bob_multiplexer], request, event_loop)

    alice_p2p_stream = alice_multiplexer.stream_protocol_messages(P2PProtocolV5)
    bob_p2p_stream = bob_multiplexer.stream_protocol_messages(P2PProtocolV5)
    alice_second_stream = alice_multiplexer.stream_protocol_messages(SecondProtocol)
    bob_second_stream = bob_multiplexer.stream_protocol_messages(SecondProtocol)

    alice_p2p_protocol = alice_multiplexer.get_protocol_by_type(P2PProtocolV5)
    alice_second_protocol = alice_multiplexer.get_protocol_by_type(SecondProtocol)

    bob_p2p_protocol = bob_multiplexer.get_protocol_by_type(P2PProtocolV5)
    bob_second_protocol = bob_multiplexer.get_protocol_by_type(SecondProtocol)

    alice_second_protocol.send(CommandA(None))
    alice_p2p_protocol.send(Ping(None))
    alice_second_protocol.send(CommandB(None))
    cmd = await asyncio.wait_for(bob_p2p_stream.asend(None), timeout=DEFAULT_TIMEOUT)
    assert isinstance(cmd, Ping)

    bob_second_protocol.send(CommandA(None))
    bob_p2p_protocol.send(Pong(None))
    bob_second_protocol.send(CommandB(None))

    cmd = await asyncio.wait_for(alice_p2p_stream.asend(None), timeout=DEFAULT_TIMEOUT)
    assert isinstance(cmd, Pong)

    cmd_1 = await asyncio.wait_for(bob_second_stream.asend(None), timeout=DEFAULT_TIMEOUT)  # noqa: E501
    cmd_2 = await asyncio.wait_for(bob_second_stream.asend(None), timeout=DEFAULT_TIMEOUT)  # noqa: E501
    cmd_3 = await asyncio.wait_for(alice_second_stream.asend(None), timeout=DEFAULT_TIMEOUT)  # noqa: E501
    cmd_4 = await asyncio.wait_for(alice_second_stream.asend(None), timeout=DEFAULT_TIMEOUT)  # noqa: E501

    assert isinstance(cmd_1, CommandA)
    assert isinstance(cmd_2, CommandB)
    assert isinstance(cmd_3, CommandA)
    assert isinstance(cmd_4, CommandB)


@pytest.mark.asyncio
async def test_multiplexer_p2p_and_two_more_protocols(request, event_loop):
    alice_multiplexer, bob_multiplexer = MultiplexerPairFactory(
        protocol_types=(SecondProtocol, ThirdProtocol),
    )
    await run_multiplexers([alice_multiplexer, bob_multiplexer], request, event_loop)

    alice_p2p_stream = alice_multiplexer.stream_protocol_messages(P2PProtocolV5)
    bob_p2p_stream = bob_multiplexer.stream_protocol_messages(P2PProtocolV5)
    alice_second_stream = alice_multiplexer.stream_protocol_messages(SecondProtocol)
    bob_second_stream = bob_multiplexer.stream_protocol_messages(SecondProtocol)
    alice_third_stream = alice_multiplexer.stream_protocol_messages(ThirdProtocol)
    bob_third_stream = bob_multiplexer.stream_protocol_messages(ThirdProtocol)

    alice_p2p_protocol = alice_multiplexer.get_protocol_by_type(P2PProtocolV5)
    alice_second_protocol = alice_multiplexer.get_protocol_by_type(SecondProtocol)
    alice_third_protocol = alice_multiplexer.get_protocol_by_type(ThirdProtocol)

    bob_p2p_protocol = bob_multiplexer.get_protocol_by_type(P2PProtocolV5)
    bob_second_protocol = bob_multiplexer.get_protocol_by_type(SecondProtocol)
    bob_third_protocol = bob_multiplexer.get_protocol_by_type(ThirdProtocol)

    alice_second_protocol.send(CommandA(None))
    alice_third_protocol.send(CommandC(None))
    alice_p2p_protocol.send(Ping(None))
    alice_second_protocol.send(CommandB(None))
    alice_third_protocol.send(CommandD(None))
    cmd = await asyncio.wait_for(bob_p2p_stream.asend(None), timeout=DEFAULT_TIMEOUT)
    assert isinstance(cmd, Ping)

    bob_second_protocol.send(CommandA(None))
    bob_third_protocol.send(CommandC(None))
    bob_p2p_protocol.send(Pong(None))
    bob_second_protocol.send(CommandB(None))
    bob_third_protocol.send(CommandD(None))
    cmd = await asyncio.wait_for(alice_p2p_stream.asend(None), timeout=DEFAULT_TIMEOUT)
    assert isinstance(cmd, Pong)

    cmd_1 = await asyncio.wait_for(bob_third_stream.asend(None), timeout=DEFAULT_TIMEOUT)  # noqa: E501
    cmd_2 = await asyncio.wait_for(bob_third_stream.asend(None), timeout=DEFAULT_TIMEOUT)  # noqa: E501
    cmd_3 = await asyncio.wait_for(bob_second_stream.asend(None), timeout=DEFAULT_TIMEOUT)  # noqa: E501
    cmd_4 = await asyncio.wait_for(bob_second_stream.asend(None), timeout=DEFAULT_TIMEOUT)  # noqa: E501
    cmd_5 = await asyncio.wait_for(alice_third_stream.asend(None), timeout=DEFAULT_TIMEOUT)  # noqa: E501
    cmd_6 = await asyncio.wait_for(alice_third_stream.asend(None), timeout=DEFAULT_TIMEOUT)  # noqa: E501
    cmd_7 = await asyncio.wait_for(alice_second_stream.asend(None), timeout=DEFAULT_TIMEOUT)  # noqa: E501
    cmd_8 = await asyncio.wait_for(alice_second_stream.asend(None), timeout=DEFAULT_TIMEOUT)  # noqa: E501

    assert isinstance(cmd_1, CommandC)
    assert isinstance(cmd_2, CommandD)
    assert isinstance(cmd_3, CommandA)
    assert isinstance(cmd_4, CommandB)
    assert isinstance(cmd_5, CommandC)
    assert isinstance(cmd_6, CommandD)
    assert isinstance(cmd_7, CommandA)
    assert isinstance(cmd_8, CommandB)


class SharedProtocol(BaseProtocol):
    name = 'shared'
    version = 1
    commands = (CommandB, CommandC)
    command_length = 2


@pytest.mark.asyncio
async def test_connection_get_protocol_for_command_type():
    multiplexer, _ = MultiplexerPairFactory(
        protocol_types=(SecondProtocol, SharedProtocol),
    )
    second_proto = multiplexer.get_protocol_by_type(SecondProtocol)
    shared_proto = multiplexer.get_protocol_by_type(SharedProtocol)

    proto_for_command_A = multiplexer.get_protocol_for_command_type(CommandA)
    assert proto_for_command_A is second_proto

    proto_for_command_C = multiplexer.get_protocol_for_command_type(CommandC)
    assert proto_for_command_C is shared_proto

    with pytest.raises(UnknownProtocol):
        multiplexer.get_protocol_for_command_type(CommandD)

    with pytest.raises(ValidationError):
        multiplexer.get_protocol_for_command_type(CommandB)


@pytest.mark.asyncio
async def test_last_msg_time(monkeypatch, request, event_loop):
    alice_multiplexer, bob_multiplexer = MultiplexerPairFactory()
    await run_multiplexers([alice_multiplexer, bob_multiplexer], request, event_loop)

    assert alice_multiplexer.last_msg_time == 0
    assert bob_multiplexer.last_msg_time == 0
    alice_stream = alice_multiplexer.stream_protocol_messages(P2PProtocolV5)
    bob_stream = bob_multiplexer.stream_protocol_messages(P2PProtocolV5)

    alice_p2p_protocol = alice_multiplexer.get_protocol_by_type(P2PProtocolV5)
    bob_p2p_protocol = bob_multiplexer.get_protocol_by_type(P2PProtocolV5)

    now = time.monotonic()
    monkeypatch.setattr(time, 'monotonic', lambda: now)

    alice_p2p_protocol.send(Ping(None))
    cmd = await asyncio.wait_for(bob_stream.asend(None), timeout=DEFAULT_TIMEOUT)
    assert isinstance(cmd, Ping)
    assert bob_multiplexer.last_msg_time == now

    bob_p2p_protocol.send(Pong(None))
    cmd = await asyncio.wait_for(alice_stream.asend(None), timeout=DEFAULT_TIMEOUT)
    assert isinstance(cmd, Pong)
    assert alice_multiplexer.last_msg_time == now


async def run_multiplexers(multiplexers, request, event_loop):
    for multiplexer in multiplexers:
        await multiplexer.stream_in_background()

    def cancel_multiplexers():
        for multiplexer in multiplexers:
            event_loop.run_until_complete(multiplexer.stop_streaming())

    request.addfinalizer(cancel_multiplexers)
