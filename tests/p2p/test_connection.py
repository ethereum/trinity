import asyncio

import pytest

from p2p.connections import Connection
from p2p.handshake import DevP2PReceipt
from p2p.protocol import Command, Protocol
from p2p.p2p_proto import Ping, Pong

from p2p.tools.factories import (
    CancelTokenFactory,
    MultiplexerPairFactory,
)


class CommandA(Command):
    _cmd_id = 0
    structure = ()


class CommandB(Command):
    _cmd_id = 1
    structure = ()


class SecondProtocol(Protocol):
    name = 'second'
    version = 1
    _commands = (CommandA, CommandB)
    cmd_length = 2

    def send_cmd(self, cmd_type) -> None:
        header, body = self.cmd_by_type[cmd_type].encode({})
        self.transport.send(header, body)


class CommandC(Command):
    _cmd_id = 0
    structure = ()


class CommandD(Command):
    _cmd_id = 1
    structure = ()


class ThirdProtocol(Protocol):
    name = 'third'
    version = 1
    _commands = (CommandC, CommandD)
    cmd_length = 2

    def send_cmd(self, cmd_type) -> None:
        header, body = self.cmd_by_type[cmd_type].encode({})
        self.transport.send(header, body)


@pytest.mark.asyncio
async def test_connection():
    cancel_token = CancelTokenFactory()

    alice_multiplexer, bob_multiplexer = MultiplexerPairFactory()

    alice_p2p_protocol = alice_multiplexer.get_base_protocol()
    bob_p2p_protocol = bob_multiplexer.get_base_protocol()

    alice_p2p_receipt = DevP2PReceipt(
        protocol=alice_p2p_protocol,
        version=5,
        client_version_string='alice',
        capabilities=(),
        listen_port=30303,
        remote_public_key=alice_multiplexer.remote.pubkey,
    )
    bob_p2p_receipt = DevP2PReceipt(
        protocol=bob_p2p_protocol,
        version=5,
        client_version_string='bob',
        capabilities=(),
        listen_port=30303,
        remote_public_key=bob_multiplexer.remote.pubkey,
    )

    alice_connection = Connection(
        alice_multiplexer,
        bob_p2p_receipt,
        (),
        is_dial_out=True,
        token=cancel_token,
    )
    bob_connection = Connection(
        bob_multiplexer,
        alice_p2p_receipt,
        (),
        is_dial_out=False,
        token=cancel_token,
    )

    asyncio.ensure_future(alice_connection.run())
    asyncio.ensure_future(bob_connection.run())

    await alice_connection.events.started.wait()
    await bob_connection.events.started.wait()

    alice_connection.start_protocol_streams()
    bob_connection.start_protocol_streams()

    got_ping = asyncio.Event()
    got_pong = asyncio.Event()

    def _handle_ping(msg):
        got_ping.set()
        bob_p2p_protocol.send_pong()

    def _handle_pong(msg):
        got_pong.set()

    alice_connection.add_command_handler(Pong, _handle_pong)
    bob_connection.add_command_handler(Ping, _handle_ping)

    alice_p2p_protocol.send_ping()

    await asyncio.wait_for(got_ping.wait(), timeout=0.01)
    await asyncio.wait_for(got_pong.wait(), timeout=0.01)
