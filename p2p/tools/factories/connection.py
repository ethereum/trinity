import asyncio
import contextlib
from typing import AsyncIterator, Tuple

from async_service import background_asyncio_service

from eth_keys import keys

from p2p.abc import ConnectionAPI, HandshakerAPI, NodeAPI, ProtocolAPI
from p2p.connection import Connection
from p2p.constants import DEVP2P_V5
from p2p.handshake import (
    DevP2PReceipt,
    negotiate_protocol_handshakes,
)

from .multiplexer import MultiplexerPairFactory
from .p2p_proto import DevP2PHandshakeParamsFactory
from .transport import MemoryTransportPairFactory


@contextlib.asynccontextmanager
async def ConnectionPairFactory(*,
                                alice_handshakers: Tuple[HandshakerAPI[ProtocolAPI], ...] = (),
                                bob_handshakers: Tuple[HandshakerAPI[ProtocolAPI], ...] = (),
                                alice_remote: NodeAPI = None,
                                alice_private_key: keys.PrivateKey = None,
                                alice_client_version: str = 'alice',
                                alice_p2p_version: int = DEVP2P_V5,
                                bob_remote: NodeAPI = None,
                                bob_private_key: keys.PrivateKey = None,
                                bob_client_version: str = 'bob',
                                bob_p2p_version: int = DEVP2P_V5,
                                start_streams: bool = True,
                                ) -> AsyncIterator[Tuple[ConnectionAPI, ConnectionAPI]]:
    alice_connection, bob_connection = await ConnectionPairFactoryNotRunning(
        alice_handshakers=alice_handshakers,
        bob_handshakers=bob_handshakers,
        alice_remote=alice_remote,
        alice_private_key=alice_private_key,
        alice_client_version=alice_client_version,
        alice_p2p_version=alice_p2p_version,
        bob_remote=bob_remote,
        bob_private_key=bob_private_key,
        bob_client_version=bob_client_version,
        bob_p2p_version=bob_p2p_version,
        start_streams=start_streams
    )
    async with background_asyncio_service(alice_connection), background_asyncio_service(bob_connection):  # noqa: E501
        if start_streams:
            alice_connection.start_protocol_streams()
            bob_connection.start_protocol_streams()
        yield alice_connection, bob_connection


async def ConnectionPairFactoryNotRunning(
        *,
        alice_handshakers: Tuple[HandshakerAPI[ProtocolAPI], ...] = (),
        bob_handshakers: Tuple[HandshakerAPI[ProtocolAPI], ...] = (),
        alice_remote: NodeAPI = None,
        alice_private_key: keys.PrivateKey = None,
        alice_client_version: str = 'alice',
        alice_p2p_version: int = DEVP2P_V5,
        bob_remote: NodeAPI = None,
        bob_private_key: keys.PrivateKey = None,
        bob_client_version: str = 'bob',
        bob_p2p_version: int = DEVP2P_V5,
        start_streams: bool = True,
) -> Tuple[ConnectionAPI, ConnectionAPI]:
    if alice_handshakers or bob_handshakers:
        # We only leverage `negotiate_protocol_handshakes` if we have actual
        # protocol handshakers since it raises `NoMatchingPeerCapabilities` if
        # there are no matching capabilities.
        alice_transport, bob_transport = MemoryTransportPairFactory(
            alice_remote=alice_remote,
            alice_private_key=alice_private_key,
            bob_remote=bob_remote,
            bob_private_key=bob_private_key,
        )
        alice_devp2p_params = DevP2PHandshakeParamsFactory(
            client_version_string=alice_client_version,
            listen_port=alice_transport.remote.address.tcp_port,
            version=alice_p2p_version,
        )
        bob_devp2p_params = DevP2PHandshakeParamsFactory(
            client_version_string=bob_client_version,
            listen_port=bob_transport.remote.address.tcp_port,
            version=bob_p2p_version,
        )

        (
            (alice_multiplexer, alice_p2p_receipt, alice_protocol_receipts),
            (bob_multiplexer, bob_p2p_receipt, bob_protocol_receipts),
        ) = await asyncio.gather(
            negotiate_protocol_handshakes(
                alice_transport,
                alice_devp2p_params,
                alice_handshakers,
            ),
            negotiate_protocol_handshakes(
                bob_transport,
                bob_devp2p_params,
                bob_handshakers,
            ),
        )
    else:
        # This path is just for testing to allow us to establish a `Connection`
        # without any protocols beyond the base p2p protocol.
        alice_multiplexer, bob_multiplexer = MultiplexerPairFactory(
            alice_remote=alice_remote,
            alice_private_key=alice_private_key,
            alice_p2p_version=alice_p2p_version,
            bob_remote=bob_remote,
            bob_private_key=bob_private_key,
            bob_p2p_version=bob_p2p_version,
        )
        alice_p2p_receipt = DevP2PReceipt(
            protocol=alice_multiplexer.get_base_protocol(),
            version=bob_p2p_version,
            client_version_string=bob_client_version,
            capabilities=(),
            listen_port=bob_multiplexer.remote.address.tcp_port,
            remote_public_key=bob_multiplexer.remote.pubkey,
        )
        bob_p2p_receipt = DevP2PReceipt(
            protocol=bob_multiplexer.get_base_protocol(),
            version=alice_p2p_version,
            client_version_string=alice_client_version,
            capabilities=(),
            listen_port=alice_multiplexer.remote.address.tcp_port,
            remote_public_key=alice_multiplexer.remote.pubkey,
        )
        alice_protocol_receipts = ()
        bob_protocol_receipts = ()
        # Here we need to manually start the multiplexers as we don't use
        # negotiate_protocol_handshakes() as above.
        await alice_multiplexer.stream_in_background()
        await bob_multiplexer.stream_in_background()

    alice_connection = Connection(
        multiplexer=alice_multiplexer,
        devp2p_receipt=alice_p2p_receipt,
        protocol_receipts=alice_protocol_receipts,
        is_dial_out=True,
    )
    bob_connection = Connection(
        multiplexer=bob_multiplexer,
        devp2p_receipt=bob_p2p_receipt,
        protocol_receipts=bob_protocol_receipts,
        is_dial_out=False,
    )

    return alice_connection, bob_connection
