import asyncio
from typing import AsyncIterator, Tuple

from async_generator import asynccontextmanager

from cancel_token import CancelToken

from eth_keys import keys

from p2p.abc import ConnectionAPI, NodeAPI
from p2p.connection import Connection
from p2p.handshake import Handshaker, negotiate_protocol_handshakes
from p2p.service import run_service

from .cancel_token import CancelTokenFactory
from .p2p_proto import DevP2PHandshakeParamsFactory
from .transport import MemoryTransportPairFactory


@asynccontextmanager
async def ConnectionPairFactory(*,
                                alice_handshakers: Tuple[Handshaker, ...],
                                bob_handshakers: Tuple[Handshaker, ...],
                                alice_remote: NodeAPI = None,
                                alice_private_key: keys.PrivateKey = None,
                                alice_client_version: str = 'bob',
                                alice_p2p_version: int = 5,
                                bob_remote: NodeAPI = None,
                                bob_private_key: keys.PrivateKey = None,
                                bob_client_version: str = 'bob',
                                bob_p2p_version: int = 5,
                                cancel_token: CancelToken = None,
                                ) -> AsyncIterator[Tuple[ConnectionAPI, ConnectionAPI]]:
    if cancel_token is None:
        cancel_token = CancelTokenFactory()

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
        (alice_multiplexer, alice_p2p_receipt, alice_receipts),
        (bob_multiplexer, bob_p2p_receipt, bob_receipts),
    ) = await asyncio.gather(
        negotiate_protocol_handshakes(
            alice_transport,
            alice_devp2p_params,
            alice_handshakers,
            cancel_token,
        ),
        negotiate_protocol_handshakes(
            bob_transport,
            bob_devp2p_params,
            bob_handshakers,
            cancel_token,
        ),
    )
    alice_connection = Connection(
        multiplexer=alice_multiplexer,
        devp2p_receipt=alice_p2p_receipt,
        protocol_receipts=alice_receipts,
        is_dial_out=True,
    )
    bob_connection = Connection(
        multiplexer=bob_multiplexer,
        devp2p_receipt=bob_p2p_receipt,
        protocol_receipts=bob_receipts,
        is_dial_out=False,
    )

    async with run_service(alice_connection), run_service(bob_connection):
        yield alice_connection, bob_connection
