import asyncio
from typing import Tuple

from eth_keys import keys

from p2p import auth
from p2p.abc import NodeAPI, TransportAPI
from p2p.transport import Transport

from p2p.tools.asyncio_streams import get_directly_connected_streams
from p2p.tools.memory_transport import MemoryTransport

from .kademlia import NodeFactory
from .keys import PrivateKeyFactory


async def TransportPairFactory(*,
                               alice_remote: NodeAPI = None,
                               alice_private_key: keys.PrivateKey = None,
                               bob_remote: NodeAPI = None,
                               bob_private_key: keys.PrivateKey = None,
                               use_eip8: bool = False,
                               ) -> Tuple[TransportAPI, TransportAPI]:
    if alice_private_key is None:
        alice_private_key = PrivateKeyFactory()
    if alice_remote is None:
        alice_remote = NodeFactory(pubkey=alice_private_key.public_key)

    if bob_private_key is None:
        bob_private_key = PrivateKeyFactory()
    if bob_remote is None:
        bob_remote = NodeFactory(pubkey=bob_private_key.public_key)

    assert alice_private_key.public_key == alice_remote.pubkey
    assert bob_private_key.public_key == bob_remote.pubkey
    assert alice_private_key != bob_private_key

    initiator = auth.HandshakeInitiator(bob_remote, alice_private_key, use_eip8)

    f_alice: 'asyncio.Future[TransportAPI]' = asyncio.Future()
    handshake_finished = asyncio.Event()

    bob_peername = (bob_remote.address.ip, bob_remote.address.udp_port, bob_remote.address.tcp_port)
    alice_peername = (alice_remote.address.ip, alice_remote.address.udp_port, alice_remote.address.tcp_port)  # noqa: E501

    (
        (alice_reader, alice_writer),
        (bob_reader, bob_writer),
    ) = get_directly_connected_streams(
        bob_extra_info={'peername': bob_peername},
        alice_extra_info={'peername': alice_peername},
    )

    async def establish_transport() -> None:
        aes_secret, mac_secret, egress_mac, ingress_mac = await auth._handshake(
            initiator, alice_reader, alice_writer)

        transport = Transport(
            remote=alice_remote,
            private_key=alice_private_key,
            reader=alice_reader,
            writer=alice_writer,
            aes_secret=aes_secret,
            mac_secret=mac_secret,
            egress_mac=egress_mac,
            ingress_mac=ingress_mac,
        )

        f_alice.set_result(transport)
        handshake_finished.set()

    asyncio.ensure_future(establish_transport())

    bob_transport = await asyncio.wait_for(Transport.receive_connection(
        reader=bob_reader,
        writer=bob_writer,
        private_key=bob_private_key,
    ), timeout=1)

    await asyncio.wait_for(handshake_finished.wait(), timeout=0.1)
    alice_transport = await asyncio.wait_for(f_alice, timeout=0.1)
    return alice_transport, bob_transport


def MemoryTransportPairFactory(alice_remote: NodeAPI = None,
                               alice_private_key: keys.PrivateKey = None,
                               bob_remote: NodeAPI = None,
                               bob_private_key: keys.PrivateKey = None,
                               ) -> Tuple[TransportAPI, TransportAPI]:
    if alice_remote is None:
        alice_remote = NodeFactory()
    if alice_private_key is None:
        alice_private_key = PrivateKeyFactory()

    if bob_remote is None:
        bob_remote = NodeFactory()
    if bob_private_key is None:
        bob_private_key = PrivateKeyFactory()

    # the remotes are intentionally switched since they represent the *other*
    # side of the connection.
    alice_transport, bob_transport = MemoryTransport.connected_pair(
        alice=(bob_remote, alice_private_key),
        bob=(alice_remote, bob_private_key),
    )
    return alice_transport, bob_transport
