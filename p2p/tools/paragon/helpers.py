import asyncio
import os
from types import MethodType
from typing import (
    Any,
    Callable,
    cast,
    Dict,
    Tuple,
)

from eth_hash.auto import keccak

from cancel_token import CancelToken

from p2p import auth
from p2p import constants
from p2p import ecies
from p2p import kademlia
from p2p import protocol
from p2p.auth import decode_authentication
from p2p.exceptions import (
    HandshakeFailure,
    NoMatchingPeerCapabilities,
)
from p2p.peer import (
    BasePeer,
    BasePeerFactory,
    PeerConnection,
)
from p2p.p2p_proto import (
    DisconnectReason,
    Hello,
)


from .peer import (
    ParagonPeerFactory,
    ParagonContext,
)


class MockTransport:
    def __init__(self) -> None:
        self._is_closing = False

    def close(self) -> None:
        self._is_closing = True

    def is_closing(self) -> bool:
        return self._is_closing


class MockStreamWriter:
    def __init__(self, write_target: Callable[..., None]) -> None:
        self._target = write_target
        self.transport = MockTransport()

    def write(self, *args: Any, **kwargs: Any) -> None:
        self._target(*args, **kwargs)

    def close(self) -> None:
        self.transport.close()


TConnectedStreams = Tuple[
    Tuple[asyncio.StreamReader, asyncio.StreamWriter],
    Tuple[asyncio.StreamReader, asyncio.StreamWriter],
]


def get_directly_connected_streams() -> TConnectedStreams:
    bob_reader = asyncio.StreamReader()
    alice_reader = asyncio.StreamReader()
    # Link the alice's writer to the bob's reader, and the bob's writer to the
    # alice's reader.
    bob_writer = MockStreamWriter(alice_reader.feed_data)
    alice_writer = MockStreamWriter(bob_reader.feed_data)
    return (
        (alice_reader, cast(asyncio.StreamWriter, alice_writer)),
        (bob_reader, cast(asyncio.StreamWriter, bob_writer)),
    )


async def get_directly_linked_peers_without_handshake(
        alice_factory: BasePeerFactory = None,
        bob_factory: BasePeerFactory = None) -> Tuple[BasePeer, BasePeer]:
    """
    See get_directly_linked_peers().

    Neither the P2P handshake nor the sub-protocol handshake will be performed here.
    """
    cancel_token = CancelToken("get_directly_linked_peers_without_handshake")

    if alice_factory is None:
        alice_factory = ParagonPeerFactory(
            privkey=ecies.generate_privkey(),
            context=ParagonContext(),
            token=cancel_token,
        )

    if bob_factory is None:
        bob_factory = ParagonPeerFactory(
            privkey=ecies.generate_privkey(),
            context=ParagonContext(),
            token=cancel_token,
        )

    alice_private_key = alice_factory.privkey
    bob_private_key = bob_factory.privkey

    alice_remote = kademlia.Node(
        bob_private_key.public_key, kademlia.Address('0.0.0.0', 0, 0))
    bob_remote = kademlia.Node(
        alice_private_key.public_key, kademlia.Address('0.0.0.0', 0, 0))

    use_eip8 = False
    initiator = auth.HandshakeInitiator(alice_remote, alice_private_key, use_eip8, cancel_token)

    f_alice: 'asyncio.Future[BasePeer]' = asyncio.Future()
    handshake_finished = asyncio.Event()

    (
        (alice_reader, alice_writer),
        (bob_reader, bob_writer),
    ) = get_directly_connected_streams()

    async def do_handshake() -> None:
        aes_secret, mac_secret, egress_mac, ingress_mac = await auth._handshake(
            initiator, alice_reader, alice_writer, cancel_token)

        connection = PeerConnection(
            reader=alice_reader,
            writer=alice_writer,
            aes_secret=aes_secret,
            mac_secret=mac_secret,
            egress_mac=egress_mac,
            ingress_mac=ingress_mac,
        )
        alice = alice_factory.create_peer(
            alice_remote,
            connection,
        )

        f_alice.set_result(alice)
        handshake_finished.set()

    asyncio.ensure_future(do_handshake())

    use_eip8 = False
    responder = auth.HandshakeResponder(bob_remote, bob_private_key, use_eip8, cancel_token)
    auth_cipher = await bob_reader.read(constants.ENCRYPTED_AUTH_MSG_LEN)

    initiator_ephemeral_pubkey, initiator_nonce, _ = decode_authentication(
        auth_cipher, bob_private_key)
    responder_nonce = keccak(os.urandom(constants.HASH_LEN))
    auth_ack_msg = responder.create_auth_ack_message(responder_nonce)
    auth_ack_ciphertext = responder.encrypt_auth_ack_message(auth_ack_msg)
    bob_writer.write(auth_ack_ciphertext)

    await handshake_finished.wait()
    alice = await f_alice

    aes_secret, mac_secret, egress_mac, ingress_mac = responder.derive_secrets(
        initiator_nonce, responder_nonce, initiator_ephemeral_pubkey,
        auth_cipher, auth_ack_ciphertext)
    assert egress_mac.digest() == alice.ingress_mac.digest()
    assert ingress_mac.digest() == alice.egress_mac.digest()
    connection = PeerConnection(
        reader=bob_reader,
        writer=bob_writer,
        aes_secret=aes_secret,
        mac_secret=mac_secret,
        egress_mac=egress_mac,
        ingress_mac=ingress_mac,
    )
    bob = bob_factory.create_peer(
        bob_remote,
        connection,
    )

    return alice, bob


async def get_directly_linked_peers(
        request: Any, event_loop: asyncio.AbstractEventLoop,
        alice_factory: BasePeerFactory = None,
        bob_factory: BasePeerFactory = None) -> Tuple[BasePeer, BasePeer]:
    """Create two peers with their readers/writers connected directly.

    The first peer's reader will write directly to the second's writer, and vice-versa.
    """
    alice, bob = await get_directly_linked_peers_without_handshake(
        alice_factory,
        bob_factory,
    )

    # Perform the base protocol (P2P) handshake.
    await asyncio.gather(alice.do_p2p_handshake(), bob.do_p2p_handshake())

    assert alice.sub_proto.name == bob.sub_proto.name
    assert alice.sub_proto.version == bob.sub_proto.version
    assert alice.sub_proto.cmd_id_offset == bob.sub_proto.cmd_id_offset

    # Perform the handshake for the enabled sub-protocol.
    await asyncio.gather(alice.do_sub_proto_handshake(), bob.do_sub_proto_handshake())

    asyncio.ensure_future(alice.run())
    asyncio.ensure_future(bob.run())

    def finalizer() -> None:
        event_loop.run_until_complete(asyncio.gather(
            alice.cancel(),
            bob.cancel(),
            loop=event_loop,
        ))
    request.addfinalizer(finalizer)

    # wait for start
    await alice.events.started.wait()
    await bob.events.started.wait()

    # wait for boot
    await alice.boot_manager.events.finished.wait()
    await bob.boot_manager.events.finished.wait()

    return alice, bob


async def process_v4_p2p_handshake(
        self: BasePeer,
        cmd: protocol.Command,
        msg: protocol.PayloadType) -> None:
    """
    A replacement to the existing process_p2p_handshake function. The
    only change is to remove snappy compression, irrespective of whether
    the other client supports it or not. This simulates a DEVp2p v4
    (in Trinity terms: P2PProtocol.version==4) node.
    """
    msg = cast(Dict[str, Any], msg)
    if not isinstance(cmd, Hello):
        await self.disconnect(DisconnectReason.bad_protocol)
        raise HandshakeFailure(f"Expected a Hello msg, got {cmd}, disconnecting")

    # snappy compression (EIP-706) was introduced in DEVp2p v5; turning it off "forces" v4
    self.snappy_support = False

    remote_capabilities = msg['capabilities']
    try:
        self.sub_proto = self.select_sub_protocol(remote_capabilities)
    except NoMatchingPeerCapabilities:
        await self.disconnect(DisconnectReason.useless_peer)
        raise HandshakeFailure(
            f"No matching capabilities between us ({self.capabilities}) and {self.remote} "
            f"({remote_capabilities}), disconnecting"
        )

    self.logger.debug(
        "Finished P2P handshake with %s, using sub-protocol %s",
        self.remote, self.sub_proto)


async def get_directly_linked_v4_and_v5_peers(
        request: Any, event_loop: asyncio.AbstractEventLoop,
        alice_factory: BasePeerFactory = None,
        bob_factory: BasePeerFactory = None) -> Tuple[BasePeer, BasePeer]:
    """Create two peers with their readers/writers connected directly.

    The first peer's reader will write directly to the second's writer, and vice-versa.
    """
    alice, bob = await get_directly_linked_peers_without_handshake(
        alice_factory,
        bob_factory,
    )

    # Tweaking the P2P Protocol Versions for Alice
    alice.base_protocol.version = 4
    # The below type ignore is because mypy still doesn't support method overwrites
    alice.process_p2p_handshake = MethodType(process_v4_p2p_handshake, alice)  # type: ignore

    # Perform the base protocol (P2P) handshake.
    await asyncio.gather(alice.do_p2p_handshake(), bob.do_p2p_handshake())

    assert alice.sub_proto.name == bob.sub_proto.name
    assert alice.sub_proto.version == bob.sub_proto.version
    assert alice.sub_proto.cmd_id_offset == bob.sub_proto.cmd_id_offset
    # NOTE: Not testing for compression here - that's done in the actual test!

    # Perform the handshake for the enabled sub-protocol.
    await asyncio.gather(alice.do_sub_proto_handshake(), bob.do_sub_proto_handshake())

    asyncio.ensure_future(alice.run())
    asyncio.ensure_future(bob.run())

    def finalizer() -> None:
        event_loop.run_until_complete(asyncio.gather(
            alice.cancel(),
            bob.cancel(),
            loop=event_loop,
        ))
    request.addfinalizer(finalizer)

    # wait for start
    await alice.events.started.wait()
    await bob.events.started.wait()

    # wait for boot
    await alice.boot_manager.events.finished.wait()
    await bob.boot_manager.events.finished.wait()

    return alice, bob
