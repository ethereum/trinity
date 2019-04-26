import asyncio

import sha3

from eth_keys import datatypes

from kademlia import Node
from p2p.service import BaseService


async def handshake(remote: Node, private_key: datatypes.PrivateKey, cancel_token: CancelToken) -> 'BasePeer':
    """Perform the auth and P2P handshakes with the given remote.

    Return an instance of the given peer_class (must be a subclass of
    BasePeer) connected to that remote in case both handshakes are
    successful and at least one of the sub-protocols supported by
    peer_class is also supported by the remote.

    Raises UnreachablePeer if we cannot connect to the peer or
    HandshakeFailure if the remote disconnects before completing the
    handshake or if none of the sub-protocols supported by us is also
    supported by the remote.
    """
    try:
        (aes_secret,
         mac_secret,
         egress_mac,
         ingress_mac,
         reader,
         writer
         ) = await auth.handshake(remote, factory.privkey, factory.cancel_token)
    except (ConnectionRefusedError, OSError) as e:
        raise UnreachablePeer(f"Can't reach {remote!r}") from e
    connection = PeerConnection(
        reader=reader,
        writer=writer,
        aes_secret=aes_secret,
        mac_secret=mac_secret,
        egress_mac=egress_mac,
        ingress_mac=ingress_mac,
    )
    peer = factory.create_peer(
        remote=remote,
        connection=connection,
        inbound=False,
    )

    try:
        await peer.do_p2p_handshake()
        await peer.do_sub_proto_handshake()
    except Exception:
        # Note: This is one of two places where we manually handle closing the
        # reader/writer connection pair in the event of an error during the
        # peer connection and handshake process.
        # See `p2p.auth.handshake` for the other.
        if not reader.at_eof():
            reader.feed_eof()
        writer.close()
        await asyncio.sleep(0)
        raise

    return peer


class Connection(NamedTuple):
    remote: Node
    private_key: datatypes.PrivateKey
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    aes_secret: bytes
    mac_secret: bytes
    egress_mac: sha3.keccak_256
    ingress_mac: sha3.keccak_256


class Transport(BaseService):
    def __init__(self, connection: Connection, token: CancelToken = None):
        super().__init__(token)
        self.connection = connection
