import asyncio
import pytest

from async_service.asyncio import background_asyncio_service

from eth_keys import keys

from eth.chains.ropsten import RopstenChain, ROPSTEN_GENESIS_HEADER
from eth.db.atomic import AtomicDB
from eth.db.chain import ChainDB

from p2p.auth import HandshakeInitiator, _handshake
from p2p.connection import Connection
from p2p.handshake import negotiate_protocol_handshakes
from p2p.tools.factories import (
    get_open_port,
    DevP2PHandshakeParamsFactory,
    NodeFactory,
)
from p2p.tools.paragon import (
    ParagonContext,
    ParagonPeer,
    ParagonPeerPool,
    ParagonPeerFactory,
)
from p2p.transport import Transport

from trinity.constants import TO_NETWORKING_BROADCAST_CONFIG
from trinity.db.eth1.header import AsyncHeaderDB
from trinity.protocol.common.events import ConnectToNodeCommand
from trinity.server import BaseServer


from tests.p2p.auth_constants import eip8_values
from tests.core.integration_test_helpers import (
    run_peer_pool_event_server,
)


port = get_open_port()
NETWORK_ID = 99
RECEIVER_PRIVKEY = keys.PrivateKey(eip8_values['receiver_private_key'])
RECEIVER_PUBKEY = RECEIVER_PRIVKEY.public_key

INITIATOR_PRIVKEY = keys.PrivateKey(eip8_values['initiator_private_key'])
INITIATOR_PUBKEY = INITIATOR_PRIVKEY.public_key


class ParagonServer(BaseServer):
    def _make_peer_pool(self):
        return ParagonPeerPool(
            privkey=self.privkey,
            context=ParagonContext(),
            event_bus=self.event_bus,
        )


def get_server(privkey, address, event_bus):
    base_db = AtomicDB()
    headerdb = AsyncHeaderDB(base_db)
    chaindb = ChainDB(base_db)
    chaindb.persist_header(ROPSTEN_GENESIS_HEADER)
    chain = RopstenChain(base_db)
    server = ParagonServer(
        privkey=privkey,
        port=address.tcp_port,
        chain=chain,
        chaindb=chaindb,
        headerdb=headerdb,
        base_db=base_db,
        network_id=NETWORK_ID,
        event_bus=event_bus,
    )
    return server


@pytest.fixture
def receiver_remote():
    return NodeFactory(
        pubkey=RECEIVER_PUBKEY,
        address__ip='127.0.0.1',
    )


@pytest.fixture
async def server(event_bus, receiver_remote):
    server = get_server(RECEIVER_PRIVKEY, receiver_remote.address, event_bus)
    async with background_asyncio_service(server):
        yield server


@pytest.mark.asyncio
async def test_server_incoming_connection(server, receiver_remote):
    use_eip8 = False
    initiator = HandshakeInitiator(receiver_remote, INITIATOR_PRIVKEY, use_eip8)
    initiator_remote = NodeFactory(
        pubkey=INITIATOR_PUBKEY,
        address__ip='127.0.0.1',
    )
    for _ in range(10):
        # The server isn't listening immediately so we give it a short grace
        # period while trying to connect.
        try:
            reader, writer = await initiator.connect()
        except ConnectionRefusedError:
            await asyncio.sleep(0)
        else:
            break
    else:
        raise AssertionError("Unable to connect within 10 loops")
    # Send auth init message to the server, then read and decode auth ack
    aes_secret, mac_secret, egress_mac, ingress_mac = await _handshake(
        initiator, reader, writer)

    transport = Transport(
        remote=receiver_remote,
        private_key=initiator.privkey,
        reader=reader,
        writer=writer,
        aes_secret=aes_secret,
        mac_secret=mac_secret,
        egress_mac=egress_mac,
        ingress_mac=ingress_mac,
    )

    factory = ParagonPeerFactory(
        initiator.privkey,
        context=ParagonContext(),
    )
    handshakers = await factory.get_handshakers()
    devp2p_handshake_params = DevP2PHandshakeParamsFactory(
        listen_port=initiator_remote.address.tcp_port,
    )

    multiplexer, devp2p_receipt, protocol_receipts = await negotiate_protocol_handshakes(
        transport=transport,
        p2p_handshake_params=devp2p_handshake_params,
        protocol_handshakers=handshakers,
    )
    connection = Connection(
        multiplexer=multiplexer,
        devp2p_receipt=devp2p_receipt,
        protocol_receipts=protocol_receipts,
        is_dial_out=False,
    )
    initiator_peer = factory.create_peer(connection=connection)

    # wait for peer to be processed
    for _ in range(100):
        if len(server.peer_pool) > 0:
            break
        await asyncio.sleep(0)

    assert len(server.peer_pool.connected_nodes) == 1
    receiver_peer = list(server.peer_pool.connected_nodes.values())[0]
    assert isinstance(receiver_peer, ParagonPeer)
    assert initiator_peer.sub_proto is not None
    assert initiator_peer.sub_proto.name == receiver_peer.sub_proto.name
    assert initiator_peer.sub_proto.version == receiver_peer.sub_proto.version
    assert initiator_peer.remote.pubkey == RECEIVER_PRIVKEY.public_key

    # Our connections are created manually and we don't call run() on them, so we need to stop the
    # background streaming task or else we'll get asyncio warnings about task exceptions not being
    # retrieved.
    await initiator_peer.connection._multiplexer.stop_streaming()
    await receiver_peer.connection._multiplexer.stop_streaming()


@pytest.mark.asyncio
async def test_peer_pool_connect(monkeypatch, server, receiver_remote):
    peer_started = asyncio.Event()

    start_peer = server.peer_pool.start_peer

    async def mock_start_peer(peer):
        # Call the original start_peer() so that we create and run Peer/Connection objects,
        # which will ensure everything is cleaned up properly.
        await start_peer(peer)
        peer_started.set()

    monkeypatch.setattr(server.peer_pool, 'start_peer', mock_start_peer)

    initiator_peer_pool = ParagonPeerPool(
        privkey=INITIATOR_PRIVKEY,
        context=ParagonContext(),
    )
    nodes = [receiver_remote]
    async with background_asyncio_service(initiator_peer_pool) as manager:
        await manager.wait_started()
        await initiator_peer_pool.connect_to_nodes(nodes)

        await asyncio.wait_for(peer_started.wait(), timeout=10)

        assert len(initiator_peer_pool.connected_nodes) == 1


@pytest.mark.asyncio
async def test_peer_pool_answers_connect_commands(event_bus, server, receiver_remote):
    # This is the PeerPool which will accept our message and try to connect to {server}
    initiator_peer_pool = ParagonPeerPool(
        privkey=INITIATOR_PRIVKEY,
        context=ParagonContext(),
        event_bus=event_bus,
    )
    async with background_asyncio_service(initiator_peer_pool) as manager:
        await manager.wait_started()
        async with run_peer_pool_event_server(event_bus, initiator_peer_pool):

            assert len(server.peer_pool.connected_nodes) == 0

            await event_bus.wait_until_any_endpoint_subscribed_to(ConnectToNodeCommand)
            await event_bus.broadcast(
                ConnectToNodeCommand(receiver_remote),
                TO_NETWORKING_BROADCAST_CONFIG
            )

            # This test was maybe 30% flaky at 0.1 sleep, so wait in a loop.
            for _ in range(5):
                await asyncio.sleep(0.1)
                if len(server.peer_pool.connected_nodes) == 1:
                    break
            else:
                assert len(server.peer_pool.connected_nodes) == 1
