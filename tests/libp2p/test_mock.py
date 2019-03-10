import asyncio

import pytest

from libp2p.mock import (
    MockControlClient,
    MockNetwork,
    MockStreamReaderWriter,
)

from libp2p.p2pclient.datastructures import (
    PeerID,
)


def test_mock_stream_reader_writer_write():
    rwtor = MockStreamReaderWriter()
    rwtor.write(b'123')
    rwtor.write(b'')
    rwtor.write(b'456')
    rwtor.write(b'\n')
    rwtor._buf == b'123456\n'


@pytest.mark.asyncio
async def test_mock_stream_reader_writer_read():
    rwtor = MockStreamReaderWriter()
    rwtor._buf = b"123\n456\n"
    assert await rwtor.read(4) == b"123\n"
    assert await rwtor.read() == b"456\n"


@pytest.mark.asyncio
async def test_mock_stream_reader_writer_readexactly():
    rwtor = MockStreamReaderWriter()
    rwtor._buf = b"123\n456\n"
    assert await rwtor.readexactly(1) == b'1'
    assert await rwtor.readexactly(3) == b'23\x0a'
    assert await rwtor.readexactly(3) == b'456'
    with pytest.raises(asyncio.IncompleteReadError):
        await rwtor.readexactly(3)


@pytest.mark.asyncio
async def test_mock_stream_reader_writer_interleaving_read_and_write():
    rwtor = MockStreamReaderWriter()
    rwtor.write(b'1234')
    assert await rwtor.read(2) == b'12'
    assert await rwtor.read(1) == b'3'
    rwtor.write(b'5')
    rwtor.write(b'67')
    assert await rwtor.read() == b'4567'


peer_id_0 = PeerID(b'\x00' * 32)
peer_id_1 = PeerID(b'\x11' * 32)
peer_id_2 = PeerID(b'\x22' * 32)


def test_mock_network_add_node():
    n = MockNetwork()
    assert len(n._nodes) == 0
    n.add_node(peer_id_0)
    assert len(n._nodes) == 1
    n.add_node(peer_id_1)
    assert len(n._nodes) == 2


def test_mock_network_property_nodes():
    n = MockNetwork()
    assert len(n.nodes) == 0
    n._nodes = set([peer_id_0, peer_id_1])
    assert len(n.nodes) == 2


def test_mock_network_remove_node():
    n = MockNetwork()
    assert len(n.nodes) == 0
    n.remove_node(peer_id_0)
    assert len(n.nodes) == 0
    n.add_node(peer_id_0)
    n.add_node(peer_id_1)
    n.remove_node(peer_id_0)
    assert len(n.nodes) == 1 and n.nodes[0] == peer_id_1
    n.remove_node(peer_id_0)
    assert len(n.nodes) == 1 and n.nodes[0] == peer_id_1
    n.remove_node(peer_id_1)
    assert len(n.nodes) == 0


def test_mock_network_connect():
    n = MockNetwork()
    n.connect(peer_id_0, peer_id_1)
    # test case: ensure `connect` doesn't accidentally add nodes originally not in the network
    assert (peer_id_0 not in n.nodes) and (peer_id_1 not in n.nodes)
    n.add_node(peer_id_0)
    n.connect(peer_id_0, peer_id_1)
    assert peer_id_1 not in n.nodes
    assert len(n._conns) == 0, "there should not be conns, if one of the peer is not in the network"
    n.add_node(peer_id_1)
    n.connect(peer_id_0, peer_id_1)
    assert len(n._conns) == 1, "nodes in the network should be able to connect"
    n.connect(peer_id_0, peer_id_1)
    assert len(n._conns) == 1, "there should only be one connection between every two peers"
    n.add_node(peer_id_2)
    n.connect(peer_id_0, peer_id_2)
    assert len(n._conns) == 2, "peer 0 should be able to connect multiple peers"


def test_mock_network_list_peers():
    n = MockNetwork()
    n.add_node(peer_id_0)
    n.add_node(peer_id_1)
    n.add_node(peer_id_2)
    assert len(n.list_peers(peer_id_0)) == 0, "peer 0 should have 0 peers before connecting"
    n.connect(peer_id_0, peer_id_1)
    peers_0 = n.list_peers(peer_id_0)
    assert len(peers_0) == 1 and peers_0[0] == peer_id_1, "peer 0 should have 1 peers after connecting"  # noqa: E501
    peers_1 = n.list_peers(peer_id_1)
    assert len(peers_1) == 1 and peers_1[0] == peer_id_0, "peer 1 should have 1 peers after connecting"  # noqa: E501
    n.connect(peer_id_0, peer_id_2)
    assert len(n.list_peers(peer_id_0)) == 2, "peer 0 should have 2 peers before connecting"
    assert len(n.list_peers(peer_id_1)) == 1, "peer 1 should have 1 peers after connecting"
    assert len(n.list_peers(peer_id_2)) == 1, "peer 1 should have 1 peers after connecting"


def test_mock_network_disconnect():
    n = MockNetwork()
    n.disconnect(peer_id_0, peer_id_1)
    # test case: ensure `disconnect` does not accidentally add nodes
    assert len(n.nodes) == 0
    n.add_node(peer_id_0)
    n.disconnect(peer_id_0, peer_id_1)
    assert len(n.nodes) == 1 and n.nodes[0] == peer_id_0
    assert len(n._conns) == 0
    n.add_node(peer_id_1)
    n.add_node(peer_id_2)
    n.connect(peer_id_0, peer_id_1)
    n.connect(peer_id_0, peer_id_2)
    assert len(n._conns) == 2
    n.disconnect(peer_id_0, peer_id_1)
    assert len(n._conns) == 1, "should have disconnected successfully"
    n.disconnect(peer_id_0, peer_id_1)
    assert len(n._conns) == 1, "should not affect other connections when disconnect the same connections twice"  # noqa: E501
    n.disconnect(peer_id_0, peer_id_2)
    assert len(n._conns) == 0, " should have disconnected successfully"


@pytest.mark.asyncio
async def test_mock_control_client():
    n = MockNetwork()
    map_pid_client = {}
    c0 = MockControlClient(network=n, map_peer_id_to_control_client=map_pid_client)
    c1 = MockControlClient(network=n, map_peer_id_to_control_client=map_pid_client)
    peer_id_0, _ = await c0.identify()
    peer_id_1, maddrs_1 = await c1.identify()
    assert len(await c0.list_peers()) == 0
    await c0.connect(peer_id_1, maddrs_1)
    assert len(await c0.list_peers()) == 1
    await c0.disconnect(peer_id_1)
    assert len(await c0.list_peers()) == 0

    assert len(map_pid_client) == 2

    proto = "proto_123"
    data = b'data_123'

    # test `stream_open` and `stream_handler`
    event = asyncio.Event()

    async def handler_cb(stream_info, reader, writer):
        received_data = await reader.read(len(data))
        assert data == received_data
        event.set()

    await c0.stream_handler(proto, handler_cb)
    _, _, writer = await c1.stream_open(peer_id_0, [proto])
    writer.write(data)
    await event.wait()
    writer.close()
