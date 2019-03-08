import asyncio
from io import (
    BytesIO,
)
from typing import (
    NamedTuple,
)
from collections import (
    UserDict,
    MutableMapping,
)
import uuid

import pytest

from multiaddr import Multiaddr

from libp2p.host import (
    DaemonHost,
)

from libp2p.p2pclient.datastructures import (
    PeerID,
    PeerInfo,
    StreamInfo,
)
from libp2p.p2pclient.exceptions import (
    ControlFailure,
    DispatchFailure,
)
from libp2p.p2pclient.p2pclient import (
    read_pbmsg_safe,
    write_pbmsg,
)
from libp2p.p2pclient.pb import p2pd_pb2 as p2pd_pb


class MockStreamReaderWriter:
    _buf: bytes

    def __init__(self):
        self._buf = b""

    def write(self, data):
        self._buf = self._buf + data

    async def read(self, n=-1):
        if n == -1:
            n = len(self._buf)
        data = self._buf[:n]
        self._buf = self._buf[n:]
        return data

    async def readexactly(self, n):
        data = await self.read(n)
        if len(data) != n:
            raise asyncio.IncompleteReadError(partial=data, expected=n)
        return data

    async def drain(self):
        # do nothing
        pass

    def close(self):
        pass


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


class MockNetwork:
    _nodes = None
    _conns = None

    def __init__(self):
        self._nodes = set()
        self._conns = set()

    def add_node(self, peer_id):
        self._nodes.add(peer_id)

    def remove_node(self, peer_id):
        try:
            self._nodes.remove(peer_id)
        except KeyError:
            pass
        conns_without_peer = set([
            conn
            for conn in self._conns
            if (conn[0] != peer_id) and (conn[1] != peer_id)
        ])
        self._conns = conns_without_peer

    @property
    def nodes(self):
        return tuple(self._nodes)

    @staticmethod
    def _to_internal_tuple(peer_id_0, peer_id_1):
        # ensure the order
        return tuple(set([peer_id_0, peer_id_1]))

    def _can_establish_conn(self, peer_id_0, peer_id_1):
        if peer_id_0 == peer_id_1:
            return False
        if (peer_id_0 not in self._nodes) or (peer_id_1 not in self._nodes):
            return False
        return True

    def connect(self, peer_id_0, peer_id_1):
        if not self._can_establish_conn(peer_id_0, peer_id_1):
            return
        self._conns.add(self._to_internal_tuple(peer_id_0, peer_id_1))

    def disconnect(self, peer_id_0, peer_id_1):
        if not self._can_establish_conn(peer_id_0, peer_id_1):
            return
        try:
            self._conns.remove(self._to_internal_tuple(peer_id_0, peer_id_1))
        except KeyError:
            pass

    def list_peers(self, peer_id):
        lefts = tuple(conn[0] for conn in self._conns if conn[1] == peer_id)
        rights = tuple([conn[1] for conn in self._conns if conn[0] == peer_id])
        return tuple(set(lefts + rights))


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


class MockControlClient:

    _network = None
    _map_peer_id_to_control_client = None
    _uuid = None
    _peer_id = None
    _maddrs = None

    handlers = None
    control_maddr = None
    listen_maddr = None

    def __init__(self, network, map_peer_id_to_control_client):
        """
        Args:
            network (MockNetwork): The mock network
            map_peer_id_to_control_client (dict): The mutable mapping from
                `peer_id_to_immutable(peer_id)` to its corresponding `MockControlClient` object.
        """
        self._uuid = uuid.uuid1()
        self._peer_id = PeerID(self._uuid.bytes.ljust(32, b'\x00'))
        self._maddrs = [Multiaddr(f"/unix/maddr_{self._uuid}")]

        self._network = network
        self._network.add_node(self._peer_id)
        self._map_peer_id_to_control_client = map_peer_id_to_control_client
        self._map_peer_id_to_control_client[self._peer_id] = self

        self.control_maddr = f"/unix/control_{self._uuid}"
        self.listen_maddr = f"/unix/listen__{self._uuid}"
        self.handlers = {}

    async def _dispatcher(self, reader, writer):
        pb_stream_info = p2pd_pb.StreamInfo()
        await read_pbmsg_safe(reader, pb_stream_info)
        stream_info = StreamInfo.from_pb(pb_stream_info)
        try:
            handler = self.handlers[stream_info.proto]
        except KeyError as e:
            # should never enter here... daemon should reject the stream for us.
            writer.close()
            raise DispatchFailure(e)
        await handler(stream_info, reader, writer)

    async def listen(self):
        pass

    async def close(self):
        pass

    async def identify(self):
        return self._peer_id, self._maddrs

    async def connect(self, peer_id, maddrs):
        self._network.connect(self._peer_id, peer_id)

    async def list_peers(self):
        peer_ids = self._network.list_peers(self._peer_id)
        return tuple(
            PeerInfo(
                peer_id,
                self._map_peer_id_to_control_client[peer_id],
            )
            for peer_id in peer_ids
        )

    async def disconnect(self, peer_id):
        self._network.disconnect(self._peer_id, peer_id)

    async def stream_open(self, peer_id, protocols):
        if len(protocols) == 0:
            raise ControlFailure(f'len(protocols) should not be 0, protocols={protocols}')

        protocol_chosen = protocols[0]

        reader = MockStreamReaderWriter()
        writer = MockStreamReaderWriter()

        stream_info_pb = StreamInfo(
            peer_id=self._peer_id,
            addr=self._maddrs[0],
            proto=protocol_chosen,
        ).to_pb()
        await write_pbmsg(writer, stream_info_pb)

        if peer_id not in self._map_peer_id_to_control_client:
            raise ControlFailure(f"failed to find the peer {peer_id}")
        peer_control_client = self._map_peer_id_to_control_client[peer_id]

        # schedule `_dispatcher` of the target peer
        # your reader is its writer, vice versa.
        asyncio.ensure_future(
            peer_control_client._dispatcher(reader=writer, writer=reader)
        )
        # and yield to it
        await asyncio.sleep(0.01)

        stream_info_peer = StreamInfo(
            peer_id=peer_id,
            addr=peer_control_client._maddrs,
            proto=protocol_chosen,
        )
        return stream_info_peer, reader, writer

    async def stream_handler(self, proto, handler_cb):
        self.handlers[proto] = handler_cb


@pytest.mark.asyncio
async def test_daemon_host():
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

    event = asyncio.Event()

    async def handler_cb(stream_info, reader, writer):
        # received_data = await reader.read(len(data))
        # print("!@#", await reader.read(), ", ", reader._buf)
        # assert data == received_data
        event.set()

    await c0.stream_handler(proto, handler_cb)
    _, _, writer = await c1.stream_open(peer_id_0, [proto])
    # writer.write(data)
    await event.wait()
    writer.close()
