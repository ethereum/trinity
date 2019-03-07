import io
from typing import (
    NamedTuple,
)
import uuid

import pytest

from multiaddr import Multiaddr

from libp2p.host import (
    DaemonHost,
)

from libp2p.p2pclient.datastructures import (
    PeerID,
    StreamInfo,
)
from libp2p.p2pclient.p2pclient import (
    read_pbmsg_safe,
    write_pbmsg,
)
from libp2p.p2pclient.pb import p2pd_pb2 as p2pd_pb
from libp2p.p2pclient.exceptions import (
    DispatchFailure,
)


class MockStreamReader(io.BytesIO):
    async def readexactly(self, n):
        return self.read(n)


class MockStreamWriter(io.BytesIO):
    async def drain(self):
        # do nothing
        pass


class MockConn(NamedTuple):
    reader: MockStreamReader
    writer: MockStreamWriter


class MockNetwork:
    _nodes = None
    _conns = None

    def __init__(self):
        self._nodes = set()
        self._conns = set()

    @staticmethod
    def _to_internal_peer_id(peer_id):
        return peer_id.to_bytes()

    @staticmethod
    def _to_peer_id(peer_id_internal):
        return PeerID(peer_id_internal)

    def add_node(self, peer_id):
        peer_id_internal = self._to_internal_peer_id(peer_id)
        self._nodes.add(peer_id_internal)

    def remove_node(self, peer_id):
        peer_id_internal = self._to_internal_peer_id(peer_id)
        try:
            self._nodes.remove(peer_id_internal)
        except KeyError:
            pass
        conns_without_peer = set([
            conn
            for conn in self._conns
            if (conn[0] != peer_id_internal) and (conn[1] != peer_id_internal)
        ])
        self._conns = conns_without_peer

    @property
    def nodes(self):
        return tuple(
            self._to_peer_id(peer)
            for peer in self._nodes
        )

    @staticmethod
    def _to_internal_tuple(peer_id_0_internal, peer_id_1_internal):
        # ensure the order
        return tuple(set([peer_id_0_internal, peer_id_1_internal]))

    def _can_establish_conn(self, peer_id_0_internal, peer_id_1_internal):
        if peer_id_0_internal == peer_id_1_internal:
            return False
        if (peer_id_0_internal not in self._nodes) or (peer_id_1_internal not in self._nodes):
            return False
        return True

    def connect(self, peer_id_0, peer_id_1):
        peer_id_0_internal = self._to_internal_peer_id(peer_id_0)
        peer_id_1_internal = self._to_internal_peer_id(peer_id_1)
        if not self._can_establish_conn(peer_id_0_internal, peer_id_1_internal):
            return
        self._conns.add(self._to_internal_tuple(peer_id_0_internal, peer_id_1_internal))

    def disconnect(self, peer_id_0, peer_id_1):
        peer_id_0_internal = self._to_internal_peer_id(peer_id_0)
        peer_id_1_internal = self._to_internal_peer_id(peer_id_1)
        if not self._can_establish_conn(peer_id_0_internal, peer_id_1_internal):
            return
        try:
            self._conns.remove(self._to_internal_tuple(peer_id_0_internal, peer_id_1_internal))
        except KeyError:
            pass

    def list_peers(self, peer_id):
        peer_id_internal = self._to_internal_peer_id(peer_id)
        lefts = tuple(conn[0] for conn in self._conns if conn[1] == peer_id_internal)
        rights = tuple([conn[1] for conn in self._conns if conn[0] == peer_id_internal])
        peers_id_internal_set = set(lefts + rights)
        return tuple(
            self._to_peer_id(value)
            for value in peers_id_internal_set
        )


peer_id_0 = PeerID(b'\x00' * 32)
peer_id_1 = PeerID(b'\x11' * 32)
peer_id_2 = PeerID(b'\x22' * 32)


def test_mock_network_add_node():
    n = MockNetwork()
    assert len(n._nodes) == 0
    n.add_node(peer_id_0)
    assert len(n._nodes) == 1 and tuple(n._nodes)[0] == peer_id_0
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

    map_conn = None
    handlers = None
    control_maddr = None
    listen_maddr = None

    def __init__(self, network):
        self._network = network
        self._uuid = uuid.uuid1()
        self._peer_id = PeerID(self._uuid.bytes.rjust(32, b'\x00'))
        self._maddrs = [Multiaddr(f"/unix/maddr_{self._uuid}")]
        self.control_maddr = f"/unix/control_{self._uuid}"
        self.listen_maddr = f"/unix/listen__{self._uuid}"
        self.handlers = {}
        self.map_conn = {}

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
        reader, writer = await self.client.open_connection()

        maddrs_bytes = [binascii.unhexlify(i.to_bytes()) for i in maddrs]
        connect_req = p2pd_pb.ConnectRequest(
            peer=peer_id.to_bytes(),
            addrs=maddrs_bytes,
        )
        req = p2pd_pb.Request(
            type=p2pd_pb.Request.CONNECT,
            connect=connect_req,
        )
        await write_pbmsg(writer, req)

        resp = p2pd_pb.Response()
        await read_pbmsg_safe(reader, resp)
        writer.close()
        raise_if_failed(resp)

    async def list_peers(self):
        req = p2pd_pb.Request(
            type=p2pd_pb.Request.LIST_PEERS,
        )
        reader, writer = await self.client.open_connection()
        await write_pbmsg(writer, req)
        resp = p2pd_pb.Response()
        await read_pbmsg_safe(reader, resp)
        writer.close()
        raise_if_failed(resp)

        peers = tuple(PeerInfo.from_pb(pinfo) for pinfo in resp.peers)
        return peers

    async def disconnect(self, peer_id):
        disconnect_req = p2pd_pb.DisconnectRequest(
            peer=peer_id.to_bytes(),
        )
        req = p2pd_pb.Request(
            type=p2pd_pb.Request.DISCONNECT,
            disconnect=disconnect_req,
        )
        reader, writer = await self.client.open_connection()
        await write_pbmsg(writer, req)
        resp = p2pd_pb.Response()
        await read_pbmsg_safe(reader, resp)
        writer.close()
        raise_if_failed(resp)

    async def stream_open(self, peer_id, protocols):
        reader, writer = await self.client.open_connection()

        stream_open_req = p2pd_pb.StreamOpenRequest(
            peer=peer_id.to_bytes(),
            proto=list(protocols),
        )
        req = p2pd_pb.Request(
            type=p2pd_pb.Request.STREAM_OPEN,
            streamOpen=stream_open_req,
        )
        await write_pbmsg(writer, req)

        resp = p2pd_pb.Response()
        await read_pbmsg_safe(reader, resp)
        raise_if_failed(resp)

        pb_stream_info = resp.streamInfo
        stream_info = StreamInfo.from_pb(pb_stream_info)

        return stream_info, reader, writer

    async def stream_handler(self, proto, handler_cb):
        reader, writer = await self.client.open_connection()

        listen_path_maddr_bytes = binascii.unhexlify(self.listen_maddr.to_bytes())
        stream_handler_req = p2pd_pb.StreamHandlerRequest(
            addr=listen_path_maddr_bytes,
            proto=[proto],
        )
        req = p2pd_pb.Request(
            type=p2pd_pb.Request.STREAM_HANDLER,
            streamHandler=stream_handler_req,
        )
        await write_pbmsg(writer, req)

        resp = p2pd_pb.Response()
        await read_pbmsg_safe(reader, resp)
        writer.close()
        raise_if_failed(resp)

        # if success, add the handler to the dict
        self.handlers[proto] = handler_cb


@pytest.mark.asyncio
async def test_daemon_host():
    pass
