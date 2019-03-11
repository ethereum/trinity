import asyncio
import functools
from typing import (
    Dict,
    List,
    NamedTuple,
    Tuple,
)
import uuid

from multiaddr import Multiaddr

from libp2p.p2pclient.datastructures import (
    PeerID,
    PeerInfo,
    StreamInfo,
)
from libp2p.p2pclient.exceptions import (
    ControlFailure,
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
        if n == 0:
            raise ValueError
        if n == -1:
            n = len(self._buf)
        # NOTE: polling the buffer, to simulate `asyncio.StreamReader.read`
        while len(self._buf) == 0:
            await asyncio.sleep(0.01)
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


class MockControlClient:

    _map_peer_id_to_control_client = None
    _uuid = None
    _peer_id = None
    _maddrs = None
    _peers = None

    handlers = None
    control_maddr = None
    listen_maddr = None

    def __init__(self, map_peer_id_to_control_client):
        """
        Args:
            map_peer_id_to_control_client (dict): The mutable mapping from
                `peer_id_to_immutable(peer_id)` to its corresponding `MockControlClient` object.
        """
        self._uuid = uuid.uuid1()
        self._peer_id = PeerID(self._uuid.bytes.ljust(32, b'\x00'))
        self._maddrs = [Multiaddr(f"/unix/maddr_{self._uuid}")]

        self._peers = set()
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
        except KeyError:
            # simulate that the daemon has rejected the stream for us,
            # so we shouldn't be called here.
            return
        await handler(stream_info, reader, writer)

    async def listen(self):
        pass

    async def close(self):
        self._map_peer_id_to_control_client.remove(self._peer_id)

    async def identify(self):
        return self._peer_id, self._maddrs

    async def connect(self, peer_id, maddrs):
        if peer_id not in self._map_peer_id_to_control_client:
            raise ControlFailure
        peer_client = self._map_peer_id_to_control_client[peer_id]
        if len(maddrs) == 0:
            raise ControlFailure
        correct_maddrs = peer_client._maddrs
        is_found = all([target_maddr in correct_maddrs for target_maddr in maddrs])
        if not is_found:
            raise ControlFailure
        self._peers.add(peer_id)
        peer_client._peers.add(self._peer_id)

    async def list_peers(self):
        return tuple(
            PeerInfo(
                peer_id,
                self._map_peer_id_to_control_client[peer_id]._maddrs,
            )
            for peer_id in self._peers
        )

    async def disconnect(self, peer_id):
        if peer_id not in self._map_peer_id_to_control_client:
            return
        peer = self._map_peer_id_to_control_client[peer_id]
        self._peers.remove(peer_id)
        peer._peers.remove(self._peer_id)

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

        # pre-check the handler map of our target peer.
        # if the peer hasn't registered a handler for the protocol, daemon returns error to us,
        # and then the p2pclient should raise `ControlFailure`
        if protocol_chosen not in peer_control_client.handlers:
            raise ControlFailure(
                f"the target peer doesn't register the protocol `{protocol_chosen}`"
            )

        # schedule `_dispatcher` of the target peer
        # your reader is its writer, vice versa.
        asyncio.ensure_future(
            peer_control_client._dispatcher(reader=writer, writer=reader)
        )

        stream_info_peer = StreamInfo(
            peer_id=peer_id,
            addr=peer_control_client._maddrs[0],  # chose the first one at my will
            proto=protocol_chosen,
        )
        return stream_info_peer, reader, writer

    async def stream_handler(self, proto, handler_cb):
        self.handlers[proto] = handler_cb


class MockPubSubClient:
    _topic_subscribed_streams: Dict[str, Tuple[MockStreamReaderWriter, MockStreamReaderWriter]]
    _control_client: MockControlClient
    _map_peer_id_to_pubsub_client: Dict[PeerID, 'MockPubSubClient']

    def __init__(
            self,
            control_client: MockControlClient,
            map_peer_id_to_pubsub_client: Dict[PeerID, 'MockPubSubClient']):
        self._topic_subscribed_streams = {}
        self._control_client = control_client
        self._map_peer_id_to_pubsub_client = map_peer_id_to_pubsub_client
        self._map_peer_id_to_pubsub_client[self._control_client._peer_id] = self

    @property
    def peer_id(self) -> PeerID:
        return self._control_client._peer_id

    @property
    def topics(self) -> Tuple[str, ...]:
        return tuple(self._topic_subscribed_streams.keys())

    async def get_topics(self) -> Tuple[str, ...]:
        return self.topics

    async def list_peers(self, topic: str) -> Tuple[PeerID, ...]:
        pinfos = await self._control_client.list_peers()
        peers_control = tuple(
            pinfo.peer_id
            for pinfo in pinfos
        )
        return tuple(
            peer_id
            for peer_id in peers_control
            if topic in self._map_peer_id_to_pubsub_client[peer_id].topics
        )

    async def publish(self, topic: str, data: bytes) -> None:
        nodes_to_publish = self._simple_bfs(topic)
        for peer_id in nodes_to_publish:
            pubsubc = self._map_peer_id_to_pubsub_client[peer_id]
            stream_pair = pubsubc._topic_subscribed_streams[topic]
            ps_msg = p2pd_pb.PSMessage(
                data=data,
                topicIDs=[topic],
            )
            setattr(ps_msg, 'from', self.peer_id.to_bytes())
            await write_pbmsg(stream_pair[0], ps_msg)

    def _simple_bfs(self, topic: str) -> Tuple[PeerID, ...]:
        visited_topic_nodes = set()
        queue = []
        queue.append(self.peer_id)
        while len(queue) != 0:
            current_peer_id = queue.pop(0)
            visited_topic_nodes.add(current_peer_id)
            pubsubc = self._map_peer_id_to_pubsub_client[current_peer_id]
            controlc = pubsubc._control_client
            for peer_id in tuple(controlc._peers):
                if peer_id in visited_topic_nodes:
                    continue
                # check if the peer runs pubsub
                if peer_id not in self._map_peer_id_to_pubsub_client:
                    continue
                peer_pubsubc = self._map_peer_id_to_pubsub_client[peer_id]
                # go through the peer only if it subscribes to the topic
                if topic in peer_pubsubc.topics:
                    queue.append(peer_id)
        return tuple(visited_topic_nodes)

    def _unsubscribe(self, topic):
        del self._topic_subscribed_streams[topic]

    async def subscribe(self, topic: str) -> Tuple[MockStreamReaderWriter, MockStreamReaderWriter]:
        if topic in self.topics:
            return
        reader = MockStreamReaderWriter()
        writer = MockStreamReaderWriter()
        setattr(writer, 'close', functools.partial(self._unsubscribe, topic=topic))
        stream_pair = (reader, writer)
        self._topic_subscribed_streams[topic] = stream_pair
        return stream_pair
