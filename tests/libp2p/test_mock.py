import asyncio

import pytest

from libp2p.mock import (
    MockControlClient,
    MockStreamReaderWriter,
)
from libp2p.p2pclient.p2pclient import (
    read_pbmsg_safe,
)
from libp2p.p2pclient.pb import p2pd_pb2 as p2pd_pb


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


@pytest.mark.asyncio
async def test_mock_control_client():
    map_pid_client = {}
    c0 = MockControlClient(map_peer_id_to_control_client=map_pid_client)
    c1 = MockControlClient(map_peer_id_to_control_client=map_pid_client)
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


@pytest.mark.asyncio
async def test_mock_pubsub_client(pubsubcs):
    assert len(pubsubcs[0].topics) == 0
    assert pubsubcs[0].peer_id == pubsubcs[0]._control_client._peer_id
    # 0 <-> 1 <-> 2
    await pubsubcs[0]._control_client.connect(*(await pubsubcs[1]._control_client.identify()))
    await pubsubcs[1]._control_client.connect(*(await pubsubcs[2]._control_client.identify()))
    assert len(pubsubcs[0]._map_peer_id_to_pubsub_client) == 3
    topic = "topic_123"
    # test case: `list_peers`
    assert len(await pubsubcs[0].list_peers(topic)) == 0
    assert len(await pubsubcs[1].list_peers(topic)) == 0
    assert len(await pubsubcs[2].list_peers(topic)) == 0
    # test case: `subscribe`
    stream_pair_0 = await pubsubcs[0].subscribe(topic)
    assert len(await pubsubcs[0].list_peers(topic)) == 0
    assert len(await pubsubcs[1].list_peers(topic)) == 1
    assert len(await pubsubcs[2].list_peers(topic)) == 0
    stream_pair_1 = await pubsubcs[1].subscribe(topic)
    stream_pair_2 = await pubsubcs[2].subscribe(topic)
    assert len(await pubsubcs[0].list_peers(topic)) == 1
    assert len(await pubsubcs[1].list_peers(topic)) == 2
    assert len(await pubsubcs[2].list_peers(topic)) == 1
    # test case: `get_topics`
    assert await pubsubcs[0].get_topics() == (topic,)
    # test case: `publish`
    data = b'123'
    await pubsubcs[0].publish(topic, data)
    ps_msg_0 = p2pd_pb.PSMessage()
    await read_pbmsg_safe(stream_pair_0[0], ps_msg_0)
    assert ps_msg_0.data == data
    ps_msg_1 = p2pd_pb.PSMessage()
    await read_pbmsg_safe(stream_pair_1[0], ps_msg_1)
    assert ps_msg_1.data == data
    ps_msg_2 = p2pd_pb.PSMessage()
    await read_pbmsg_safe(stream_pair_2[0], ps_msg_2)
    assert ps_msg_2.data == data
    # test case: unsubscribe by `writer.close`
    stream_pair_0[1].close()
    assert len(await pubsubcs[0].get_topics()) == 0


@pytest.mark.asyncio
async def test_mock_dht_client_find_peer(dhtcs):
    # dhtcs[0].find_peer()
    pass
