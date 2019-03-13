import asyncio

import pytest

import multihash

from libp2p.mock import (
    MockControlClient,
    MockStreamReaderWriter,
)
from libp2p.p2pclient.datastructures import (
    PeerID,
)
from libp2p.p2pclient.exceptions import (
    ControlFailure,
)
from libp2p.p2pclient.p2pclient import (
    read_pbmsg_safe,
)
from libp2p.p2pclient.pb import p2pd_pb2 as p2pd_pb


@pytest.fixture("module")
def content_id_bytes_example():
    return b'\x01r\x12 \xc0F\xc8\xechB\x17\xf0\x1b$\xb9\xecw\x11\xde\x11Cl\x8eF\xd8\x9a\xf1\xaeLa?\xb0\xaf\xe6K\x8b'  # noqa: E501


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
async def test_mock_dht_client_find_peer(controlcs, dhtcs):
    # test case: peer doesn't exist
    with pytest.raises(ControlFailure):
        await dhtcs[0].find_peer(PeerID(b'\x11' * 32))
    # test case: peer with no connections
    with pytest.raises(ControlFailure):
        await dhtcs[0].find_peer(dhtcs[1].peer_id)
    # 0 <-> 1 <-> 2
    await controlcs[0].connect(*(await controlcs[1].identify()))
    await controlcs[1].connect(*(await controlcs[2].identify()))
    await dhtcs[0].find_peer(dhtcs[2].peer_id)
    await dhtcs[0].find_peer(dhtcs[1].peer_id)


@pytest.mark.asyncio
async def test_mock_dht_client_find_peers_connected_to_peer(controlcs, dhtcs):
    pinfo_0_to_2 = await dhtcs[0].find_peers_connected_to_peer(dhtcs[2].peer_id)
    assert len(pinfo_0_to_2) == 0

    # 0 <-> 1 <-> 2
    await controlcs[0].connect(*(await controlcs[1].identify()))
    await controlcs[1].connect(*(await controlcs[2].identify()))

    pinfo_0_to_1 = await dhtcs[0].find_peers_connected_to_peer(dhtcs[1].peer_id)
    assert len(pinfo_0_to_1) == 2
    pinfo_0_to_2_second = await dhtcs[0].find_peers_connected_to_peer(dhtcs[2].peer_id)
    assert len(pinfo_0_to_2_second) == 1


@pytest.mark.asyncio
async def test_mock_dht_client_find_providers(controlcs, dhtcs, content_id_bytes_example):
    dhtcs[2]._provides_store.add(content_id_bytes_example)
    # test case: no route to the provider
    assert len(await dhtcs[0].find_providers(content_id_bytes_example, count=10)) == 0

    # 0 <-> 1 <-> 2
    await controlcs[0].connect(*(await controlcs[1].identify()))
    await controlcs[1].connect(*(await controlcs[2].identify()))
    # test case: with route to the provider
    assert len(await dhtcs[0].find_providers(content_id_bytes_example, count=10)) == 1
    # test case: multiple providers
    dhtcs[1]._provides_store.add(content_id_bytes_example)
    assert len(await dhtcs[0].find_providers(content_id_bytes_example, count=10)) == 2
    # test case: with `count` set to limit the number of the returned peer info
    assert len(await dhtcs[0].find_providers(content_id_bytes_example, count=1)) == 1


@pytest.mark.asyncio
async def test_mock_dht_client_provide(controlcs, dhtcs, content_id_bytes_example):
    assert len(dhtcs[1]._provides_store) == 0
    await dhtcs[1].provide(content_id_bytes_example)
    assert len(dhtcs[1]._provides_store) == 1
    await controlcs[0].connect(*(await controlcs[1].identify()))
    # test case: `find_providers` should work
    assert len(await dhtcs[0].find_providers(content_id_bytes_example, count=10)) == 1


@pytest.mark.asyncio
async def test_mock_dht_client_get_closest_peers(controlcs, dhtcs):
    await controlcs[0].connect(*(await controlcs[1].identify()))
    await controlcs[1].connect(*(await controlcs[2].identify()))
    mock_kvalue = 2
    dhtcs[0].KVALUE = mock_kvalue

    peer_id_bytes_2 = dhtcs[2].peer_id.to_bytes()
    lsb = peer_id_bytes_2[-1]
    if lsb % 2 == 0:
        lsb_flipped = lsb + 1
    else:
        lsb_flipped = lsb - 1
    key_nearest_peer_2 = peer_id_bytes_2[:-1] + bytes([lsb_flipped])
    closet_peers = await dhtcs[0].get_closest_peers(key_nearest_peer_2)
    # test case: `len(mock_routing_table_peers) > mock_kvalue`,
    #   so `len(closet_peers)` becomes `mock_kvalue`.
    assert len(closet_peers) == mock_kvalue
    assert closet_peers[0] == dhtcs[2].peer_id


@pytest.mark.asyncio
async def test_mock_dht_client_get_value(controlcs, dhtcs):
    key_0 = b'key_0'
    value_0 = b'value_0'

    # test case: not found
    with pytest.raises(ControlFailure):
        assert (await dhtcs[0].get_value(key_0)) == value_0

    # test case: key stored in local
    dhtcs[0]._values_store[key_0] = value_0
    assert (await dhtcs[0].get_value(key_0)) == value_0

    key_1 = b'key_1'
    value_1 = b'value_1'
    # test case: key stored in remote peer without paths to it
    dhtcs[2]._values_store[key_1] = value_1
    with pytest.raises(ControlFailure):
        await dhtcs[0].get_value(key_1)

    await controlcs[0].connect(*(await controlcs[1].identify()))
    await controlcs[1].connect(*(await controlcs[2].identify()))
    # test case: with paths to the remote node which stores the key
    assert (await dhtcs[0].get_value(key_1)) == value_1


@pytest.mark.asyncio
async def test_mock_dht_client_search_value(controlcs, dhtcs):
    key_0 = b'key_0'
    value_0 = b'value_0'
    value_1 = b'456789'
    value_2 = b'abcdefg'
    dhtcs[0]._values_store[key_0] = value_0
    dhtcs[1]._values_store[key_0] = value_1
    dhtcs[2]._values_store[key_0] = value_2

    assert len(await dhtcs[0].search_value(key_0)) == 1

    await controlcs[0].connect(*(await controlcs[1].identify()))
    await controlcs[1].connect(*(await controlcs[2].identify()))

    assert len(await dhtcs[0].search_value(key_0)) == 3


@pytest.mark.asyncio
async def test_mock_dht_client_put_value(controlcs, dhtcs):
    key_0 = b'key_0'
    value_0 = b'value_0'

    # test case: with no connections, only store to itself
    await dhtcs[0].put_value(key_0, value_0)
    assert key_0 in dhtcs[0]._values_store
    assert key_0 not in dhtcs[1]._values_store
    assert key_0 not in dhtcs[2]._values_store

    key_1 = b'key_1'
    value_1 = b'value_1'
    # test case: with connections, store to other nodes as well
    await controlcs[0].connect(*(await controlcs[1].identify()))
    await controlcs[1].connect(*(await controlcs[2].identify()))
    await dhtcs[0].put_value(key_1, value_1)
    assert key_1 in dhtcs[0]._values_store
    assert key_1 in dhtcs[1]._values_store
    assert key_1 in dhtcs[2]._values_store


@pytest.mark.asyncio
async def test_mock_dht_client_get_public_key(controlcs, dhtcs):
    # test case: local
    pubkey_0 = await dhtcs[0].get_public_key(dhtcs[0].peer_id)
    mh_digest_bytes = multihash.digest(pubkey_0.Data, multihash.Func.sha2_256).encode()
    assert dhtcs[0].peer_id.to_bytes() == mh_digest_bytes
    # test case: remote nodes without paths fail to get it
    with pytest.raises(ControlFailure):
        await dhtcs[2].get_public_key(dhtcs[0].peer_id)

    await controlcs[0].connect(*(await controlcs[1].identify()))
    await controlcs[1].connect(*(await controlcs[2].identify()))
    # test case: remote nodes with paths should be able to get it
    await dhtcs[2].get_public_key(dhtcs[0].peer_id)


@pytest.mark.asyncio
async def test_mock_connmgr_client_tag_peer(controlcs, connmgrcs):
    tag = "tag_123"
    await connmgrcs[0].tag_peer(
        controlcs[1]._peer_id,
        tag,
        1,
    )
    assert controlcs[1]._peer_id in connmgrcs[0]._map_peer_tag_weight
    assert tag in connmgrcs[0]._map_peer_tag_weight[controlcs[1]._peer_id]
    assert connmgrcs[0]._map_peer_tag_weight[controlcs[1]._peer_id][tag] == 1
    # test case: multiple tags
    tag_another = "tag_another"
    await connmgrcs[0].tag_peer(
        controlcs[1]._peer_id,
        tag_another,
        2,
    )
    assert controlcs[1]._peer_id in connmgrcs[0]._map_peer_tag_weight
    assert tag_another in connmgrcs[0]._map_peer_tag_weight[controlcs[1]._peer_id]
    assert connmgrcs[0]._map_peer_tag_weight[controlcs[1]._peer_id][tag_another] == 2
    # test case: multiple peers
    await connmgrcs[0].tag_peer(
        controlcs[2]._peer_id,
        tag,
        3,
    )
    assert controlcs[1]._peer_id in connmgrcs[0]._map_peer_tag_weight
    assert tag in connmgrcs[0]._map_peer_tag_weight[controlcs[2]._peer_id]
    assert connmgrcs[0]._map_peer_tag_weight[controlcs[2]._peer_id][tag] == 3


@pytest.mark.asyncio
async def test_mock_connmgr_client_untag_peer(controlcs, connmgrcs):
    tag = "tag_123"
    connmgrcs[0]._map_peer_tag_weight[controlcs[1]._peer_id][tag] = 1
    tag_another = "tag_another"
    connmgrcs[0]._map_peer_tag_weight[controlcs[1]._peer_id][tag_another] = 2
    await connmgrcs[0].untag_peer(
        controlcs[1]._peer_id,
        tag,
    )
    assert tag not in connmgrcs[0]._map_peer_tag_weight[controlcs[1]._peer_id]
    assert tag_another in connmgrcs[0]._map_peer_tag_weight[controlcs[1]._peer_id]
    assert connmgrcs[0]._map_peer_tag_weight[controlcs[1]._peer_id][tag_another] == 2


@pytest.mark.asyncio
async def test_mock_connmgr_client_trim(controlcs, connmgrcs):
    # 0 <-> 1 <-> 2
    await controlcs[0].connect(*(await controlcs[1].identify()))
    await controlcs[1].connect(*(await controlcs[2].identify()))

    tag = "tag_123"
    await connmgrcs[1].tag_peer(
        controlcs[0]._peer_id,
        tag,
        1,
    )
    tag_another = "tag_another"
    await connmgrcs[1].tag_peer(
        controlcs[2]._peer_id,
        tag_another,
        2,
    )

    # ensure #peers will be trimmed down to 1
    connmgrcs[1].low_water_mark = 1
    await connmgrcs[1].trim()

    peers_1 = await controlcs[1].list_peers()
    assert len(peers_1) == 1 and peers_1[0].peer_id == controlcs[2]._peer_id
