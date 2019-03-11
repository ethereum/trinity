import asyncio

import pytest

from libp2p.mock import (
    MockControlClient,
)
from libp2p.host import (
    DaemonHost,
)


@pytest.fixture('module')
def num_hosts():
    return 2


@pytest.fixture
def hosts(num_hosts):
    map_peer_id_to_control_client = {}
    return tuple(
        DaemonHost(
            MockControlClient(
                map_peer_id_to_control_client=map_peer_id_to_control_client,
            )
        )
        for _ in range(num_hosts)
    )


@pytest.mark.asyncio
async def test_daemon_host_get_peer_info(hosts):
    assert hosts[0].peer_info is None, "`peer_info` should be set lazily"
    pinfo = await hosts[0].get_peer_info()
    assert pinfo is not None, "`get_peer_info` should never return None"
    assert pinfo == hosts[0].peer_info, "`peer_info` should be set after calling `get_peer_info`"


@pytest.mark.asyncio
async def test_daemon_host_get_id(hosts):
    await hosts[0].get_id() is not None


@pytest.mark.asyncio
async def test_daemon_host_get_addrs(hosts):
    # should at least have one maddr listened
    assert len(await hosts[0].get_addrs()) != 0


@pytest.mark.asyncio
async def test_daemon_host_list_peers(hosts):
    assert len(await hosts[0].list_peers()) == 0
    # mock
    hosts[0].control_client._peers.add(await hosts[1].get_id())
    assert len(await hosts[0].list_peers()) == 1


@pytest.mark.asyncio
async def test_daemon_host_connect(hosts):
    peer_id_0 = await hosts[0].get_id()
    peer_id_1 = await hosts[1].get_id()
    pinfo_1 = await hosts[1].get_peer_info()
    await hosts[0].connect(pinfo_1)
    peers_ids_0 = tuple(
        pinfo.peer_id
        for pinfo in await hosts[0].list_peers()
    )
    assert peer_id_1 in peers_ids_0
    peers_ids_1 = tuple(
        pinfo.peer_id
        for pinfo in await hosts[1].list_peers()
    )
    assert peer_id_0 in peers_ids_1


@pytest.mark.asyncio
async def test_daemon_host_disconnect(hosts):
    await hosts[0].connect(await hosts[1].get_peer_info())
    assert len(await hosts[0].list_peers()) == 1
    await hosts[0].disconnect(await hosts[1].get_id())
    assert len(await hosts[0].list_peers()) == 0
    assert len(await hosts[1].list_peers()) == 0


@pytest.mark.asyncio
async def test_daemon_host_set_stream_handler(hosts):
    proto = '123'

    async def handler(stream_info, reader, writer):
        pass

    await hosts[0].set_stream_handler(proto, handler)
    assert proto in hosts[0].control_client.handlers


@pytest.mark.asyncio
async def test_daemon_host_new_stream(hosts):
    proto = '123'
    data = b"123"
    event = asyncio.Event()

    async def handler(stream_info, reader, writer):
        assert stream_info.peer_id == (await hosts[1].get_id())
        assert stream_info.proto == proto
        assert (await reader.read(len(data))) == data
        event.set()

    await hosts[0].set_stream_handler(proto, handler)
    stream_info, _, writer = await hosts[1].new_stream(
        peer_id=await hosts[0].get_id(),
        protocol_ids=[proto],
    )
    assert stream_info.peer_id == await hosts[0].get_id()
    writer.write(data)
    await event.wait()
