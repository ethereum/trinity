import asyncio

import pytest


@pytest.mark.asyncio
async def test_daemon_host_get_peer_info(daemon_hosts):
    assert daemon_hosts[0].peer_info is None, "`peer_info` should be set lazily"
    pinfo = await daemon_hosts[0].get_peer_info()
    assert pinfo is not None, "`get_peer_info` should never return None"
    assert pinfo == daemon_hosts[0].peer_info, "`peer_info` should be set after calling `get_peer_info`"  # noqa: E501


@pytest.mark.asyncio
async def test_daemon_host_get_id(daemon_hosts):
    await daemon_hosts[0].get_id() is not None


@pytest.mark.asyncio
async def test_daemon_host_get_addrs(daemon_hosts):
    # should at least have one maddr listened
    assert len(await daemon_hosts[0].get_addrs()) != 0


@pytest.mark.asyncio
async def test_daemon_host_list_peers(daemon_hosts):
    assert len(await daemon_hosts[0].list_peers()) == 0
    # mock
    daemon_hosts[0].control_client._peers.add(await daemon_hosts[1].get_id())
    assert len(await daemon_hosts[0].list_peers()) == 1


@pytest.mark.asyncio
async def test_daemon_host_connect(daemon_hosts):
    peer_id_0 = await daemon_hosts[0].get_id()
    peer_id_1 = await daemon_hosts[1].get_id()
    pinfo_1 = await daemon_hosts[1].get_peer_info()
    await daemon_hosts[0].connect(pinfo_1)
    peers_ids_0 = tuple(
        pinfo.peer_id
        for pinfo in await daemon_hosts[0].list_peers()
    )
    assert peer_id_1 in peers_ids_0
    peers_ids_1 = tuple(
        pinfo.peer_id
        for pinfo in await daemon_hosts[1].list_peers()
    )
    assert peer_id_0 in peers_ids_1


@pytest.mark.asyncio
async def test_daemon_host_disconnect(daemon_hosts):
    await daemon_hosts[0].connect(await daemon_hosts[1].get_peer_info())
    assert len(await daemon_hosts[0].list_peers()) == 1
    await daemon_hosts[0].disconnect(await daemon_hosts[1].get_id())
    assert len(await daemon_hosts[0].list_peers()) == 0
    assert len(await daemon_hosts[1].list_peers()) == 0


@pytest.mark.asyncio
async def test_daemon_host_set_stream_handler(daemon_hosts):
    proto = '123'

    async def handler(stream_info, reader, writer):
        pass

    await daemon_hosts[0].set_stream_handler(proto, handler)
    assert proto in daemon_hosts[0].control_client.handlers


@pytest.mark.asyncio
async def test_daemon_host_new_stream(daemon_hosts):
    proto = '123'
    data = b"123"
    event = asyncio.Event()

    async def handler(stream_info, reader, writer):
        assert stream_info.peer_id == (await daemon_hosts[1].get_id())
        assert stream_info.proto == proto
        assert (await reader.read(len(data))) == data
        event.set()

    await daemon_hosts[0].set_stream_handler(proto, handler)
    stream_info, _, writer = await daemon_hosts[1].new_stream(
        peer_id=await daemon_hosts[0].get_id(),
        protocol_ids=[proto],
    )
    assert stream_info.peer_id == await daemon_hosts[0].get_id()
    writer.write(data)
    await event.wait()
