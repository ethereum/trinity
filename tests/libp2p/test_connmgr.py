import pytest


@pytest.mark.asyncio
async def test_daemon_connmgr(daemon_hosts, daemon_connmgrs):
    # 0 <-> 1 <-> 2
    await daemon_hosts[0].connect(await daemon_hosts[1].get_peer_info())
    await daemon_hosts[1].connect(await daemon_hosts[2].get_peer_info())

    tag = "tag_123"
    await daemon_connmgrs[1].tag_peer(
        await daemon_hosts[0].get_id(),
        tag,
        1,
    )
    await daemon_connmgrs[1].tag_peer(
        await daemon_hosts[2].get_id(),
        tag,
        2,
    )
    tag_another = "tag_345"
    await daemon_connmgrs[1].tag_peer(
        await daemon_hosts[0].get_id(),
        tag_another,
        3,
    )
    await daemon_connmgrs[1].trim()
    peers_1 = await daemon_hosts[1].list_peers()
    assert len(peers_1) == 1 and peers_1[0].peer_id == (await daemon_hosts[0].get_id())
