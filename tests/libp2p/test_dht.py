import pytest


@pytest.mark.asyncio
async def test_daemon_dht(daemon_hosts, daemon_dhts):
    # 0 <-> 1 <-> 2
    await daemon_hosts[0].connect(await daemon_hosts[1].get_peer_info())
    await daemon_hosts[1].connect(await daemon_hosts[2].get_peer_info())

    pinfo_2 = await daemon_dhts[0].find_peer(await daemon_hosts[2].get_id())
    assert pinfo_2.peer_id == (await daemon_hosts[2].get_id())

    pinofs_2 = await daemon_dhts[0].find_peers_connected_to_peer(await daemon_hosts[2].get_id())
    connected_peers_2 = (pinfo.peer_id for pinfo in pinofs_2)
    assert (await daemon_hosts[1].get_id()) in connected_peers_2

    cid = b'\x01r\x12 \xc0F\xc8\xechB\x17\xf0\x1b$\xb9\xecw\x11\xde\x11Cl\x8eF\xd8\x9a\xf1\xaeLa?\xb0\xaf\xe6K\x8b'  # noqa: E501
    await daemon_dhts[2].provide(cid)
    providers = await daemon_dhts[0].find_providers(cid, count=1)
    assert len(providers) == 1 and providers[0].peer_id == (await daemon_hosts[2].get_id())

    key = (await daemon_hosts[2].get_id()).to_bytes()
    closet_peers = await daemon_dhts[0].get_closest_peers(key)
    assert len(closet_peers) == 3 and closet_peers[0].to_bytes() == key

    await daemon_dhts[0].get_public_key(await daemon_hosts[2].get_id())

    key_another = b"key_another"
    value_another = b"value_another"
    await daemon_dhts[2].put_value(key_another, value_another)
    assert value_another == (await daemon_dhts[0].get_value(key_another))
    assert len(await daemon_dhts[1].search_value(key_another)) == 1
