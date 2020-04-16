from async_service.asyncio import background_asyncio_service

import pytest

from p2p.disconnect import DisconnectReason

from trinity.protocol.eth.peer import ETHPeer
from trinity.protocol.eth.proto import (
    ETHProtocolV63,
    ETHProtocolV65,
)
from trinity.protocol.les.peer import LESPeer
from trinity.protocol.les.proto import (
    LESProtocolV1,
    LESProtocolV2,
)

from trinity.tools.factories import (
    ETHPeerPairFactory,
    LESV1PeerPairFactory,
    LESV2PeerPairFactory,
)

from tests.core.peer_helpers import (
    MockPeerPoolWithConnectedPeers,
)
from trinity.tools.factories.eth.proto import ETHV63PeerPairFactory


@pytest.mark.asyncio
async def test_LES_v1_peers():
    async with LESV1PeerPairFactory() as (alice, bob):
        assert isinstance(alice, LESPeer)
        assert isinstance(bob, LESPeer)

        assert isinstance(alice.sub_proto, LESProtocolV1)
        assert isinstance(bob.sub_proto, LESProtocolV1)


@pytest.mark.asyncio
async def test_LES_v2_peers():
    async with LESV2PeerPairFactory() as (alice, bob):
        assert isinstance(alice, LESPeer)
        assert isinstance(bob, LESPeer)

        assert isinstance(alice.sub_proto, LESProtocolV2)
        assert isinstance(bob.sub_proto, LESProtocolV2)


@pytest.mark.asyncio
async def test_ETH_v63_peers():
    async with ETHV63PeerPairFactory() as (alice, bob):
        assert isinstance(alice, ETHPeer)
        assert isinstance(bob, ETHPeer)

        assert isinstance(alice.sub_proto, ETHProtocolV63)
        assert isinstance(bob.sub_proto, ETHProtocolV63)


@pytest.mark.asyncio
async def test_ETH_peers():
    async with ETHPeerPairFactory() as (alice, bob):
        assert isinstance(alice, ETHPeer)
        assert isinstance(bob, ETHPeer)

        assert isinstance(alice.sub_proto, ETHProtocolV65)
        assert isinstance(bob.sub_proto, ETHProtocolV65)


@pytest.mark.asyncio
async def test_peer_pool_iter(event_loop):
    factory_a = ETHPeerPairFactory()
    factory_b = ETHPeerPairFactory()
    factory_c = ETHPeerPairFactory()
    async with factory_a as (peer1, _), factory_b as (peer2, _), factory_c as (peer3, _):
        pool = MockPeerPoolWithConnectedPeers([peer1, peer2, peer3])
        peers = list([peer async for peer in pool])

        assert len(peers) == 3
        assert peer1 in peers
        assert peer2 in peers
        assert peer3 in peers

        peers = []
        await peer2.disconnect(DisconnectReason.DISCONNECT_REQUESTED)
        async for peer in pool:
            peers.append(peer)

        assert len(peers) == 2
        assert peer1 in peers
        assert peer2 not in peers
        assert peer3 in peers


@pytest.mark.asyncio
async def test_remote_dao_fork_validation_skipped_on_eth64(monkeypatch):
    dao_fork_validator_called = False

    async def validate_remote_dao_fork_block():
        nonlocal dao_fork_validator_called
        dao_fork_validator_called = True

    async with ETHPeerPairFactory() as (alice, _):
        boot_manager = alice.get_boot_manager()
        monkeypatch.setattr(
            boot_manager, 'validate_remote_dao_fork_block', validate_remote_dao_fork_block)
        async with background_asyncio_service(boot_manager) as manager:
            await manager.wait_finished()
        assert not dao_fork_validator_called


@pytest.mark.asyncio
async def test_remote_dao_fork_validation_on_eth63(monkeypatch):
    dao_fork_validator_called = False

    async def validate_remote_dao_fork_block():
        nonlocal dao_fork_validator_called
        dao_fork_validator_called = True

    async with ETHV63PeerPairFactory() as (alice, _):
        boot_manager = alice.get_boot_manager()
        monkeypatch.setattr(
            boot_manager, 'validate_remote_dao_fork_block', validate_remote_dao_fork_block)
        async with background_asyncio_service(boot_manager) as manager:
            await manager.wait_finished()
        assert dao_fork_validator_called
