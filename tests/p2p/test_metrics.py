import pytest

from pyformance import MetricsRegistry

from p2p.metrics import PeerReporterRegistry
from trinity.protocol.eth.peer import ETHPeer
from trinity.tools.factories import LatestETHPeerPairFactory


@pytest.mark.asyncio
async def test_peer_reporter_registry():
    async with LatestETHPeerPairFactory() as (alice, bob):
        assert isinstance(alice, ETHPeer)
        assert isinstance(bob, ETHPeer)

        metrics_registry = MetricsRegistry()
        peer_reporter_registry = PeerReporterRegistry(metrics_registry)
        assert len(peer_reporter_registry._peer_reporters.keys()) == 0

        peer_reporter_registry.assign_peer_reporter(alice)
        assert len(peer_reporter_registry._peer_reporters.keys()) == 1
        assert peer_reporter_registry._peer_reporters[alice] == 0

        peer_reporter_registry.assign_peer_reporter(bob)
        assert len(peer_reporter_registry._peer_reporters.keys()) == 2
        assert peer_reporter_registry._peer_reporters[bob] == 1

        peer_reporter_registry.unassign_peer_reporter(alice)
        assert alice not in peer_reporter_registry._peer_reporters.keys()
        assert len(peer_reporter_registry._peer_reporters.keys()) == 1

        peer_reporter_registry.unassign_peer_reporter(bob)
        assert bob not in peer_reporter_registry._peer_reporters.keys()
        assert len(peer_reporter_registry._peer_reporters.keys()) == 0
