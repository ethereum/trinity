from typing import Dict, Generic
from pyformance import MetricsRegistry

from p2p.abc import TPeer
from p2p.exceptions import PeerReporterRegistryError


class PeerReporterRegistry(Generic[TPeer]):
    """
    Registry to track active peers and report metrics to influxdb/grafana.
    """
    def __init__(self, metrics_registry: MetricsRegistry) -> None:
        self.metrics_registry = metrics_registry
        self._peer_reporters: Dict[TPeer, int] = {}

    def assign_peer_reporter(self, peer: TPeer) -> None:
        if peer in self._peer_reporters.keys():
            raise PeerReporterRegistryError(
                f"Cannot reassign peer: {peer} to PeerReporterRegistry. "
                "Peer already assigned."
            )
        min_unclaimed_id = self._get_min_unclaimed_gauge_id()
        self._peer_reporters[peer] = min_unclaimed_id
        self.reset_peer_meters(min_unclaimed_id)

    def unassign_peer_reporter(self, peer: TPeer) -> None:
        try:
            gauge_id = self._peer_reporters[peer]
        except AttributeError:
            raise PeerReporterRegistryError(
                f"Cannot unassign peer: {peer} to PeerReporterRegistry. "
                "Peer not found in peer reporter registry."
            )
        self.reset_peer_meters(gauge_id)
        del self._peer_reporters[peer]

    def _get_min_unclaimed_gauge_id(self) -> int:
        try:
            max_claimed_id = max(self._peer_reporters.values())
        except ValueError:
            return 0

        unclaimed_ids = [
            num for num in range(0, max_claimed_id + 1) if num not in self._peer_reporters.values()
        ]
        if len(unclaimed_ids) == 0:
            return max_claimed_id + 1

        return min(unclaimed_ids)

    def trigger_peer_reports(self) -> None:
        for peer, peer_id in self._peer_reporters.items():
            if peer.get_manager().is_running:
                self.make_periodic_update(peer, peer_id)

    def reset_peer_meters(self, peer_id: int) -> None:
        # subclasses are responsible for implementing method that resets all implemented meters
        pass

    def make_periodic_update(self, peer: TPeer, peer_id: int) -> None:
        # subclasses are responsible for implementing method that updates all implemented meters
        pass
