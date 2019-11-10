from eth_utils import get_extended_debug_logger

from lahja import EndpointAPI

from p2p.service import Service

from .tracker import (
    BaseEth1PeerTracker,
)
from .events import (
    TrackPeerEvent,
    GetPeerCandidatesRequest,
    GetPeerCandidatesResponse,
)


class PeerDBServer(Service):
    """
    Server to handle the event bus communication for PeerDB
    """
    logger = get_extended_debug_logger('trinity.components.network-db.PeerDBServer')

    def __init__(self,
                 event_bus: EndpointAPI,
                 tracker: BaseEth1PeerTracker) -> None:
        self.tracker = tracker
        self.event_bus = event_bus

    async def handle_track_peer_event(self) -> None:
        async for command in self.event_bus.stream(TrackPeerEvent):
            self.tracker.track_peer_connection(
                command.remote,
                command.is_outbound,
                command.last_connected_at,
                command.genesis_hash,
                command.protocol,
                command.protocol_version,
                command.network_id,
            )

    async def handle_get_peer_candidates_request(self) -> None:
        async for req in self.event_bus.stream(GetPeerCandidatesRequest):
            candidates = tuple(await self.tracker.get_peer_candidates(
                req.num_requested,
                req.connected_remotes,
            ))
            await self.event_bus.broadcast(
                GetPeerCandidatesResponse(candidates),
                req.broadcast_config(),
            )

    async def run(self) -> None:
        self.logger.debug("Running PeerDBServer")

        self.manager.run_daemon_task(self.handle_track_peer_event)
        self.manager.run_daemon_task(self.handle_get_peer_candidates_request)

        await self.manager.wait_forever()
