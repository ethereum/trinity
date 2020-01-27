import logging

from lahja import EndpointAPI

from async_service import Service

from p2p.events import PeerCandidatesRequest, PeerCandidatesResponse

from .tracker import (
    BaseEth1PeerTracker,
)
from .events import (
    TrackPeerEvent,
)


class PeerDBServer(Service):
    """
    Server to handle the event bus communication for PeerDB
    """
    logger = logging.getLogger('trinity.components.network_db.PeerDBServer')

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
        async for req in self.event_bus.stream(PeerCandidatesRequest):
            candidates = tuple(await self.tracker.get_peer_candidates(
                req.max_candidates,
                req.should_skip_fn,
            ))
            await self.event_bus.broadcast(
                PeerCandidatesResponse(candidates),
                req.broadcast_config(),
            )

    async def run(self) -> None:
        self.logger.debug("Running PeerDBServer")

        self.manager.run_daemon_task(
            self.handle_track_peer_event,
            name='PeerDBServer.handle_track_peer_event',
        )
        self.manager.run_daemon_task(
            self.handle_get_peer_candidates_request,
            name='PeerDBServer.handle_get_peer_candidates_request',
        )

        await self.manager.wait_finished()
