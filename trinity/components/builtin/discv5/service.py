from async_service import Service, background_trio_service

from ddht.boot_infor import BootInfo
from ddht.v5.app import Application
from lahja import EndpointAPI

from p2p.events import PeerCandidatesRequest

from trinity.config import TrinityConfig


class DiscoveryV5Service(Service):
    def __init__(self,
                 event_bus: EndpointAPI,
                 trinity_config: TrinityConfig,
                 boot_info: BootInfo) -> None:
        self._event_bus = event_bus
        self._trinity_config = trinity_config
        self._app = Application(boot_info)

    async def run(self) -> None:
        async with background_trio_service(self._app):
            self.manager.run_daemon_task(self._handle_peer_candidate_requests)
            await self.manager.wait_finished()

    async def _handle_peer_candidate_requests(self) -> None:
        async for request in self._event_bus.stream(PeerCandidatesRequest):
            ...
