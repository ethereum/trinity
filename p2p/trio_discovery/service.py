import logging

import trio

from lahja.trio.endpoint import TrioEndpoint

from p2p.trio_service import Service, ManagerAPI
from p2p.events import (
    PeerCandidatesRequest,
    RandomBootnodeRequest,
)


class NoopDiscoveryRequestHandler(Service):
    logger = logging.getLogger('p2p.trio_discovery.service.NoopDiscoveryRequestHandler')

    def __init__(self, event_bus: TrioEndpoint) -> None:
        self._event_bus = event_bus

    async def run(self, manager: ManagerAPI) -> None:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(
                manager.run_daemon_task,
                self.handle_get_peer_candidates_requests(),
                'peer-candidate-request-handler',
            )
            nursery.start_soon(
                manager.run_daemon_task,
                self.handle_get_random_bootnode_requests(),
                'random-bootnode-request-handler',
            )
            await manager.wait_cancelled()

    async def handle_get_peer_candidates_requests(self) -> None:
        async for event in self._event_bus.stream(PeerCandidatesRequest):
            self.logger.debug2("Servicing request for more peer candidates")

            await self._event_bus.broadcast(
                event.expected_response_type()(tuple()),
                event.broadcast_config()
            )

    async def handle_get_random_bootnode_requests(self) -> None:
        async for event in self._event_bus.stream(RandomBootnodeRequest):
            self.logger.debug2("Servicing request for boot nodes")

            await self._event_bus.broadcast(
                event.expected_response_type()(tuple()),
                event.broadcast_config()
            )
