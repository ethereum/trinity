import secrets
from typing import Callable, Iterable

from async_service import Service

from ddht.boot_info import BootInfo
from ddht.v5.app import Application
from eth_typing import NodeID
from eth_utils import get_logger, ExtendedDebugLogger
from eth_utils.toolz import take
from lahja import EndpointAPI

from p2p.abc import NodeAPI
from p2p.events import PeerCandidatesRequest
from p2p.kademlia import Node

from trinity.config import TrinityConfig


class DiscoveryV5Service(Service):
    logger: ExtendedDebugLogger

    def __init__(self,
                 event_bus: EndpointAPI,
                 trinity_config: TrinityConfig,
                 boot_info: BootInfo) -> None:
        self.logger = get_logger('trinity.discv5')
        self._event_bus = event_bus
        self._trinity_config = trinity_config
        self._app = Application(boot_info)

    async def run(self) -> None:
        self.manager.run_daemon_child_service(self._app)
        self.manager.run_daemon_task(self._handle_peer_candidate_requests)
        self.logger.info('I AM RUNNING')
        await self.manager.wait_finished()

    async def _handle_peer_candidate_requests(self) -> None:
        self.logger.info('HANDLING REQUESTS')
        async for request in self._event_bus.stream(PeerCandidatesRequest):
            candidates_iter = self._get_candidates(request.should_skip_fn)
            candidates = tuple(take(request.max_candidates, candidates_iter))
            self.logger.info('DiscoveryV5 produced %d candidates', len(candidates))
            await self._event_bus.broadcast(
                request.expected_response_type()(candidates),
                request.broadcast_config(),
            )

    def _get_candidates(self,
                        skip_fn: Callable[[NodeAPI], bool]) -> Iterable[NodeAPI]:
        random_node_id = NodeID(secrets.token_bytes(32))
        for node_id in self._app.routing_table.iter_nodes_around(random_node_id):
            enr = self._app.enr_db.get_enr(node_id)
            node = Node(enr)
            if skip_fn(node):
                continue
            yield node
