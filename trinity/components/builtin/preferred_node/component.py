import random
import time
from typing import Dict, Iterator, List

from async_service import Service, background_trio_service
from eth_utils import to_tuple
from lahja import EndpointAPI

from p2p import trio_utils
from p2p.abc import NodeAPI
from p2p.constants import DEFAULT_MAX_PEERS
from trinity._utils.logging import get_logger
from trinity.constants import TO_NETWORKING_BROADCAST_CONFIG
from trinity.extensibility import TrioIsolatedComponent
from trinity.protocol.common.events import (
    ConnectToNodeCommand,
    GetConnectedPeersRequest,
)


class PreferredNodeComponent(TrioIsolatedComponent):
    """
    Monitor peer pool and attempt to connect to a preferred node
    when the pool is not at full capacity.
    """
    name = "PreferredNode"

    @property
    def is_enabled(self) -> bool:
        return bool(self.preferred_nodes)

    @property
    def max_peers(self) -> int:
        if self._boot_info.args.max_peers is not None:
            return self._boot_info.args.max_peers
        return DEFAULT_MAX_PEERS

    @property
    def preferred_nodes(self) -> List[NodeAPI]:
        return self._boot_info.args.preferred_nodes

    async def do_run(self, event_bus: EndpointAPI) -> None:
        service = PreferredNodeService(event_bus, self.preferred_nodes, self.max_peers)
        async with background_trio_service(service) as manager:
            await manager.wait_finished()


class PreferredNodeService(Service):
    preferred_node_recycle_time: int = 300
    pool_availability_check_period: int = 5
    _preferred_node_tracker: Dict[NodeAPI, float] = None

    def __init__(
        self, event_bus: EndpointAPI, preferred_nodes: List[NodeAPI], max_peers: int
    ) -> None:
        self.event_bus = event_bus
        self.max_peers = max_peers
        self.preferred_nodes = preferred_nodes
        self._preferred_node_tracker = {}
        self.logger = get_logger('trinity.components.preferred_node.PreferredNodeService')

    async def run(self) -> None:
        while self.manager.is_running:
            await self.periodically_try_to_fill_pool_with_preferred_nodes()

    async def periodically_try_to_fill_pool_with_preferred_nodes(self) -> None:
        """
        Check pool for available peer slots, and brodacast command to connect
        to preferred nodes if there's room in the pool.
        """
        # seed _preferred_node_tracker so all preferred nodes are considered eligible initially
        for node in self.preferred_nodes:
            self._preferred_node_tracker[node] = time.monotonic() - self.preferred_node_recycle_time

        await self.event_bus.wait_until_any_endpoint_subscribed_to(GetConnectedPeersRequest)

        async for _ in trio_utils.every(self.pool_availability_check_period):
            response = await self.event_bus.request(GetConnectedPeersRequest())
            available_slots = self.max_peers - len(response.peers)
            if available_slots > 0:
                connected_peers = [peer.session.remote for peer in response.peers]
                eligible_nodes = self._sample_eligible_preferred_nodes(
                    connected_peers,
                    available_slots
                )
                for node in eligible_nodes:
                    await self._connect_to_preferred_node(node)

    async def _connect_to_preferred_node(self, node: NodeAPI) -> None:
        """
        Broadcast a command for peer pool to connect to a preferred node,
        and track the time of attempted connection.
        """
        self._preferred_node_tracker[node] = time.monotonic()
        await self.event_bus.broadcast(
            ConnectToNodeCommand(node),
            TO_NETWORKING_BROADCAST_CONFIG,
        )

    def _sample_eligible_preferred_nodes(
            self,
            connected_peers: List[NodeAPI],
            available_slots: int) -> List[NodeAPI]:
        """
        Return a random sampling of preferred_nodes that are eligible for a connection attempt.
        Sample size attempts to fill all available slots in the peer pool.
        """
        eligible_nodes = self._get_eligible_nodes(connected_peers)
        sample_size = min(available_slots, len(eligible_nodes))
        return random.sample(eligible_nodes, sample_size)

    @to_tuple
    def _get_eligible_nodes(self, connected_peers: List[NodeAPI]) -> Iterator[NodeAPI]:
        """
        Return all preferred nodes that are not currently connected and have exceeded the recycle
        time since the last connection attempt.
        """
        missing_preferred = [node for node in self.preferred_nodes if node not in connected_peers]
        for node in missing_preferred:
            last_used = self._preferred_node_tracker[node]
            if time.monotonic() - last_used > self.preferred_node_recycle_time:
                yield node
