from abc import ABC, abstractmethod
from typing import (
    Callable,
    Tuple,
)

from lahja import (
    BroadcastConfig,
    EndpointAPI,
)

from p2p.abc import NodeAPI
from p2p.constants import DISCOVERY_EVENTBUS_ENDPOINT
from p2p.events import (
    PeerCandidatesRequest,
    RandomBootnodeRequest,
)


class BasePeerBackend(ABC):
    @abstractmethod
    async def get_peer_candidates(self,
                                  max_candidates: int,
                                  should_skip_fn: Callable[[NodeAPI], bool]) -> Tuple[NodeAPI, ...]:
        ...


TO_DISCOVERY_BROADCAST_CONFIG = BroadcastConfig(filter_endpoint=DISCOVERY_EVENTBUS_ENDPOINT)


class DiscoveryPeerBackend(BasePeerBackend):
    def __init__(self, event_bus: EndpointAPI) -> None:
        self.event_bus = event_bus

    async def get_peer_candidates(self,
                                  max_candidates: int,
                                  should_skip_fn: Callable[[NodeAPI], bool]) -> Tuple[NodeAPI, ...]:
        await self.event_bus.wait_until_any_endpoint_subscribed_to(PeerCandidatesRequest)
        response = await self.event_bus.request(
            PeerCandidatesRequest(max_candidates, should_skip_fn),
            TO_DISCOVERY_BROADCAST_CONFIG,
        )
        return response.candidates


class BootnodesPeerBackend(BasePeerBackend):
    def __init__(self, event_bus: EndpointAPI) -> None:
        self.event_bus = event_bus

    async def get_peer_candidates(self,
                                  max_candidates: int,
                                  should_skip_fn: Callable[[NodeAPI], bool]) -> Tuple[NodeAPI, ...]:
        await self.event_bus.wait_until_any_endpoint_subscribed_to(RandomBootnodeRequest)
        response = await self.event_bus.request(
            RandomBootnodeRequest(),
            TO_DISCOVERY_BROADCAST_CONFIG
        )
        return response.candidates
