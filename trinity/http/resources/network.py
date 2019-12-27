from typing import Tuple
from aiohttp import web
from trinity.components.eth2.metrics.events import Libp2pPeersRequest
from trinity.http.events import (
    Libp2pPeerIDRequest,
)
from trinity.http.resources.base import BaseResource, get_method


class Network(BaseResource):
    @get_method
    async def peer_id(self, request: web.Request) -> str:
        response = await self.event_bus.request(Libp2pPeerIDRequest())
        peer_id = response.result
        return str(peer_id)

    @get_method
    async def peers(self, request: web.Request) -> Tuple[str, ...]:
        response = await self.event_bus.request(Libp2pPeersRequest())
        peers = response.result
        return peers

    @get_method
    async def enr(self, request: web.Request) -> str:
        # TODO
        ...
