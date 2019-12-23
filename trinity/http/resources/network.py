from typing import Tuple
from aiohttp import web
from trinity.http.resources.base import BaseResource, get_method


class Beacon(BaseResource):
    @get_method
    async def peer_id(self, request: web.Request) -> str:
        # TODO
        ...

    @get_method
    async def peers(self, request: web.Request) -> Tuple[str, ...]:
        # TODO
        ...

    @get_method
    async def enr(self, request: web.Request) -> str:
        # TODO
        ...
