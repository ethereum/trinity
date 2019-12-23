from aiohttp import web

from ssz.tools import (
    to_formatted_dict,
)

from eth2.beacon.types.forks import Fork
from trinity.http.resources.base import BaseResource, get_method


class Node(BaseResource):
    @get_method
    async def version(self, request: web.Request) -> str:
        # TODO: add version number
        return "Trinity"

    @get_method
    async def genesis_time(self, request: web.Request) -> int:
        state = self.chain.get_head_state()
        return int(state.genesis_time)

    @get_method
    async def syncing(self, request: web.Request) -> Fork:
        # TODO
        ...

    @get_method
    async def fork(self, request: web.Request) -> Fork:
        return to_formatted_dict(self.chain.get_head_state().fork, sedes=Fork)
