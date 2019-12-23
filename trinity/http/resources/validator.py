from typing import Any, Dict
from aiohttp import web
from trinity.http.resources.base import BaseResource, get_method, post_method


class Validator(BaseResource):
    #
    # GET methods
    #
    @get_method
    async def pubkey(self, request: web.Request) -> Dict[str, Any]:
        # TODO
        ...

    @get_method
    async def duties(self, request: web.Request) -> Dict[str, Any]:
        # TODO
        ...

    @get_method
    async def block(self, request: web.Request) -> Dict[str, Any]:
        # TODO
        ...

    @get_method
    async def attestation(self, request: web.Request) -> Dict[str, Any]:
        # TODO
        ...

    #
    # POST method
    #
    @post_method
    async def post_block(self, request: web.Request) -> Dict[str, Any]:
        # TODO
        ...

    @post_method
    async def post_attestation(self, request: web.Request) -> Dict[str, Any]:
        # TODO
        ...
