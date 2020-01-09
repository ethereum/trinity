from typing import Any, Dict, cast
from aiohttp import web

from eth_typing import Hash32
from eth_utils import decode_hex
from ssz.tools import to_formatted_dict

from eth2.beacon.types.blocks import BeaconBlock
from eth2.beacon.typing import HashTreeRoot, SigningRoot, Slot
from trinity.http.api.resources.base import BaseResource, get_method
from trinity.http.exceptions import APIServerError


class Beacon(BaseResource):

    async def route(self, request: web.Request, sub_collection: str) -> Any:
        handler = getattr(self, sub_collection)
        result = await handler(request)
        return result

    @get_method
    async def head(self, request: web.Request) -> Dict[str, Any]:
        return to_formatted_dict(self.chain.get_canonical_head(), sedes=BeaconBlock)

    @get_method
    async def block(self, request: web.Request) -> Dict[str, Any]:
        if 'slot' in request.query:
            slot = Slot(int(request.query['slot']))
            block = self.chain.get_canonical_block_by_slot(slot)
        elif 'root' in request.query:
            root = SigningRoot(Hash32(decode_hex(request.query['root'])))
            block = self.chain.get_block_by_root(root)

        return to_formatted_dict(block, sedes=BeaconBlock)

    @get_method
    async def state(self, request: web.Request) -> Dict[str, Any]:
        if 'slot' in request.query:
            slot = Slot(int(request.query['slot']))
            state = self.chain.get_state_by_slot(slot)
            state_class = self.chain.get_state_machine(slot).state_class
        elif 'root' in request.query:
            root = cast(HashTreeRoot, decode_hex(request.query['root']))
            state = self.chain.get_state_by_root(root)
            state_class = self.chain.get_head_state().__class__  # TODO: It's a simple workaround
        else:
            raise APIServerError(f"Wrong querystring: {request.query}")

        return to_formatted_dict(state, sedes=state_class)
