from typing import Any, Dict
from aiohttp import web

from eth_typing import Hash32
from eth_utils import decode_hex
from ssz.tools import to_formatted_dict

from eth2.beacon.types.blocks import BeaconBlock
from eth2.beacon.typing import SigningRoot, Slot
from trinity.http.resources.base import BaseResource, get_method
from trinity.http.exceptions import APIServerError


class Beacon(BaseResource):
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
        elif 'root' in request.query:
            # TODO
            pass
        else:
            raise APIServerError(f"Wrong querystring: {request.query}")

        # TODO: find another way to get correct state class
        state_class = self.chain.get_head_state().__class__
        return to_formatted_dict(state, sedes=state_class)
