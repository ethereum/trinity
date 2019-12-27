import json
from typing import Any, Dict, Tuple

from aiohttp import web

from eth_utils import decode_hex
from ssz.tools import to_formatted_dict

from eth2.beacon.helpers import compute_epoch_at_slot
from eth2.beacon.tools.builder.validator_duty import get_validator_duties
from eth2.beacon.types.validators import Validator as ValidatorSSZ

from trinity.http.exceptions import (
    InvalidRequestSyntaxError_400,
    NotFoundError_404,
)
from trinity.http.resources.base import BaseResource, get_method, post_method


class Validator(BaseResource):
    #
    # GET methods
    #
    @get_method
    async def pubkey(self, request: web.Request) -> Dict[str, Any]:
        # Parse pubkey
        path = request.path.lower()
        try:
            path_array = tuple(path.split('/'))
            pubkey = decode_hex(path_array[2])
        except Exception as e:
            raise InvalidRequestSyntaxError_400("Failed to parse the request", e)

        try:
            validator = tuple(filter(
                lambda v: v.pubkey == pubkey,
                self.chain.get_head_state().validators
            ))[0]
        except Exception:
            raise NotFoundError_404(f"Not found validator {validator}")

        return to_formatted_dict(validator, sedes=ValidatorSSZ)

    @get_method
    async def duties(self, request: web.Request) -> Tuple[Dict[str, Any], ...]:
        # TODO: cache in beacon node
        # Duties should only need to be checked once per epoch,
        # however a chain reorganization (of > MIN_SEED_LOOKAHEAD epochs) could occur,
        # resulting in a change of duties.For full safety, this API call should be polled
        # at every slot to ensure that chain reorganizations are recognized, and to ensure
        # that the beacon node is properly synchronized.

        state = self.chain.get_head_state()
        config = self.chain.get_state_machine().config
        epoch = compute_epoch_at_slot(state.slot, config.SLOTS_PER_EPOCH)

        validator_duties = get_validator_duties(state, config, epoch)
        return validator_duties

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
