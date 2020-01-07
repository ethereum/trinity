import json
from typing import Any, Dict, Tuple, Union
from urllib.parse import parse_qs

from aiohttp import web

from eth_utils import decode_hex
from ssz import BaseSedes
from ssz.tools import from_formatted_dict, to_formatted_dict

from eth2.beacon.helpers import compute_epoch_at_slot
from eth2.beacon.tools.builder.validator_duty import get_validator_duties
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.blocks import BeaconBlock
from eth2.beacon.types.validators import Validator as ValidatorSSZ
from eth2.beacon.typing import (
    Epoch,
)

from trinity.http.exceptions import (
    InternalError_500,
    InvalidRequestSyntaxError_400,
    NotFoundError_404,
)
from trinity.http.resources.base import BaseResource, get_method, post_method


def json_to_ssz(json_object: Union[str, bytes, bytearray], ssz_codec: BaseSedes) -> Any:
    dict_object = json.loads(json_object)
    return from_formatted_dict(dict_object, ssz_codec)


class Validator(BaseResource):

    async def route(self, request: web.Request, sub_collection: str) -> Any:
        if request.method == 'POST':
            sub_collection = 'post_' + sub_collection

        if sub_collection.startswith('0x'):
            handler = self.pubkey
        else:
            handler = getattr(self, sub_collection)

        result = await handler(request)
        return result

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
        # TODO: cache the total duties in beacon node
        # Duties should only need to be checked once per epoch,
        # however a chain reorganization (of > MIN_SEED_LOOKAHEAD epochs) could occur,
        # resulting in a change of duties.For full safety, this API call should be polled
        # at every slot to ensure that chain reorganizations are recognized, and to ensure
        # that the beacon node is properly synchronized.

        state = self.chain.get_head_state()
        state_machine = self.chain.get_state_machine()
        config = state_machine.config

        # Parse query string
        parameters = parse_qs(request.query_string)
        try:
            validator_pubkeys_hex = parameters['validator_pubkeys']

            if 'epoch' in request.query:
                epoch = Epoch(request.query['epoch'])
            else:
                epoch = compute_epoch_at_slot(state.slot, config.SLOTS_PER_EPOCH)
        except Exception as e:
            raise InvalidRequestSyntaxError_400("Failed to parse the request", e)

        try:
            validator_duties = tuple(filter(
                lambda duty: duty['validator_pubkey'] in validator_pubkeys_hex,
                get_validator_duties(state, state_machine, epoch, config),
            ))
        except Exception as e:
            raise InternalError_500(str(e))

        return validator_duties

    @get_method
    async def block(self, request: web.Request) -> Dict[str, Any]:
        try:
            parameters = parse_qs(request.query_string)
            slot = parameters['slot']
            randao_reveal_hex = parameters['randao_reveal']
        except Exception as e:
            raise InvalidRequestSyntaxError_400("Failed to parse the request", e)

        # TODO: refactor trinity.components.eth2.validator.propose_block
        # and move the block preparation to beacon node.
        # Then sending request to beacon node here.

        return {}

    @get_method
    async def attestation(self, request: web.Request) -> Dict[str, Any]:
        try:
            parameters = parse_qs(request.query_string)
            validator_pubkey = parameters['validator_pubkey']
            slot = parameters['slot']
            committee_index = parameters['committee_index']
        except Exception as e:
            raise InvalidRequestSyntaxError_400("Failed to parse the request", e)

        # TODO: refactor trinity.components.eth2.validator.attest
        # and move the attestation preparation to beacon node.
        # Then sending request to beacon node here.

        return {}

    #
    # POST method
    #
    @post_method
    async def post_block(self, request: web.Request) -> Dict[str, Any]:
        try:
            body_json = await request.json()
            block = json_to_ssz(body_json, BeaconBlock)
        except Exception as e:
            raise InvalidRequestSyntaxError_400("Cannot read body", e)

        # TODO: forward to beacon node

        return {}

    @post_method
    async def post_attestation(self, request: web.Request) -> Dict[str, Any]:
        try:
            body_json = await request.json()
            attestation = json_to_ssz(body_json, Attestation)
        except Exception as e:
            raise InvalidRequestSyntaxError_400("Cannot read body", e)

        # TODO: forward to attestation pool

        return {}
