import logging

from aiohttp import web
from eth_typing import BLSSignature
from eth_utils import decode_hex, encode_hex, humanize_hash
from lahja.base import EndpointAPI
from ssz.tools.dump import to_formatted_dict
from ssz.tools.parse import from_formatted_dict

from eth2.beacon.chains.base import BaseBeaconChain
from eth2.beacon.types.attestations import Attestation, AttestationData
from eth2.beacon.types.blocks import BeaconBlock, BeaconBlockBody
from eth2.beacon.typing import Bitfield, CommitteeIndex, Slot
from eth2.api.http.validator import Paths as APIEndpoint
from trinity._utils.version import construct_trinity_client_identifier
from trinity.http.apps.base_handler import BaseHandler, get, post


class ValidatorAPIHandler(BaseHandler):
    logger = logging.getLogger("trinity.http.apps.validator_api.ValidatorAPIHandler")

    def __init__(
        self, chain: BaseBeaconChain, event_bus: EndpointAPI, genesis_time: int
    ):
        self._chain = chain
        self._event_bus = event_bus
        self._genesis_time = genesis_time
        self._client_identifier = construct_trinity_client_identifier()

    @get(APIEndpoint.node_version)
    async def _get_client_version(self, request: web.Request) -> web.Response:
        return web.json_response(self._client_identifier)

    @get(APIEndpoint.genesis_time)
    async def _get_genesis_time(self, request: web.Request) -> web.Response:
        return web.json_response(self._genesis_time)

    @get(APIEndpoint.sync_status)
    async def _get_sync_status(self, request: web.Request) -> web.Response:
        # TODO: get actual status in real tim
        status = {
            "is_syncing": False,
            "sync_status": {"starting_slot": 0, "current_slot": 0, "highest_slot": 0},
        }
        return web.json_response(status)

    @get(APIEndpoint.validator_duties)
    async def _get_validator_duties(self, request: web.Request) -> web.Response:
        public_keys = tuple(
            map(decode_hex, request.query["validator_pubkeys"].split(","))
        )
        # epoch = Epoch(request.query["epoch"])
        duties = tuple(
            {
                "validator_pubkey": encode_hex(public_key),
                "attestation_slot": 2222,
                "attestation_shard": 22,
                "block_proposal_slot": 90,
            }
            for public_key in public_keys
        )
        return web.json_response(duties)

    @get(APIEndpoint.block_proposal)
    async def _get_block_proposal(self, request: web.Request) -> web.Response:
        slot = Slot(int(request.query["slot"]))
        randao_reveal = BLSSignature(
            decode_hex(request.query["randao_reveal"]).ljust(96, b"\x00")
        )
        block = BeaconBlock.create(
            slot=slot, body=BeaconBlockBody.create(randao_reveal=randao_reveal)
        )
        return web.json_response(to_formatted_dict(block))

    @post(APIEndpoint.block_proposal)
    async def _post_block_proposal(self, request: web.Request) -> web.Response:
        block_data = await request.json()
        block = from_formatted_dict(block_data, BeaconBlock)
        self.logger.info(
            "broadcasting block with root %s", humanize_hash(block.hash_tree_root)
        )
        # TODO the actual brodcast
        return web.Response()

    @get(APIEndpoint.attestation)
    async def _get_attestation(self, request: web.Request) -> web.Response:
        # _public_key = BLSPubkey(decode_hex(request.query["validator_pubkey"]))
        slot = Slot(int(request.query["slot"]))
        committee_index = CommitteeIndex(int(request.query["committee_index"]))
        attestation = Attestation.create(
            aggregation_bits=Bitfield([True, False, False]),
            data=AttestationData.create(index=committee_index, slot=slot),
        )
        return web.json_response(to_formatted_dict(attestation))

    @post(APIEndpoint.attestation)
    async def _post_attestation(self, request: web.Request) -> web.Response:
        attestation_data = await request.json()
        attestation = from_formatted_dict(attestation_data, Attestation)
        self.logger.info(
            "broadcasting attestation with root %s",
            humanize_hash(attestation.hash_tree_root),
        )
        # TODO the actual brodcast
        return web.Response()
