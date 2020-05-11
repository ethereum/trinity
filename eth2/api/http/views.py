from dataclasses import asdict
from typing import Any, Mapping

from eth_typing import BLSSignature
from eth_utils import decode_hex, encode_hex
from quart import request, jsonify, Response, make_response
from ssz.tools.dump import to_formatted_dict
from ssz.tools.parse import from_formatted_dict

from eth2.beacon.types.blocks import SignedBeaconBlock
from eth2.beacon.typing import Slot

from .app import app
from .ir import ValidatorDuty
from .status import HTTP_201_CREATED


GET = "GET"
POST = "POST"


#
# Routes: `/beacon`
#
@app.route('/beacon/genesis')
async def beacon_genesis() -> Response:
    context = app.config["context"]
    return jsonify({
        "genesis_time": str(context.genesis_time),
        "genesis_validators_root": "TODO",
    })


@app.route('/beacon/fork')
async def beacon_fork() -> Response:
    raise NotImplementedError


@app.route('/beacon/fork/stream')
async def beacon_fork_stream() -> Response:
    # TODO: websockets and streaming response....?
    raise NotImplementedError


#
# Routes: `/node`
#
@app.route('/node/version')
async def node_version() -> Response:
    return jsonify(app.config["context"].client_identifier)


@app.route("/node/syncing")
async def sync_status() -> Response:
    context = app.config["context"]
    status = await context.get_sync_status()
    status_data = asdict(status)
    is_syncing = status_data.pop("is_syncing")
    return jsonify({"is_syncing": is_syncing, "sync_status": status_data})


#
# Routes: `/validator`
#
@app.route("/validator/<pubkey:public_key>")
async def validator_details(public_key) -> Response:
    raise NotImplementedError


def _marshal_duty(duty: ValidatorDuty) -> Mapping[str, Any]:
    duty_data = asdict(duty)
    duty_data["validator_pubkey"] = encode_hex(duty.validator_pubkey)
    return duty_data


async def _get_validator_duties_attester(epoch) -> Response:
    if "validator_pubkeys" not in request.args:
        return ()
    context = app.config['context']
    public_keys_as_hex = request.args.getlist('validator_pubkeys', ())
    public_keys = tuple(map(decode_hex, public_keys_as_hex))
    duties = context.get_validator_duties(public_keys, epoch)
    return jsonify(tuple(map(_marshal_duty, duties)))


async def _post_validator_duties_attester(epoch) -> Response:
    raise NotImplementedError()


@app.route("/validator/duties/<int:epoch>/attester", methods=(GET, POST))
async def validator_duties_attester(epoch) -> Response:
    if request.method == 'GET':
        return await _get_validator_duties_attester(epoch)
    elif request.method == 'POST':
        return await _post_validator_duties_attester(epoch)
    else:
        raise Exception("TODO: return correct error code")


@app.route("/validator/duties/<int:epoch>/proposer", methods=(GET, POST))
async def validator_duties_proposer(epoch) -> Response:
    raise NotImplementedError()


@app.route("/validator/beacon_committee_subscriptions", methods=(POST,))
async def validator_beacon_committee_subscriptions() -> Response:
    raise NotImplementedError


@app.route("/validator/beacon_committee_subscriptions/<int:committee_index>/attestations")
async def validator_beacon_committee_subscriptions_attestations(committee_index) -> Response:
    raise NotImplementedError


@app.route("/validator/aggregate_and_proof", methods=(GET, POST))
async def validator_aggregate_and_proof() -> Response:
    raise NotImplementedError


@app.route("/validator/attestation", methods=(GET, POST))
async def validator_attestation() -> Response:
    raise NotImplementedError


@app.route('/validator/block', methods=(GET, POST))
async def validator_block(epoch) -> Response:
    if request.method == 'GET':
        return await _get_validator_block(epoch)
    elif request.method == 'POST':
        return await _post_validator_block(epoch)
    else:
        raise Exception("TODO: return correct error code")


async def _get_validator_block(epoch) -> Response:
    context = app.config['context']
    slot = Slot(int(request["slot"]))
    randao_reveal = BLSSignature(
        decode_hex(request["randao_reveal"]).ljust(96, b"\x00")
    )
    block = context.get_block_proposal(slot, randao_reveal)
    return jsonify(to_formatted_dict(block))


async def _post_validator_block(epoch) -> Response:
    context = app.config['context']
    raw_payload = await request.get_json()
    # TODO: should validate post-data
    block = from_formatted_dict(raw_payload, SignedBeaconBlock)
    await context.broadcast_block(block)
    return make_response("", HTTP_201_CREATED)
