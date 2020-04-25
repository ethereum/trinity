"""
This module contains the eth2 HTTP validator API connecting a validator client to a beacon node.
"""
from dataclasses import dataclass
from enum import Enum, unique

from trinity._utils.trio_utils import Request, Response


@unique
class Paths(Enum):
    node_version = "/node/version"
    genesis_time = "/node/genesis_time"
    sync_status = "/node/syncing"
    validator_duties = "/validator/duties"
    block_proposal = "/validator/block"
    attestation = "/validator/attestation"


@dataclass
class Context:
    client_identifier: str


async def _get_node_version(context: Context, request: Request) -> Response:
    return context.client_identifier


GET = "GET"
POST = "POST"


ServerHandlers = {Paths.node_version.value: {GET: _get_node_version}}
