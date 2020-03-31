from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, NewType, TypeVar

HTTPMethod = NewType("HTTPMethod", str)

GET = HTTPMethod("GET")
POST = HTTPMethod("POST")

Request = Dict[str, Any]
Response = Dict[str, Any]
Context = TypeVar("Context")
Handler = Callable[[Context, Request], Response]


class APIResource(Dict[HTTPMethod, Handler]):
    pass


@dataclass
class ValidatorAPIContext:
    client_identifier: str


# node: BeaconNode


async def _get_node_version_handler(
    context: ValidatorAPIContext, request: Request
) -> Response:
    return context.client_identifier


async def _get_sync_status_handler(
    context: ValidatorAPIContext, request: Request
) -> Response:
    return {"is_syncing": False}


async def _get_validator_duties_handler(
    context: ValidatorAPIContext, request: Request
) -> Response:
    return


@dataclass
class APIHandler:
    server_handler: Callable[[Context, Request], Awaitable[Response]]


NodeVersionAPI = APIResource(
    ((GET, APIHandler(server_handler=_get_node_version_handler)),)
)

# SyncStatusAPI = APIResource(((GET, _get_sync_status_handler),))

# ValidtorDutiesAPI = APIResource(((GET, _get_valdidator_duties_handler),))

validator_api = {
    "/node/version": NodeVersionAPI,
    # "/node/genesis_time"
    # "/node/syncing": SyncStatusAPI,
    # "/validator/duties": ValidatorDutiesAPI,
    # "/validator/block"
    # "/validator/attestation"
}


# server handler
# client encoder
# client decoder
