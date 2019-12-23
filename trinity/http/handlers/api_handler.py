import logging
from typing import (
    Any,
    Callable,
    Dict,
)

from aiohttp import web
from eth_utils.toolz import curry
from lahja.base import EndpointAPI

from eth2.beacon.chains.base import (
    BaseBeaconChain,
)

from trinity.http.handlers.base import BaseHTTPHandler, response_error
from trinity.http.exceptions import APIServerError
from trinity.http.resources.beacon import Beacon
from trinity.http.resources.node import Node
from trinity.http.resources.validator import Validator

logger = logging.getLogger('trinity.http.handlers.api_handler.APIHandler')


async def process_request(
    request: web.Request,
    chain: BaseBeaconChain,
    event_bus: EndpointAPI
) -> Dict[str, int]:
    """
    A simple RESTful API parser
    """
    router = _get_router(request)
    return await router(request, chain, event_bus)


def _get_router(
    request: web.Request
) -> Callable[[web.Request, BaseBeaconChain, EndpointAPI], Any]:
    path = request.path.lower()

    if path.startswith("/beacon"):
        return beacon_router
    elif path.startswith("/validator"):
        return validator_router
    elif path.startswith("/node"):
        return node_router
    else:
        raise APIServerError(f"Wrong path: {request.path}")


def _get_path_object(request: web.Request) -> str:
    path = request.path.lower()
    path_array = tuple(path.split('/'))
    if len(path_array) <= 2:
        raise APIServerError(f"Wrong path: {path}")
    object = path_array[2]
    return object


async def node_router(
    request: web.Request,
    chain: BaseBeaconChain,
    event_bus: EndpointAPI
) -> Any:
    object = _get_path_object(request)
    node_resource = Node(chain, event_bus)
    handler = getattr(node_resource, object)
    result = await handler(request)
    return result


async def beacon_router(
    request: web.Request,
    chain: BaseBeaconChain,
    event_bus: EndpointAPI
) -> Any:
    object = _get_path_object(request)
    resource = Beacon(chain, event_bus)
    handler = getattr(resource, object)
    result = await handler(request)
    return result


async def validator_router(
    request: web.Request,
    chain: BaseBeaconChain,
    event_bus: EndpointAPI
) -> Any:
    object = _get_path_object(request)

    # TODO: handle {pubkey} especially

    resource = Validator(chain, event_bus)
    handler = getattr(resource, object)
    result = await handler(request)
    return result


class APIHandler(BaseHTTPHandler):

    @staticmethod
    @curry
    async def handle(
            chain: BaseBeaconChain,
            event_bus: EndpointAPI,
            request: web.Request
    ) -> web.Response:
        try:
            logger.debug('Receiving request: %s', request.path)
            data = await process_request(request, chain, event_bus)
            return web.json_response(data=data)
        except Exception as e:
            msg = f"[APIHandler] Error: {str(e)}"
            logger.error(msg)
            return response_error(msg)
