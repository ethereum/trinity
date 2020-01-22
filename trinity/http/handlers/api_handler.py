import logging
from typing import (
    Dict,
    Type,
)

from aiohttp import web
from eth_utils.toolz import curry
from lahja.base import EndpointAPI

from eth2.beacon.chains.base import (
    BaseBeaconChain,
)

from trinity.http.handlers.base import BaseHTTPHandler, response_error
from trinity.http.exceptions import APIServerError, InvalidRequestSyntaxError_400
from trinity.http.api.resources.base import BaseResource
from trinity.http.api.resources.beacon import Beacon


logger = logging.getLogger('trinity.http.handlers.api_handler.APIHandler')


RESOURCES: Dict[str, Type[BaseResource]] = {
    Beacon.name(): Beacon,
}


async def process_request(
    request: web.Request,
    chain: BaseBeaconChain,
    event_bus: EndpointAPI
) -> Dict[str, int]:
    """
    A simple RESTful API parser
    """
    path = request.path.lower()
    path_array = tuple(path.split('/'))

    if len(path_array) <= 2:
        raise APIServerError(f"Wrong path: {path}")

    resource_name = path_array[1]
    sub_collection = path_array[2]

    if resource_name in RESOURCES:
        resource_class = RESOURCES[resource_name]
    else:
        raise InvalidRequestSyntaxError_400(f"Wrong path: {request.path}")

    resource = resource_class(chain, event_bus)
    return await resource.route(request, sub_collection)


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
        except APIServerError as e:
            msg = f"[APIHandler] Error: {str(e)}"
            logger.error(msg)
            return response_error(msg, e)
