import logging
from typing import (
    Any,
    Callable,
    Dict,
)

from aiohttp import web
from eth_utils.toolz import curry

from trinity.http.handlers.base import BaseHTTPHandler, response_error


#
# JSON-RPC
#
class JsonParsingException(Exception):
    ...


class JsonRpcCallException(Exception):
    ...


async def load_json_request(request: web.Request) -> Any:
    try:
        body_json = await request.json()
    except Exception:
        raise JsonParsingException(f"Invalid request: {request}")
    else:
        return body_json


async def execute_json_rpc(
    execute_rpc: Callable[[Any], Any],
    json_request: Dict['str', Any]
) -> str:
    try:
        result = await execute_rpc(json_request)
    except Exception as e:
        msg = f"Unrecognized exception while executing RPC: {e}"
        raise JsonRpcCallException(msg)
    else:
        return result


class RPCHandler(BaseHTTPHandler):

    @staticmethod
    @curry
    async def handle(execute_rpc: Callable[[Any], Any], request: web.Request) -> web.Response:
        logger = logging.getLogger('trinity.http.handlers.rpc_handler')

        if request.method == 'POST':
            logger.debug('Receiving POST request: %s', request.path)
            try:
                body_json = await load_json_request(request)
            except JsonParsingException as e:
                return response_error(e)

            try:
                result = await execute_json_rpc(execute_rpc, body_json)
            except JsonRpcCallException as e:
                return response_error(e)
            else:
                return web.Response(content_type='application/json', text=result)
        else:
            return response_error(f"Metrics Server doesn't support {request.method} request")
