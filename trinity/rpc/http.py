import logging
from typing import (
    Any,
    Callable,
)

from aiohttp import web
from eth_utils import get_extended_debug_logger
from eth_utils.toolz import curry

from p2p.service import Service

from trinity.rpc.main import (
    RPCServer,
)


@curry
async def handler(execute_rpc: Callable[[Any], Any], request: web.Request) -> web.Response:
    logger = logging.getLogger('trinity.rpc.http')

    if request.method == 'POST':
        logger.debug(f'Receiving request: {request}')
        try:
            body_json = await request.json()
            logger.debug(f'data: {body_json}')
        except Exception as e:
            # invalid json request, keep reading data until a valid json is formed
            msg = f"Invalid request: {request}"
            logger.debug(msg)
            return response_error(msg)

        try:
            result = await execute_rpc(body_json)
        except Exception as e:
            msg = "Unrecognized exception while executing RPC"
            logger.exception(msg)
            return response_error(msg)
        else:
            logger.debug(f'writing: {result.encode()}')
            return web.Response(content_type='application/json', text=result)
    else:
        return response_error("Request method should be POST")


def response_error(message: Any) -> web.Response:
    data = {'error': message}
    return web.json_response(data)


class HTTPServer(Service):
    rpc = None
    server = None
    host = None
    port = None

    logger = get_extended_debug_logger('trinity.rpc.HTTPServer')

    def __init__(
            self,
            rpc: RPCServer,
            host: str = '127.0.0.1',
            port: int = 8545) -> None:
        self.rpc = rpc
        self.host = host
        self.port = port
        self.server = web.Server(handler(self.rpc.execute))

    async def run(self) -> None:
        runner = web.ServerRunner(self.server)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        self.logger.info('HTTP started at: %s', site.name)
        try:
            await self.manager.wait_forever()
        finally:
            await self.server.shutdown()
