import asyncio
from typing import (
    Any,
    Callable,
)

from aiohttp import web

from cancel_token import (
    CancelToken,
)

from p2p.service import (
    BaseService,
)


class HTTPServer(BaseService):
    server = None
    host = None
    port = None

    def __init__(
            self,
            handler: Callable[..., Any],
            host: str = '127.0.0.1',
            port: int = 8545,
            token: CancelToken = None,
            loop: asyncio.AbstractEventLoop = None) -> None:
        super().__init__(token=token, loop=loop)
        self.host = host
        self.port = port
        self.server = web.Server(handler)

    async def _run(self) -> None:
        runner = web.ServerRunner(self.server)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        self.logger.info("Running HTTP Server %s:%d", self.host, self.port)
        await site.start()

        await self.cancellation()

    async def _cleanup(self) -> None:
        self.logger.info("Closing HTTPServer...")
        await self.server.shutdown()
