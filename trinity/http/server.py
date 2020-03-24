import asyncio
from typing import Collection

from aiohttp import web

from cancel_token import CancelToken

from p2p.service import BaseService


class HTTPServer(BaseService):
    """
    NOTE: this class differs from ``trinity.http.main.HTTPServer``
    in that it accepts a collection of ``web.RouteDef`` and builds
    a ``web.Application``, rather than the lower-level ``web.Server``.
    """

    host: str
    port: int

    def __init__(
        self,
        routes: Collection[web.RouteDef],
        host: str = "127.0.0.1",
        port: int = 8545,
        token: CancelToken = None,
        loop: asyncio.AbstractEventLoop = None,
    ) -> None:
        super().__init__(token=token, loop=loop)
        self.host = host
        self.port = port
        app = web.Application()
        app.add_routes(routes)
        self._runner = web.AppRunner(app)

    async def _run(self) -> None:
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        self.logger.info("Running HTTP Server %s:%d", self.host, self.port)
        await site.start()

        await self.cancellation()

    async def _cleanup(self) -> None:
        self.logger.info("Closing HTTPServer...")
        await self._runner.cleanup()
