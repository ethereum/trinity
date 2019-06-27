import asyncio


import trio
import trio_asyncio

from lahja import AsyncioEndpoint

from trinity.extensibility.events import PluginStartedEvent

from .plugin import BaseIsolatedPlugin


class AsyncioIsolatedPlugin(BaseIsolatedPlugin):
    _event_bus: AsyncioEndpoint = None
    _loop: asyncio.AbstractEventLoop

    @property
    def event_bus(self) -> AsyncioEndpoint:
        if self._event_bus is None:
            raise AttributeError("Event bus is not available yet")
        return self._event_bus

    def _spawn_start(self) -> None:
        self._setup_logging()

        with self.boot_info.trinity_config.process_id_file(self.normalized_name):
            trio.run(self._spawn_start_coro)

    async def _spawn_start_coro(self) -> None:
        async with trio_asyncio.open_loop() as loop:
            self._loop = loop
            await self._prepare_start()

    @trio_asyncio.aio_as_trio
    async def _prepare_start(self) -> None:
        # prevent circular import
        from trinity.event_bus import AsyncioEventBusService

        self._event_bus_service = AsyncioEventBusService(
            self.boot_info.trinity_config,
            self.normalized_name,
        )
        asyncio.ensure_future(self._event_bus_service.run())
        await self._event_bus_service.wait_event_bus_available()
        self._event_bus = self._event_bus_service.get_event_bus()

        await self.event_bus.broadcast(
            PluginStartedEvent(type(self))
        )

        await self.do_start()
