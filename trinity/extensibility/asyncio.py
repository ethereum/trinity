import asyncio

from lahja import AsyncioEndpoint, EndpointAPI

from trinity.extensibility.events import PluginStartedEvent
from trinity.event_bus import AsyncioEventBusService

from .plugin import BaseIsolatedPlugin


class AsyncioIsolatedPlugin(BaseIsolatedPlugin):
    _event_bus: AsyncioEndpoint = None

    @property
    def event_bus(self) -> EndpointAPI:
        if self._event_bus is None:
            raise AttributeError(f"Event bus not yet available.. plugin status: {self.status}")
        return self._event_bus

    def _spawn_start(self) -> None:
        self._setup_logging()

        with self.boot_info.trinity_config.process_id_file(self.normalized_name):
            loop = asyncio.get_event_loop()
            asyncio.ensure_future(self._prepare_start())
            loop.run_forever()
            loop.close()

    async def _prepare_start(self) -> None:
        event_bus_service = AsyncioEventBusService(self._trinity_config, self.normalized_name)
        asyncio.ensure_future(event_bus_service.run())
        await event_bus_service.wait_event_bus_available()
        self._event_bus = event_bus_service.get_event_bus()

        await self.event_bus.broadcast(
            PluginStartedEvent(type(self))
        )

        self.do_start()
