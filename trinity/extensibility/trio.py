import trio

from lahja import TrioEndpoint, EndpointAPI
from p2p.trio_service import run_service

from trinity.events import PluginStartedEvent
from trinity.event_bus import TrioEventBusService

from .plugin import BaseIsolatedPlugin


class TrioIsolatedPlugin(BaseIsolatedPlugin):
    """
    A :class:`~trinity.extensibility.plugin.BaseIsolatedPlugin` runs in an isolated process and
    hence provides security and flexibility by not making assumptions about its internal
    operations.

    Such plugins are free to use non-blocking asyncio as well as synchronous calls. When an
    isolated plugin is stopped it does first receive a SIGINT followed by a SIGTERM soon after.
    It is up to the plugin to handle these signals accordingly.
    """
    _event_bus: TrioEndpoint = None

    @property
    def event_bus(self) -> EndpointAPI:
        if self._event_bus is None:
            raise AttributeError(f"Event bus not yet available.. plugin status: {self.status}")
        return self._event_bus

    def _spawn_start(self) -> None:
        """
        This function runs in a different process than the main plugin.
        """
        self._setup_logging()

        with self.boot_info.trinity_config.process_id_file(self.normalized_name):
            try:
                trio.run(self._prepare_and_start)
            except KeyboardInterrupt:
                return

    async def _prepare_and_start(self) -> None:
        service = TrioEventBusService(self.boot_info._trinity_config, self.normalized_name)
        async with run_service(service):
            await service.wait_event_bus_available()
            self._event_bus = service.get_event_bus()

            await self.do_start()

            await self.event_bus.broadcast(
                PluginStartedEvent(type(self))
            )
