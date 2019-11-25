from abc import abstractmethod

import trio

from lahja import TrioEndpoint

from async_service.trio import TrioManager

from trinity.extensibility.events import ComponentStartedEvent

from .component import BaseIsolatedComponent


class TrioIsolatedComponent(BaseIsolatedComponent):
    _event_bus: TrioEndpoint = None

    @property
    def event_bus(self) -> TrioEndpoint:
        if self._event_bus is None:
            raise AttributeError("Event bus is not available yet")
        return self._event_bus

    @abstractmethod
    async def run(self) -> None:
        ...

    def _spawn_start(self) -> None:
        async def _run() -> None:
            from trinity.event_bus import TrioEventBusService
            self._event_bus_service = TrioEventBusService(
                self.boot_info.trinity_config,
                self.normalized_name,
            )
            async with trio.open_nursery() as nursery:
                nursery.start_soon(TrioManager.run_service, self._event_bus_service)
                await self._event_bus_service.wait_event_bus_available()
                self._event_bus = self._event_bus_service.get_event_bus()
                await self.event_bus.broadcast(ComponentStartedEvent(type(self)))
                await self.run()

        trio.run(_run)
