from abc import abstractmethod

import trio
from async_service import background_trio_service

from lahja import EndpointAPI

from asyncio_run_in_process import open_in_process_with_trio

from trinity._utils.logging import child_process_logging, get_logger
from trinity._utils.profiling import profiler
from trinity.events import ShutdownRequest

from .component import BaseComponent, BaseIsolatedComponent
from .event_bus import TrioEventBusService


class TrioComponent(BaseComponent):
    """
    ``TrioComponent`` is a component that executes in-process
    with the ``trio`` concurrency library.
    """
    pass


class TrioIsolatedComponent(BaseIsolatedComponent):
    logger = get_logger('trinity.extensibility.TrioIsolatedComponent')

    async def run(self) -> None:
        proc_ctx = open_in_process_with_trio(
            self._do_run,
            subprocess_kwargs=self.get_subprocess_kwargs(),
        )
        try:
            async with proc_ctx as proc:
                await proc.wait_result_or_raise()
        finally:
            # Only attempt to log the proc's returncode if we succesfully entered the context
            # manager above.
            if 'proc' in locals():
                returncode = getattr(proc, 'returncode', 'unset')
                self.logger.debug("%s terminated: returncode=%s", self.name, returncode)

    async def run_process(self, event_bus: EndpointAPI) -> None:
        try:
            if self._boot_info.profile:
                with profiler(f'profile_{self.get_endpoint_name()}'):
                    await self.do_run(event_bus)
            else:
                await self.do_run(event_bus)
        except (trio.Cancelled, trio.MultiError):
            # These are expected, when trinity is terminating because of a Ctrl-C
            raise
        except BaseException:
            # Leaving trinity running after a component crashes can lead to unexpected
            # behavior that'd be hard to debug/reproduce, so for now we shut it down if
            # any component crashes unexpectedly.
            event_bus.broadcast_nowait(ShutdownRequest(f"Unexpected error in {self}"))
            # Because of an issue in the ComponentManager (see comment in
            # _cleanup_component_task), when a component crashes and requests trinity to
            # shutdown, there's still a chance its exception could be lost, so we log it
            # here as well.
            self.logger.exception(
                "Unexpected error in component %s, shutting down trinity", self)
            raise

    async def _do_run(self) -> None:
        with child_process_logging(self._boot_info):
            event_bus_service = TrioEventBusService(
                self._boot_info.trinity_config,
                self.get_endpoint_name(),
            )
            async with background_trio_service(event_bus_service):
                event_bus = await event_bus_service.get_event_bus()
                async with trio.open_nursery() as nursery:
                    nursery.start_soon(self.run_process, event_bus)
                    try:
                        await trio.sleep_forever()
                    except KeyboardInterrupt:
                        self.logger.debug("%s: Got KeyboardInterrupt, exiting", self.name)
                        nursery.cancel_scope.cancel()

    @abstractmethod
    async def do_run(self, event_bus: EndpointAPI) -> None:
        """
        This is where subclasses should override
        """
        ...
