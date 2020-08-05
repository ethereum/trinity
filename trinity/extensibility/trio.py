from abc import abstractmethod
from typing import (
    Any,
    Callable,
)

import trio
from async_service import background_trio_service

from lahja import EndpointAPI

from asyncio_run_in_process import run_in_process_with_trio
from asyncio_run_in_process.typing import SubprocessKwargs

from trinity._utils.logging import child_process_logging
from trinity._utils.profiling import profiler
from trinity._utils.timer import Timer

from .component import BaseComponent, BaseIsolatedComponent, TReturn
from .event_bus import TrioEventBusService


class TrioComponent(BaseComponent):
    """
    ``TrioComponent`` is a component that executes in-process
    with the ``trio`` concurrency library.
    """

    @abstractmethod
    async def run(self) -> None:
        ...


class TrioIsolatedComponent(BaseIsolatedComponent):

    async def _run_in_process(
            self,
            async_fn: Callable[..., TReturn],
            *args: Any,
            subprocess_kwargs: 'SubprocessKwargs' = None,
    ) -> TReturn:
        return await run_in_process_with_trio(async_fn, *args, subprocess_kwargs=subprocess_kwargs)

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

    # FIXME: Disabled by default
    async def _loop_monitoring_task(self) -> None:
        debug_log_since = 0
        while True:
            timer = Timer()
            await trio.sleep(self.loop_monitoring_wakeup_interval)
            delay = timer.elapsed - self.loop_monitoring_wakeup_interval
            # FIXME: Ideally we'd use a debug2 here, but because of
            # https://github.com/ethereum/trinity/issues/1971 we need to use debug so only log
            # every 10th time we're called.
            debug_log_since += 1
            if debug_log_since == 10:
                self.logger.debug("Loop monitoring task called; delay=%.3fs", delay)
                debug_log_since = 0
            if delay > self.loop_monitoring_max_delay:
                stats = trio.hazmat.current_statistics()
                self.logger.warning(
                    "Event loop blocked or overloaded: delay=%.3fs, tasks=%d, stats=%s",
                    delay,
                    stats.tasks_living,
                    stats.io_statistics,
                )

    async def _do_run(self) -> None:
        with child_process_logging(self._boot_info):
            event_bus_service = TrioEventBusService(
                self._boot_info.trinity_config,
                self.get_endpoint_name(),
            )
            async with background_trio_service(event_bus_service):
                event_bus = await event_bus_service.get_event_bus()
                async with trio.open_nursery() as nursery:
                    nursery.start_soon(self._loop_monitoring_task)
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
