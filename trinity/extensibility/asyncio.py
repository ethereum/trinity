from abc import abstractmethod
import asyncio
from typing import (
    Any,
    Callable,
)

from asyncio_run_in_process import run_in_process
from asyncio_run_in_process.typing import SubprocessKwargs
from async_service import background_asyncio_service
from async_service.asyncio import cleanup_tasks
from lahja import EndpointAPI

from p2p.asyncio_utils import create_task

from trinity._utils.logging import child_process_logging
from trinity._utils.profiling import profiler
from trinity._utils.timer import Timer

from .component import BaseIsolatedComponent, TReturn
from .event_bus import AsyncioEventBusService


class AsyncioIsolatedComponent(BaseIsolatedComponent):

    async def _run_in_process(
            self,
            async_fn: Callable[..., TReturn],
            *args: Any,
            subprocess_kwargs: 'SubprocessKwargs' = None,
    ) -> TReturn:
        return await run_in_process(async_fn, *args, subprocess_kwargs=subprocess_kwargs)

    # FIXME: Disabled by default
    async def _loop_monitoring_task(self) -> None:
        debug_log_since = 0
        while True:
            timer = Timer()
            await asyncio.sleep(self.loop_monitoring_wakeup_interval)
            delay = timer.elapsed - self.loop_monitoring_wakeup_interval
            # FIXME: Ideally we'd use a debug2 here, but because of
            # https://github.com/ethereum/trinity/issues/1971 we need to use debug so only log
            # every 10th time we're called.
            debug_log_since += 1
            if debug_log_since == 10:
                self.logger.debug("Loop monitoring task called; delay=%.3fs", delay)
                debug_log_since = 0
            if delay > self.loop_monitoring_max_delay:
                pending_tasks = len([task for task in asyncio.all_tasks() if not task.done()])
                self.logger.warning(
                    "Event loop blocked or overloaded: delay=%.3fs, tasks=%d",
                    delay,
                    pending_tasks,
                )

    async def _do_run(self) -> None:
        with child_process_logging(self._boot_info):
            endpoint_name = self.get_endpoint_name()
            event_bus_service = AsyncioEventBusService(
                self._boot_info.trinity_config,
                endpoint_name,
            )
            loop_monitoring_task = create_task(
                self._loop_monitoring_task(),
                f'AsyncioIsolatedComponent/{self.name}/loop_monitoring_task')
            # FIXME: Must terminate component if loop_monitoring_task terminates.
            async with cleanup_tasks(loop_monitoring_task):
                # FIXME: Must terminate component if event_bus_service terminates.
                async with background_asyncio_service(event_bus_service):
                    event_bus = await event_bus_service.get_event_bus()

                    try:
                        if self._boot_info.profile:
                            with profiler(f'profile_{self.get_endpoint_name()}'):
                                await self.do_run(event_bus)
                        else:
                            # XXX: When open_in_process() injects a KeyboardInterrupt into us (via
                            # coro.throw()), we hang forever here, until open_in_process() times
                            # out and sends us a SIGTERM, at which point we exit without executing
                            # either the except or the finally blocks below.
                            # See https://github.com/ethereum/trinity/issues/1711 for more.
                            await self.do_run(event_bus)
                    except KeyboardInterrupt:
                        # Currently we never reach this code path, but when we fix the issue above
                        # it will be needed.
                        return
                    finally:
                        # Once we start seeing this in the logs after a Ctrl-C, we'll likely have
                        # figured out the issue above.
                        self.logger.debug("%s: do_run() finished", self)

    @abstractmethod
    async def do_run(self, event_bus: EndpointAPI) -> None:
        """
        Define the entry point of the component. Should be overwritten in subclasses.
        """
        ...
