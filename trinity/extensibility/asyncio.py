from abc import abstractmethod
import asyncio

from asyncio_run_in_process import run_in_process
from async_service import background_asyncio_service
from lahja import EndpointAPI

from p2p.asyncio_utils import create_task, wait_first

from trinity._utils.logging import child_process_logging
from trinity._utils.profiling import profiler
from trinity._utils.timer import Timer

from .component import BaseIsolatedComponent
from .event_bus import AsyncioEventBusService


class AsyncioIsolatedComponent(BaseIsolatedComponent):

    async def run_in_process(self) -> None:
        await run_in_process(self._do_run, subprocess_kwargs=self.get_subprocess_kwargs())

    # The EndpointAPI argument here is currently only used by tests.
    async def _loop_monitoring_task(self, _: EndpointAPI) -> None:
        while True:
            timer = Timer()
            await asyncio.sleep(self.loop_monitoring_wakeup_interval)
            delay = timer.elapsed - self.loop_monitoring_wakeup_interval
            self.logger.debug2("Loop monitoring task called; delay=%.3fs", delay)
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
            async with background_asyncio_service(event_bus_service) as eventbus_manager:
                event_bus = await event_bus_service.get_event_bus()
                loop_monitoring_task = create_task(
                    self._loop_monitoring_task(event_bus),
                    f'AsyncioIsolatedComponent/{self.name}/loop_monitoring_task')

                do_run_task = create_task(
                    self.do_run(event_bus),
                    f'AsyncioIsolatedComponent/{self.name}/do_run')
                eventbus_task = create_task(
                    eventbus_manager.wait_finished(),
                    f'AsyncioIsolatedComponent/{self.name}/eventbus/wait_finished')
                try:
                    max_wait_after_cancellation = 2
                    if self._boot_info.profile:
                        with profiler(f'profile_{self.get_endpoint_name()}'):
                            await wait_first(
                                [do_run_task, eventbus_task, loop_monitoring_task],
                                max_wait_after_cancellation,
                            )
                    else:
                        # XXX: When open_in_process() injects a KeyboardInterrupt into us (via
                        # coro.throw()), we hang forever here, until open_in_process() times
                        # out and sends us a SIGTERM, at which point we exit without executing
                        # either the except or the finally blocks below.
                        # See https://github.com/ethereum/trinity/issues/1711 for more.
                        await wait_first(
                            [do_run_task, eventbus_task, loop_monitoring_task],
                            max_wait_after_cancellation,
                        )
                except KeyboardInterrupt:
                    self.logger.debug("%s: KeyboardInterrupt", self)
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
