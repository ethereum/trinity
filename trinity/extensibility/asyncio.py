from abc import abstractmethod
import asyncio
import logging
import signal
from typing import Optional

from asyncio_run_in_process import open_in_process
from asyncio_run_in_process.typing import SubprocessKwargs
from async_service import background_asyncio_service
from lahja import EndpointAPI


from trinity._utils.logging import child_process_logging
from trinity._utils.profiling import profiler
from trinity.boot_info import BootInfo

from .component import BaseIsolatedComponent
from .event_bus import AsyncioEventBusService


logger = logging.getLogger('trinity.extensibility.asyncio.AsyncioIsolatedComponent')


class AsyncioIsolatedComponent(BaseIsolatedComponent):
    def get_subprocess_kwargs(self) -> Optional[SubprocessKwargs]:
        # Note that this method currently only exist to facilitate testing.
        return None

    async def run(self) -> None:
        proc_ctx = open_in_process(
            self._do_run,
            self._boot_info,
            subprocess_kwargs=self.get_subprocess_kwargs(),
        )
        async with proc_ctx as proc:
            try:
                await proc.wait()
            except asyncio.CancelledError as err:
                logger.debug('Component %s exiting. Sending SIGINT to pid=%d', self, proc.pid)
                proc.send_signal(signal.SIGINT)
                try:
                    await asyncio.wait_for(proc.wait(), timeout=2)
                except asyncio.TimeoutError:
                    logger.debug(
                        'Component %s running in process pid=%d timed out '
                        'during shutdown. Sending SIGTERM and exiting.',
                        self,
                        proc.pid,
                    )
                    proc.send_signal(signal.SIGTERM)
                finally:
                    raise err

    @classmethod
    async def _do_run(cls, boot_info: BootInfo) -> None:
        with child_process_logging(boot_info):
            endpoint_name = cls.get_endpoint_name()
            event_bus_service = AsyncioEventBusService(
                boot_info.trinity_config,
                endpoint_name,
            )
            async with background_asyncio_service(event_bus_service):
                event_bus = await event_bus_service.get_event_bus()

                try:
                    if boot_info.profile:
                        with profiler(f'profile_{cls.get_endpoint_name()}'):
                            await cls.do_run(boot_info, event_bus)
                    else:
                        await cls.do_run(boot_info, event_bus)
                except KeyboardInterrupt:
                    return

    @classmethod
    @abstractmethod
    async def do_run(self, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        """
        Define the entry point of the component. Should be overwritten in subclasses.
        """
        ...
