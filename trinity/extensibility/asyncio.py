from abc import abstractmethod
import os
from typing import Optional

from asyncio_run_in_process import open_in_process
from asyncio_run_in_process.typing import SubprocessKwargs
from async_service import background_asyncio_service
from lahja import EndpointAPI


from trinity._utils.logging import child_process_logging, get_logger
from trinity._utils.profiling import profiler
from trinity.boot_info import BootInfo

from .component import BaseIsolatedComponent
from .event_bus import AsyncioEventBusService


logger = get_logger('trinity.extensibility.asyncio.AsyncioIsolatedComponent')


class AsyncioIsolatedComponent(BaseIsolatedComponent):

    def get_subprocess_kwargs(self) -> Optional[SubprocessKwargs]:
        # By default we want every child process its own process group leader as we don't want a
        # Ctrl-C in the terminal to send a SIGINT to each one of our process, as that is already
        # handled by open_in_process().
        start_new_session = True
        if os.getenv('TRINITY_SINGLE_PROCESS_GROUP') == "1":
            # This is needed because some of our integration tests rely on all processes being in
            # a single process group.
            start_new_session = False
        return {'start_new_session': start_new_session}

    async def run(self) -> None:
        proc_ctx = open_in_process(
            self._do_run,
            self._boot_info,
            subprocess_kwargs=self.get_subprocess_kwargs(),
        )
        async with proc_ctx as proc:
            await proc.wait_result_or_raise()

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
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        """
        Define the entry point of the component. Should be overwritten in subclasses.
        """
        ...
