from abc import abstractmethod
import asyncio
import logging
import signal

from lahja import EndpointAPI

from p2p.service import run_service

from trinity._utils.ipc import kill_process_gracefully
from trinity._utils.mp import ctx
from trinity.boot_info import BootInfo

from .component import BaseIsolatedComponent
from .event_bus import AsyncioEventBusService


class AsyncioIsolatedComponent(BaseIsolatedComponent):
    async def run(self) -> None:
        """
        Call chain is:

        - multiprocessing.Process -> _run_process
            * isolates to a new process
        - _run_process -> run_process
            * sets up subprocess logging
        - run_process -> _do_run
            * runs the event loop and transitions into async context
        - _do_run -> do_run
            * sets up event bus and then enters user function.
        """
        process = ctx.Process(
            target=self._run_process,
            args=(self._boot_info,),
        )
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, process.start)
        try:
            await loop.run_in_executor(None, process.join)
        finally:
            kill_process_gracefully(
                process,
                logging.getLogger('trinity.extensibility.AsyncioIsolatedComponent'),
            )

    @classmethod
    def run_process(cls, boot_info: BootInfo) -> None:
        loop = asyncio.get_event_loop()
        task = asyncio.ensure_future(cls._do_run(boot_info))
        loop.add_signal_handler(signal.SIGINT, task.cancel)
        loop.add_signal_handler(signal.SIGTERM, task.cancel)
        try:
            loop.run_until_complete(task)
        except asyncio.CancelledError:
            pass
        finally:
            loop.close()

    @classmethod
    async def _do_run(cls, boot_info: BootInfo) -> None:
        endpoint_name = cls._get_endpoint_name()
        event_bus_service = AsyncioEventBusService(
            boot_info.trinity_config,
            endpoint_name,
        )
        async with run_service(event_bus_service):
            await event_bus_service.wait_event_bus_available()
            event_bus = event_bus_service.get_event_bus()
            try:
                await cls.do_run(boot_info, event_bus)
            except asyncio.CancelledError:
                await event_bus_service.cancel()
                raise

    @classmethod
    @abstractmethod
    async def do_run(self, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        """
        Define the entry point of the component. Should be overwritten in subclasses.
        """
        ...
