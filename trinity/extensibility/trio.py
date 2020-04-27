from abc import abstractmethod
import asyncio
from multiprocessing import Process
import os
import signal

import trio
from async_service import background_trio_service
from asyncio_run_in_process.constants import SIGINT_TIMEOUT_SECONDS, SIGTERM_TIMEOUT_SECONDS

from lahja import EndpointAPI

from trinity._utils.logging import child_process_logging, get_logger
from trinity._utils.mp import ctx
from trinity._utils.profiling import profiler
from trinity.boot_info import BootInfo

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
        # FIXME: Use subprocess.Popen() so that we can make every process be its own process group
        # leader, like in AsyncioIsolatedComponent.
        process = ctx.Process(
            target=self.run_process,
            args=(self._boot_info,),
        )
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, process.start)
        try:
            await loop.run_in_executor(None, process.join)
        finally:
            # NOTE: Since the subprocess we start here runs in the same process group as us (see
            # comment above as to why), when we get a Ctrl-C in the terminal the subprocess will
            # get it as well, so we first wait a fraction of SIGINT_TIMEOUT_SECONDS for it to
            # terminate (which is usually enough for trio to terminate all pending tasks), and
            # only if it hasn't finished by then we send it a SIGINT. If we send a SIGINT straight
            # away we cause trio to crash when we've been Ctrl-C'ed (because it would get two
            # SIGINTs), and if we don't send one at all the subprocess never terminates when
            # trinity exits because of a crash.
            self.logger.debug("Waiting for process %d to terminate", process.pid)
            await loop.run_in_executor(None, process.join, SIGINT_TIMEOUT_SECONDS / 4)
            if process.is_alive():
                self.logger.debug("Process %d did not terminate, sending SIGINT", process.pid)
                self._send_signal_and_join(process, signal.SIGINT, SIGINT_TIMEOUT_SECONDS)
            if process.is_alive():
                self.logger.debug("Process %d did not terminate, sending SIGTERM", process.pid)
                self._send_signal_and_join(process, signal.SIGTERM, SIGTERM_TIMEOUT_SECONDS)

    def _send_signal_and_join(self, process: Process, sig: int, timeout: int) -> None:
        try:
            os.kill(process.pid, sig)
        except ProcessLookupError:
            self.logger.debug("Process %d has already terminated", process.pid)
            return
        # XXX: Using process.join() here is far from ideal as it blocks the main process,
        # forcing us to wait in sequence for every trio components, but try and run it
        # asynchronously (using an executor, like above) and you'll get a
        #   RuntimeError: cannot reuse already awaited coroutine
        process.join(timeout)
        self.logger.debug(
            "process (%d) .join() returned, exitcode=%s", process.pid, process.exitcode)

    @classmethod
    def run_process(cls, boot_info: BootInfo) -> None:
        with child_process_logging(boot_info):
            if boot_info.profile:
                with profiler(f'profile_{cls.get_endpoint_name()}'):
                    trio.run(cls._do_run, boot_info)
            else:
                trio.run(cls._do_run, boot_info)

    @classmethod
    async def _do_run(cls, boot_info: BootInfo) -> None:
        event_bus_service = TrioEventBusService(
            boot_info.trinity_config,
            cls.get_endpoint_name(),
        )
        with trio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signal_aiter:
            async with background_trio_service(event_bus_service):
                event_bus = await event_bus_service.get_event_bus()
                async with trio.open_nursery() as nursery:
                    nursery.start_soon(cls.do_run, boot_info, event_bus)
                    async for sig in signal_aiter:
                        nursery.cancel_scope.cancel()

    @classmethod
    @abstractmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        """
        This is where subclasses should override
        """
        ...
