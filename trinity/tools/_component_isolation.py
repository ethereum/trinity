"""
This module exists because these classes end up being unpickleable if defined
in the tests modules.
"""
import logging
from pathlib import Path
import subprocess
import tempfile

from async_service import Service
from async_service import background_asyncio_service
from asyncio_run_in_process.typing import SubprocessKwargs

from eth_utils.toolz import merge

from lahja import EndpointAPI, BaseEvent

from trinity.boot_info import BootInfo
from trinity.extensibility import AsyncioIsolatedComponent


class IsStarted(BaseEvent):
    def __init__(self, path: Path) -> None:
        self.path = path


class AsyncioComponentService(Service):
    logger = logging.getLogger('trinity.testing.ServiceForTest')

    def __init__(self, event_bus: EndpointAPI) -> None:
        self.event_bus = event_bus

    async def run(self) -> None:
        self.logger.debug('Broadcasting `IsStarted`')
        path = Path(tempfile.NamedTemporaryFile().name)
        try:
            await self.event_bus.broadcast(IsStarted(path))
            self.logger.debug('Waiting for cancellation')
            await self.manager.wait_finished()
        except BaseException as err:
            self.logger.debug('Exiting due to error: `%r`', err)
        finally:
            self.logger.debug('Got cancellation: touching `%s`', path)
            path.touch()


class AsyncioComponentForTest(AsyncioIsolatedComponent):
    name = "component-test"
    endpoint_name = 'component-test'
    logger = logging.getLogger('trinity.testing.ComponentForTest')

    def get_subprocess_kwargs(self) -> SubprocessKwargs:
        return merge(
            super().get_subprocess_kwargs(),
            {
                'stdin': subprocess.PIPE,
                'stdout': subprocess.PIPE,
                'stderr': subprocess.PIPE,
            }
        )

    @property
    def is_enabled(self) -> bool:
        return True

    @classmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        cls.logger.debug('Entered `do_run`')
        service = AsyncioComponentService(event_bus)
        try:
            async with background_asyncio_service(service) as manager:
                cls.logger.debug('Running service')
                try:
                    await manager.wait_finished()
                finally:
                    cls.logger.debug('Exiting `do_run`')
        finally:
            # XXX: We never reach this line, so if you run test_isolated_component.py by itself it
            # will pass but hang forever after pytest reports success.
            # Figuring this out is probably the key to fixing our shutdown.
            cls.logger.debug('Finished: `do_run`')
