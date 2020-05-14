"""
This module exists because these classes end up being unpickleable if defined
in the tests modules.
"""
import logging
from pathlib import Path
import subprocess
import tempfile

from async_service import Service, background_asyncio_service, background_trio_service
from asyncio_run_in_process.typing import SubprocessKwargs
from eth_utils.toolz import merge
from lahja import EndpointAPI, BaseEvent

from trinity.boot_info import BootInfo
from trinity.extensibility import AsyncioIsolatedComponent, TrioIsolatedComponent


class IsStarted(BaseEvent):
    def __init__(self, path: Path) -> None:
        self.path = path


class ComponentTestService(Service):
    logger = logging.getLogger('trinity.testing.ServiceForTest')

    def __init__(self, event_bus: EndpointAPI) -> None:
        self.event_bus = event_bus

    async def run(self) -> None:
        path = Path(tempfile.NamedTemporaryFile().name)
        self.logger.info('Broadcasting `IsStarted(%s)`', path)
        try:
            await self.event_bus.broadcast(IsStarted(path))
            self.logger.info('Waiting for cancellation')
            await self.manager.wait_finished()
        except BaseException as err:
            self.logger.info('Exiting due to error: `%r`', err)
        finally:
            self.logger.info('Got cancellation: touching `%s`', path)
            path.touch()


class AsyncioComponentForTest(AsyncioIsolatedComponent):
    name = "component-test-asyncio"
    endpoint_name = 'component-test-asyncio'
    logger = logging.getLogger('trinity.testing.AsyncioComponentForTest')

    def get_subprocess_kwargs(self) -> SubprocessKwargs:
        # This is needed so that pytest can capture the subproc's output. Otherwise it will crash
        # with a "io.UnsupportedOperation: redirected stdin is pseudofile, has no fileno()" error.
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

    async def do_run(self, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        self.logger.info('Entered `do_run`')
        service = ComponentTestService(event_bus)
        try:
            async with background_asyncio_service(service) as manager:
                self.logger.info('Running service')
                try:
                    await manager.wait_finished()
                finally:
                    self.logger.info('Exiting `do_run`')
        finally:
            # XXX: We never reach this line, so if you run test_isolated_component.py by itself it
            # will pass but hang forever after pytest reports success.
            # Figuring this out is probably the key to fixing our shutdown.
            self.logger.info('Finished: `do_run`')


class TrioComponentForTest(TrioIsolatedComponent):
    name = "component-test-trio"
    endpoint_name = 'component-test-trio'

    def get_subprocess_kwargs(self) -> SubprocessKwargs:
        # This is needed so that pytest can capture the subproc's output. Otherwise it will crash
        # with a "io.UnsupportedOperation: redirected stdin is pseudofile, has no fileno()" error.
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

    async def do_run(self, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        self.logger.info('Entered `do_run`')
        service = ComponentTestService(event_bus)
        try:
            async with background_trio_service(service) as manager:
                self.logger.info('Running service')
                try:
                    await manager.wait_finished()
                finally:
                    self.logger.info('Exiting `do_run`')
        finally:
            self.logger.info('Finished: `do_run`')


class IdleService(Service):
    def __init__(self, event_bus: EndpointAPI) -> None:
        self.event_bus = event_bus

    async def run(self) -> None:
        await self.event_bus.broadcast(IsStarted(None))
        await self.manager.wait_finished()


class ComponentException(Exception):
    pass


class AsyncioBrokenComponent(AsyncioComponentForTest):
    name = "component-test-asyncio-broken"
    endpoint_name = 'component-test-asyncio-broken'

    async def do_run(self, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        async with background_asyncio_service(IdleService(event_bus)):
            raise ComponentException("This is a component that crashes after starting a service")


class TrioBrokenComponent(TrioComponentForTest):
    name = "component-test-trio-broken"
    endpoint_name = 'component-test-trio-broken'

    async def do_run(self, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        async with background_trio_service(IdleService(event_bus)):
            raise ComponentException("This is a component that crashes after starting a service")
