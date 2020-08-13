"""
This module exists because these classes end up being unpickleable if defined
in the tests modules.
"""
import asyncio
from pathlib import Path
import subprocess
import tempfile

import trio

from async_service import Service, background_asyncio_service, background_trio_service
from asyncio_run_in_process.typing import SubprocessKwargs
from eth_utils.toolz import merge
from lahja import EndpointAPI, BaseEvent

from trinity.extensibility import AsyncioIsolatedComponent, TrioIsolatedComponent
from trinity._utils.logging import get_logger


class IsStarted(BaseEvent):
    def __init__(self, path: Path) -> None:
        self.path = path


class MonitoringTaskCalled(BaseEvent):
    pass


class ComponentTestService(Service):
    logger = get_logger('trinity.testing.ServiceForTest')

    def __init__(self, event_bus: EndpointAPI) -> None:
        self.event_bus = event_bus

    async def run(self) -> None:
        path = Path(tempfile.NamedTemporaryFile().name)
        self.logger.info('Broadcasting `IsStarted(%s)`', path)
        try:
            await self.event_bus.broadcast(IsStarted(path))
            self.logger.info('Waiting for cancellation')
            await self.manager.wait_finished()
        except (asyncio.CancelledError, trio.Cancelled) as err:
            self.logger.info('Got %r: touching `%s`', err, path)
            path.touch()
        except BaseException:
            self.logger.exception("Unexpected error in ComponentTestService")
            raise


class AsyncioComponentForTest(AsyncioIsolatedComponent):
    name = "component-test-asyncio"
    endpoint_name = 'component-test-asyncio'

    async def _loop_monitoring_task(self, event_bus: EndpointAPI) -> None:
        # Wait forever, as this task is supposed to run forever and will cause the component to
        # terminate if it returns.
        await event_bus.broadcast(MonitoringTaskCalled())
        await asyncio.Future()

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

    async def do_run(self, event_bus: EndpointAPI) -> None:
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
            self.logger.info('Finished: `do_run`')


class TrioComponentForTest(TrioIsolatedComponent):
    name = "component-test-trio"
    endpoint_name = 'component-test-trio'

    async def _loop_monitoring_task(self, event_bus: EndpointAPI) -> None:
        await event_bus.broadcast(MonitoringTaskCalled())
        # Wait forever, as this task is supposed to run forever and will cause the component to
        # terminate if it returns.
        await trio.sleep_forever()

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

    async def do_run(self, event_bus: EndpointAPI) -> None:
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

    async def _loop_monitoring_task(self, event_bus: EndpointAPI) -> None:
        # Wait forever, as this task is supposed to run forever and will cause the component to
        # terminate if it returns.
        await asyncio.Future()

    async def do_run(self, event_bus: EndpointAPI) -> None:
        async with background_asyncio_service(IdleService(event_bus)):
            raise ComponentException("This is a component that crashes after starting a service")


class TrioBrokenComponent(TrioComponentForTest):
    name = "component-test-trio-broken"
    endpoint_name = 'component-test-trio-broken'

    async def _loop_monitoring_task(self, event_bus: EndpointAPI) -> None:
        # Wait forever, as this task is supposed to run forever and will cause the component to
        # terminate if it returns.
        await trio.sleep_forever()

    async def do_run(self, event_bus: EndpointAPI) -> None:
        async with background_trio_service(IdleService(event_bus)):
            raise ComponentException("This is a component that crashes after starting a service")
