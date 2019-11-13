import asyncio
import sys
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Set,
)

from async_generator import asynccontextmanager

from trio import MultiError

from .abc import (
    ServiceAPI,
    ManagerAPI,
)
from .base import BaseManager
from .exceptions import (
    DaemonTaskExit,
    LifecycleError,
)


class AsyncioManager(BaseManager):
    # Tracking of the system level background tasks.
    _system_tasks: Set['asyncio.Future[None]']

    # Tracking of the background tasks that the service has initiated.
    _service_tasks: Set['asyncio.Future[Any]']

    def __init__(self,
                 service: ServiceAPI,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        if hasattr(service, 'manager'):
            raise LifecycleError("Service already has a manager.")
        else:
            service.manager = self

        self._service = service

        self._loop = loop

        # events
        self._started = asyncio.Event()
        self._cancelled = asyncio.Event()
        self._stopped = asyncio.Event()

        # locks
        self._run_lock = asyncio.Lock()

        # errors
        self._errors = []

        # task tracking
        self._service_tasks = set()
        self._system_tasks = set()

    #
    # System Tasks
    #
    async def _handle_cancelled(self) -> None:
        """
        Handles the cancellation triggering cancellation of the task nursery.
        """
        self.logger.debug('%s: _handle_cancelled waiting for cancellation', self)
        await self.wait_cancelled()
        self.logger.debug('%s: _handle_cancelled triggering task nursery cancellation', self)

        # trigger cancellation of all of the service tasks
        for task in self._service_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                self._errors.append(sys.exc_info())

    async def _handle_stopped(self) -> None:
        """
        Once the `_stopped` event is set this triggers cancellation of the system nursery.
        """
        self.logger.debug('%s: _handle_stopped waiting for stopped', self)
        await self.wait_stopped()
        self.logger.debug('%s: _handle_stopped triggering system nursery cancellation', self)

        # trigger cancellation of all of the system tasks
        for task in self._system_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                self._errors.append(sys.exc_info())

    @classmethod
    async def run_service(cls, service: ServiceAPI, loop: asyncio.AbstractEventLoop = None) -> None:
        manager = cls(service, loop=loop)
        await manager.run()

    async def run(self) -> None:
        if self._run_lock.locked():
            raise LifecycleError(
                "Cannot run a service with the run lock already engaged.  Already started?"
            )
        elif self.is_started:
            raise LifecycleError("Cannot run a service which is already started.")

        async with self._run_lock:
            try:
                handle_cancelled_task = asyncio.ensure_future(
                    self._handle_cancelled(),
                    loop=self._loop,
                )
                handle_stopped_task = asyncio.ensure_future(self._handle_stopped(), loop=self._loop)

                self._system_tasks.add(handle_cancelled_task)
                self._system_tasks.add(handle_stopped_task)

                self.run_task(self._service.run)

                self._started.set()

                # block here until all service tasks have finished
                await self._wait_service_tasks()
            finally:
                # Mark as having stopped
                self._stopped.set()

            # block here until all system tasks have finished.
            await asyncio.wait(
                (handle_cancelled_task, handle_stopped_task),
                return_when=asyncio.ALL_COMPLETED,
            )
        self.logger.debug('%s stopped', self)

        # If an error occured, re-raise it here
        if self.did_error:
            raise MultiError(tuple(
                exc_value.with_traceback(exc_tb)
                for _, exc_value, exc_tb
                in self._errors
            ))

    async def _wait_service_tasks(self) -> None:
        while True:
            done, pending = await asyncio.wait(
                self._service_tasks,
                return_when=asyncio.ALL_COMPLETED,
            )
            if all(task.done() for task in self._service_tasks):
                break

    #
    # Event API mirror
    #
    @property
    def is_started(self) -> bool:
        return self._started.is_set()

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled.is_set()

    @property
    def is_stopped(self) -> bool:
        return self._stopped.is_set()

    #
    # Control API
    #
    def cancel(self) -> None:
        if not self.is_started:
            raise LifecycleError("Cannot cancel as service which was never started.")
        self._cancelled.set()

    #
    # Wait API
    #
    async def wait_started(self) -> None:
        await self._started.wait()

    async def wait_cancelled(self) -> None:
        await self._cancelled.wait()

    async def wait_stopped(self) -> None:
        await self._stopped.wait()

    async def wait_forever(self) -> None:
        await self.wait_stopped()

    async def _run_and_manage_task(self,
                                   async_fn: Callable[..., Awaitable[Any]],
                                   *args: Any,
                                   daemon: bool,
                                   name: str) -> None:
        try:
            await async_fn(*args)
        except asyncio.CancelledError:
            raise
        except Exception as err:
            self.logger.debug(
                "task '%s[daemon=%s]' exited with error: %s",
                name,
                daemon,
                err,
                exc_info=True,
            )
            self._errors.append(sys.exc_info())
            self.cancel()
        else:
            self.logger.debug(
                "task '%s[daemon=%s]' finished.",
                name,
                daemon,
            )
            if daemon:
                self.logger.debug(
                    "daemon task '%s' exited unexpectedly.  Cancelling service: %s",
                    name,
                    self,
                )
                self.cancel()
                raise DaemonTaskExit(f"Daemon task {name} exited")

    def run_task(self,
                 async_fn: Callable[..., Awaitable[Any]],
                 *args: Any,
                 daemon: bool = False,
                 name: str = None) -> None:

        task = asyncio.ensure_future(self._run_and_manage_task(
            async_fn,
            *args,
            daemon=daemon,
            name=name or repr(async_fn),
        ), loop=self._loop)
        self._service_tasks.add(task)

    def run_child_service(self,
                          service: ServiceAPI,
                          daemon: bool = False,
                          name: str = None) -> ManagerAPI:
        child_manager = type(self)(
            service,
            loop=self._loop,
        )
        self.run_task(
            child_manager.run,
            daemon=daemon,
            name=name or repr(service)
        )
        return child_manager


@asynccontextmanager
async def background_asyncio_service(service: ServiceAPI,
                                     loop: asyncio.AbstractEventLoop = None,
                                     ) -> AsyncIterator[ManagerAPI]:
    """
    This is the primary API for running a service without explicitely managing
    its lifecycle with a nursery.  The service is running within the context
    block and will be properly cleaned up upon exiting the context block.
    """
    manager = AsyncioManager(service, loop=loop)
    task = asyncio.ensure_future(manager.run(), loop=loop)

    try:
        await manager.wait_started()

        try:
            yield manager
        finally:
            await manager.stop()

        assert not manager.did_error, 'ARST ARST ARST'

    finally:
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        if manager.did_error:
            # TODO: better place for this.
            raise MultiError(tuple(
                exc_value.with_traceback(exc_tb)
                for _, exc_value, exc_tb
                in manager._errors
            ))
