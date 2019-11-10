import functools
import sys
from typing import Any, Callable, Awaitable, AsyncIterator

from async_generator import asynccontextmanager

import trio

import trio_typing

from .abc import ManagerAPI, ServiceAPI
from .base import BaseManager
from .exceptions import DaemonTaskExit, LifecycleError


class TrioManager(BaseManager):
    # A nursery for system tasks.  This nursery is cancelled in the event that
    # the service is cancelled or exits.
    _system_nursery: trio_typing.Nursery

    # A nursery for sub tasks and services.  This nursery is cancelled if the
    # service is cancelled but allowed to exit normally if the service exits.
    _task_nursery: trio_typing.Nursery

    def __init__(self, service: ServiceAPI) -> None:
        if hasattr(service, 'manager'):
            raise LifecycleError("Service already has a manager.")
        else:
            service.manager = self

        self._service = service

        # events
        self._started = trio.Event()
        self._cancelled = trio.Event()
        self._stopped = trio.Event()

        # locks
        self._run_lock = trio.Lock()

        # errors
        self._errors = []

    #
    # System Tasks
    #
    async def _handle_cancelled(self,
                                task_nursery: trio_typing.Nursery,
                                ) -> None:
        """
        Handles the cancellation triggering cancellation of the task nursery.
        """
        self.logger.debug('%s: _handle_cancelled waiting for cancellation', self)
        await self.wait_cancelled()
        self.logger.debug('%s: _handle_cancelled triggering task nursery cancellation', self)
        task_nursery.cancel_scope.cancel()

    async def _handle_stopped(self,
                              system_nursery: trio_typing.Nursery) -> None:
        """
        Once the `_stopped` event is set this triggers cancellation of the system nursery.
        """
        self.logger.debug('%s: _handle_stopped waiting for stopped', self)
        await self.wait_stopped()
        self.logger.debug('%s: _handle_stopped triggering system nursery cancellation', self)
        system_nursery.cancel_scope.cancel()

    async def _handle_run(self) -> None:
        """
        Run and monitor the actual :meth:`ServiceAPI.run` method.

        In the event that it throws an exception the service will be cancelled.

        Upon a clean exit
        Triggers cancellation in the case where the service exits normally or
        throws an exception.
        """
        try:
            await self._service.run()
        except Exception as err:
            self.logger.debug(
                '%s: _handle_run got error, storing exception and setting cancelled',
                self
            )
            self._errors.append(sys.exc_info())
            self.cancel()
        else:
            # NOTE: Any service which uses daemon tasks will need to trigger
            # cancellation in order for the service to exit since this code
            # path does not trigger task cancellation.  It might make sense to
            # trigger cancellation if all of the running tasks are daemon
            # tasks.
            self.logger.debug(
                '%s: _handle_run exited cleanly, waiting for full stop...',
                self
            )

    @classmethod
    async def run_service(cls, service: ServiceAPI) -> None:
        manager = cls(service)
        await manager.run()

    async def run(self) -> None:
        if self._run_lock.locked():
            raise LifecycleError(
                "Cannot run a service with the run lock already engaged.  Already started?"
            )
        elif self.is_started:
            raise LifecycleError("Cannot run a service which is already started.")

        async with self._run_lock:
            async with trio.open_nursery() as system_nursery:
                try:
                    async with trio.open_nursery() as task_nursery:
                        self._task_nursery = task_nursery

                        system_nursery.start_soon(
                            self._handle_cancelled,
                            task_nursery,
                        )
                        system_nursery.start_soon(
                            self._handle_stopped,
                            system_nursery,
                        )

                        task_nursery.start_soon(self._handle_run)

                        self._started.set()

                        # ***BLOCKING HERE***
                        # The code flow will block here until the background tasks have
                        # completed or cancellation occurs.
                finally:
                    # Mark as having stopped
                    self._stopped.set()
        self.logger.debug('%s stopped', self)

        # If an error occured, re-raise it here
        if self.did_error:
            raise trio.MultiError(tuple(
                exc_value.with_traceback(exc_tb)
                for _, exc_value, exc_tb
                in self._errors
            ))

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
        await trio.sleep_forever()

    async def _run_and_manage_task(self,
                                   async_fn: Callable[..., Awaitable[Any]],
                                   *args: Any,
                                   daemon: bool,
                                   name: str) -> None:
        try:
            await async_fn(*args)
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

        self._task_nursery.start_soon(
            functools.partial(
                self._run_and_manage_task,
                daemon=daemon,
                name=name or repr(async_fn),
            ),
            async_fn,
            *args,
            name=name,
        )

    def run_child_service(self,
                          service: ServiceAPI,
                          daemon: bool = False,
                          name: str = None) -> ManagerAPI:
        child_manager = type(self)(service)
        self.run_task(
            child_manager.run,
            daemon=daemon,
            name=name or repr(service)
        )
        return child_manager


@asynccontextmanager
async def background_trio_service(service: ServiceAPI) -> AsyncIterator[ManagerAPI]:
    """
    This is the primary API for running a service without explicitely managing
    its lifecycle with a nursery.  The service is running within the context
    block and will be properly cleaned up upon exiting the context block.
    """
    async with trio.open_nursery() as nursery:
        manager = TrioManager(service)
        nursery.start_soon(manager.run)
        await manager.wait_started()
        try:
            yield manager
        finally:
            await manager.stop()
