from abc import ABC, abstractmethod
import logging
import sys
from types import TracebackType
from typing import Any, Callable, Awaitable, Optional, Tuple, Type

import trio

import trio_typing


class ServiceAPI(ABC):
    @property
    @abstractmethod
    def has_started(self) -> bool:
        ...

    @property
    @abstractmethod
    def is_running(self) -> bool:
        ...

    @property
    @abstractmethod
    def is_cancelled(self) -> bool:
        ...

    @property
    @abstractmethod
    def is_stopped(self) -> bool:
        ...

    @property
    @abstractmethod
    def did_error(self) -> bool:
        ...

    @abstractmethod
    def cancel(self) -> None:
        ...

    @abstractmethod
    async def wait_started(self) -> None:
        ...

    @abstractmethod
    async def wait_cancelled(self) -> None:
        ...

    @abstractmethod
    async def wait_stopped(self) -> None:
        ...


LogicFnType = Callable[[ServiceAPI], Awaitable[Any]]


class ServiceLogic(ABC):
    @abstractmethod
    async def run(self, service: ServiceAPI) -> None:
        ...


class Service(ServiceAPI):
    logger = logging.getLogger('p2p.trio_service.Service')

    logic_class: ServiceLogic

    _run_error: Optional[Tuple[
        Optional[Type[BaseException]],
        Optional[BaseException],
        Optional[TracebackType],
    ]] = None

    def __init__(self, *args, **kwargs) -> None:
        # initialize the service logic
        self.logic = self.logic_class(*args, **kwargs)

        # events
        self._started = trio.Event()
        self._cancelled = trio.Event()
        self._stopped = trio.Event()

        # locks
        self._run_lock = trio.Lock()

    async def _handle_cancelled(self, nursery: trio_typing.Nursery):
        """
        Handles the case where cancellation occurs because the
        `event.set_cancelled()` has been called, this propagates that to force
        the nursery to be cancelled.
        """
        self.logger.debug('_handle_cancelled waiting for cancellation')
        await self._cancelled.wait()
        self.logger.debug('_handle_cancelled triggering nursery cancellation')
        nursery.cancel_scope.cancel()

    async def _handle_run(self) -> None:
        """
        Triggers cancellation in the case where the service exits normally or
        throws an exception.
        """
        self._started.set()
        try:
            await self.logic.run(self)
        except Exception as err:
            self.logger.debug('_handle_run got error, storing exception and setting cancelled')
            self._run_error = sys.exc_info()
        finally:
            self.logger.debug('_handle_run triggering service cancellation')
            self.cancel()

    async def run(self) -> None:
        if self._run_lock.locked():
            raise Exception("TODO: run lock already engaged")
        elif self.has_started:
            raise Exception("TODO: already started. No reentrance")

        async with self._run_lock:

            # Open a nursery
            async with trio.open_nursery() as nursery:
                nursery.start_soon(self._handle_cancelled, nursery)
                nursery.start_soon(self._handle_run)

                # This wait is not strictly necessary as this context block
                # will not exit until the background tasks have completed.
                await self.wait_cancelled()

        # Mark as having stopped
        self._stopped.set()
        self.logger.debug('Service stopped')

        # If an error occured, re-raise it here
        if self.did_error:
            _, exc_value, exc_tb = self._run_error
            raise exc_value.with_traceback(exc_tb)

    #
    # Event API mirror
    #
    @property
    def has_started(self) -> bool:
        return self._started.is_set()

    @property
    def is_running(self) -> bool:
        return self._started.is_set() and not self._stopped.is_set()

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled.is_set()

    @property
    def is_stopped(self) -> bool:
        return self._stopped.is_set()

    @property
    def did_error(self) -> bool:
        return self._run_error is not None

    #
    # Control API
    #
    def cancel(self) -> None:
        if not self.has_started:
            raise Exception("TODO: never started")
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

    #
    # Service from simple function API
    #
    @classmethod
    def from_function(cls, logic_fn: LogicFnType) -> 'Service':
        logic_class = type(
            f'ServiceLogic[{logic_fn}]',
            (ServiceLogic,),
            {'run': staticmethod(logic_fn)},
        )
        return type(
            f'Service[{logic_fn}]',
            (cls,),
            {'logic_class': logic_class},
        )
