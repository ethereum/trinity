from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable

import trio_typing


class ServiceAPI(ABC):
    manager: 'ManagerAPI'

    @abstractmethod
    async def run(self) -> None:
        """
        This method is where all of the Service class logic should be
        implemented.  It should **not** be invoked by user code but instead run
        with either:

        .. code-block: python

            # 1. run the service in the background using a context manager
            async with run_service(service) as manager:
                # service runs inside context block
                ...
                # service cancels and stops when context exits
            # service will have fully stopped

            # 2. run the service blocking until completion
            await Manager.run_service(service)

            # 3. create manager and then run service blocking until completion
            manager = Manager(service)
            await manager.run()
        """
        ...


class ManagerAPI(ABC):
    @property
    @abstractmethod
    def is_started(self) -> bool:
        """
        Return boolean indicating if the underlying service has been started.
        """
        ...

    @property
    @abstractmethod
    def is_running(self) -> bool:
        """
        Return boolean indicating if the underlying service is actively
        running.  A service is considered running if it has been started and
        has not yet been stopped.
        """
        ...

    @property
    @abstractmethod
    def is_cancelled(self) -> bool:
        """
        Return boolean indicating if the underlying service has been cancelled.
        This can occure externally via the `cancel()` method or internally due
        to a task crash or a crash of the actual :meth:`ServiceAPI.run` method.
        """
        ...

    @property
    @abstractmethod
    def is_stopped(self) -> bool:
        """
        Return boolean indicating if the underlying service is stopped.  A
        stopped service will have completed all of the background tasks.
        """
        ...

    @property
    @abstractmethod
    def did_error(self) -> bool:
        """
        Return boolean indicating if the underlying service threw an exception.
        """
        ...

    @abstractmethod
    def cancel(self) -> None:
        """
        Trigger cancellation of the service.
        """
        ...

    @abstractmethod
    async def stop(self) -> None:
        """
        Trigger cancellation of the service and wait for it to stop.
        """
        ...

    @abstractmethod
    async def wait_started(self) -> None:
        """
        Wait until the service is started.
        """
        ...

    @abstractmethod
    async def wait_cancelled(self) -> None:
        """
        Wait until the service is cancelled.
        """
        ...

    @abstractmethod
    async def wait_stopped(self) -> None:
        """
        Wait until the service is stopped.
        """
        ...

    @abstractmethod
    async def wait_forever(self) -> None:
        """
        Blocks indefinitly.

        Typically used at the end of a `Service.run()` method to wait until the
        service is cancelled by an external mechanism.
        """
        ...

    @classmethod
    @abstractmethod
    async def run_service(cls, service: ServiceAPI) -> None:
        """
        Run a service
        """
        ...

    @abstractmethod
    async def run(self) -> None:
        """
        Run a service
        """
        ...

    @trio_typing.takes_callable_and_args
    @abstractmethod
    async def run_task(self,
                       async_fn: Callable[..., Awaitable[Any]],
                       *args: Any,
                       daemon: bool = False,
                       name: str = None) -> None:
        """
        Run a task in the background.  If the function throws an exception it
        will trigger the service to be cancelled and be propogated.

        If `daemon == True` then the the task is expected to run indefinitely
        and will trigger cancellation if the task finishes.
        """
        ...

    @trio_typing.takes_callable_and_args
    @abstractmethod
    async def run_daemon_task(self,
                              async_fn: Callable[..., Awaitable[Any]],
                              *args: Any,
                              name: str = None) -> None:
        """
        Run a daemon task in the background.

        Equivalent to `run_task(..., daemon=True)`.
        """
        ...

    @abstractmethod
    def run_child_service(self,
                          service: ServiceAPI,
                          daemon: bool = False,
                          name: str = None) -> "ManagerAPI":
        """
        Run a service in the background.  If the function throws an exception it
        will trigger the parent service to be cancelled and be propogated.

        If `daemon == True` then the the service is expected to run indefinitely
        and will trigger cancellation if the service finishes.
        """
        ...

    @abstractmethod
    def run_daemon_child_service(self,
                                 service: ServiceAPI,
                                 name: str = None) -> "ManagerAPI":
        """
        Run a daemon service in the background.

        Equivalent to `run_child_service(..., daemon=True)`.
        """
        ...
