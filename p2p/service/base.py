import functools
import logging

from .abc import ManagerAPI, ServiceAPI
from types import TracebackType
from typing import cast, Any, Awaitable, Callable, List, Optional, Type, TypeVar, Tuple


class Service(ServiceAPI):
    pass


LogicFnType = Callable[..., Awaitable[Any]]


def as_service(service_fn: LogicFnType) -> Type[ServiceAPI]:
    """
    Create a service out of a simple function
    """
    class _Service(Service):
        def __init__(self, *args: Any, **kwargs: Any):
            self._args = args
            self._kwargs = kwargs

        async def run(self) -> None:
            await service_fn(self.manager, *self._args, **self._kwargs)

    _Service.__name__ = service_fn.__name__
    _Service.__doc__ = service_fn.__doc__
    return _Service


TReturn = TypeVar("TReturn")
TFunc = TypeVar("TFunc", bound=Callable[..., TReturn])


def external_api(func: TFunc) -> TFunc:
    @functools.wraps(func)
    def inner(self: ServiceAPI, *args: Any) -> TReturn:
        ...
    return cast(TFunc, inner)


class BaseManager(ManagerAPI):
    logger = logging.getLogger('p2p.service.Manager')

    _service: ServiceAPI

    _errors: List[Tuple[
        Optional[Type[BaseException]],
        Optional[BaseException],
        Optional[TracebackType],
    ]]

    #
    # Event API mirror
    #
    @property
    def is_running(self) -> bool:
        return self.is_started and not self.is_stopped

    @property
    def did_error(self) -> bool:
        return len(self._errors) > 0

    #
    # Control API
    #
    async def stop(self) -> None:
        self.cancel()
        await self.wait_stopped()

    #
    # Wait API
    #
    def run_daemon_task(self,
                        async_fn: Callable[..., Awaitable[Any]],
                        *args: Any,
                        name: str = None) -> None:

        self.run_task(async_fn, *args, daemon=True, name=name)

    def run_daemon_child_service(self,
                                 service: ServiceAPI,
                                 name: str = None) -> ManagerAPI:
        return self.run_child_service(service, daemon=True, name=name)
