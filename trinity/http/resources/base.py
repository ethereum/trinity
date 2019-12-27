from abc import (
    ABC,
)
from typing import Any, Callable, TypeVar

from aiohttp import web
from lahja.base import EndpointAPI

from eth2.beacon.chains.base import (
    BaseBeaconChain,
)
from trinity.http.exceptions import APIServerError


TBaseResource = TypeVar("TBaseResource", bound="BaseResource")


class BaseResource(ABC):

    timeout = 2

    def __init__(self, chain: BaseBeaconChain, event_bus: EndpointAPI):
        self.chain = chain
        self.event_bus = event_bus

    @property
    def name(self) -> str:
        # By default the name is the lower-case class name.
        # This encourages a standard name of the module, but can
        # be overridden if necessary.
        return self.__class__.__name__.lower()


def get_method(
    func: Callable[[TBaseResource, web.Request], Any]
) -> Callable[[TBaseResource, web.Request], Any]:
    async def wrapper(self: TBaseResource, request: web.Request) -> Any:
        if request.method != "GET":
            raise APIServerError(f"Wrong HTTP method, should be GET, got {request.method}")
        return await func(self, request)

    return wrapper


def post_method(
    func: Callable[[TBaseResource, web.Request], Any]
) -> Callable[[TBaseResource, web.Request], Any]:
    async def wrapper(self: TBaseResource, request: web.Request) -> Any:
        if request.method != "POST":
            raise APIServerError(f"Wrong HTTP method, should be GET, got {request.method}")
        return await func(self, request)

    return wrapper
