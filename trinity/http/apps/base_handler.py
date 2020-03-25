import functools
import inspect
from typing import Iterable

from aiohttp import web
from eth_utils import to_tuple

from eth2.validator_client.beacon_node import BeaconNodePath as APIEndpoint

WEB_APP_HANDLER_KEY = "__web_app_handler_data"


# NOTE: ignoring the typing until this stabilizes a bit
def _mk_method_decorator(action):  # type: ignore
    def _method_func(endpoint: APIEndpoint):  # type: ignore
        def _decorator(method):  # type: ignore
            @functools.wraps(method)
            def _inner(*args, **kwargs):  # type: ignore
                return method(*args, **kwargs)

            setattr(
                _inner,
                WEB_APP_HANDLER_KEY,
                {"_action": action, "_path": endpoint.value},
            )
            return _inner

        return _decorator

    return _method_func


# NOTE: ignoring the typing until this stabilizes a bit
get = _mk_method_decorator(web.get)  # type: ignore
post = _mk_method_decorator(web.post)  # type: ignore


def _web_app_handler_predicate(obj: object) -> bool:
    return inspect.ismethod(obj) and hasattr(obj, WEB_APP_HANDLER_KEY)


class BaseHandler:
    @to_tuple
    def make_routes(self) -> Iterable[web.RouteDef]:
        for (_, handler) in inspect.getmembers(
            self, predicate=_web_app_handler_predicate
        ):
            data = getattr(handler, WEB_APP_HANDLER_KEY)
            yield data["_action"](data["_path"], handler)
