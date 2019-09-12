from abc import (
    ABC,
    abstractmethod,
)
from typing import Any

from aiohttp import web


def response_error(message: Any) -> web.Response:
    data = {'error': message}
    return web.json_response(data)


class BaseHTTPHandler(ABC):

    @staticmethod
    @abstractmethod
    def handle(*arg: Any) -> web.Response:
        ...
