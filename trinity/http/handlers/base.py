from abc import (
    ABC,
    abstractmethod,
)
from typing import Any, Optional

from aiohttp import web

from trinity.http.exceptions import (
    InternalError_500,
    InvalidRequestSyntaxError_400,
    NotFoundError_404,
)

EXCEPTION_TO_STATUS = {
    InvalidRequestSyntaxError_400: 400,
    NotFoundError_404: 404,
    InternalError_500: 500,
}


def response_error(message: Any, exception: Optional[Exception] = None) -> web.Response:
    data = {'error': message}
    if exception is not None:
        status = EXCEPTION_TO_STATUS[exception.__class__]
        return web.json_response(data, status=status, reason=str(exception))
    else:
        return web.json_response(data)


class BaseHTTPHandler(ABC):

    @staticmethod
    @abstractmethod
    def handle(*arg: Any) -> web.Response:
        ...
