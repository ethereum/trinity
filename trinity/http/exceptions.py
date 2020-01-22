from typing import Dict, Type


class APIServerError(Exception):
    ...


class InvalidRequestSyntaxError_400(APIServerError):
    ...


class NotFoundError_404(APIServerError):
    ...


class InternalError_500(APIServerError):
    ...


EXCEPTION_TO_STATUS: Dict[Type[Exception], int] = {
    InvalidRequestSyntaxError_400: 400,
    NotFoundError_404: 404,
    InternalError_500: 500,
}
