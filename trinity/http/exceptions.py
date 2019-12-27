class APIServerError(Exception):
    ...


class InvalidRequestSyntaxError_400(APIServerError):
    ...


class NotFoundError_404(APIServerError):
    ...


class InternalError_500(APIServerError):
    ...
