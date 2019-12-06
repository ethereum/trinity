import functools
from typing import (
    Any,
    Awaitable,
    Callable,
    Type,
    TypeVar,
)


TAsyncFn = TypeVar("TAsyncFn", bound=Callable[..., Awaitable[None]])

# It's only permitted to ignore an exception and return None if
#   the wrapped function always returns None
TFn = TypeVar("TFn", bound=Callable[..., None])


def async_suppress_exceptions(
        *exception_types: Type[BaseException]) -> Callable[[TAsyncFn], TAsyncFn]:

    def _suppress_decorator(func: TAsyncFn) -> TAsyncFn:
        async def _suppressed_func(*args: Any, **kwargs: Any) -> None:
            try:
                await func(*args, **kwargs)
            except exception_types:
                # these exceptions are expected, and require no handling
                pass

        return _suppressed_func  # type: ignore

    return _suppress_decorator


def suppress_exceptions(*exception_types: Type[BaseException]) -> Callable[[TFn], TFn]:
    """
    Sometimes, rarely, it's okay for a function to just stop running,
    in the case of certain exceptions. Use this decorator, defining the
    exceptions that are allowed to simply exit.
    """
    def _suppress_decorator(func: TFn) -> TFn:

        @functools.wraps(func)
        def _suppressed_func(*args: Any, **kwargs: Any) -> None:
            try:
                func(*args, **kwargs)
            except exception_types:
                # these exceptions are expected, and require no handling
                pass

            return None

        return _suppressed_func  # type: ignore

    return _suppress_decorator
