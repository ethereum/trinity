import contextlib
import cProfile
import functools
from typing import (
    Any,
    Callable,
    Iterator,
)


@contextlib.contextmanager
def profiler(filename: str) -> Iterator[None]:
    pr = cProfile.Profile()
    pr.enable()
    try:
        yield
    finally:
        pr.disable()
        pr.dump_stats(filename)


def setup_cprofiler(filename: str) -> Callable[..., Any]:
    def outer(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def inner(*args: Any, **kwargs: Any) -> None:
            should_profile = kwargs.pop('profile', False)
            if should_profile:
                with profiler(filename):
                    # type ignored to fix https://github.com/ethereum/trinity/issues/1520
                    return fn(*args, **kwargs)  # type: ignore
            else:
                # type ignored to fix https://github.com/ethereum/trinity/issues/1520
                return fn(*args, **kwargs)  # type: ignore
        return inner
    return outer
