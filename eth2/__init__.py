from functools import wraps
from typing import Any, Callable

# This is to ensure we call setup_extended_logging() before anything else.
import eth as _eth_module  # noqa: F401


def impure(f: Callable[..., Any]) -> Any:
    """
    Just a mark now.
    We can change all marked back to pure functions by
    adding ``state = copy.deepcopy(state)`` in this decorator.
    """

    @wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return f(*args, **kwargs)

    return wrapper
