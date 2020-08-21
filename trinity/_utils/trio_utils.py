from typing import (
    Any,
    Awaitable,
    Callable,
    Sequence,
)

import trio


async def wait_first(callables: Sequence[Callable[[], Awaitable[Any]]]) -> None:
    """
    Run any number of tasks but cancel out any outstanding tasks as soon as the first one finishes.
    """
    async with trio.open_nursery() as nursery:
        for task in callables:
            async def _run_then_cancel() -> None:
                await task()
                nursery.cancel_scope.cancel()
            nursery.start_soon(_run_then_cancel)
