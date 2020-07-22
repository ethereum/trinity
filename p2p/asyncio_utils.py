from __future__ import annotations
import asyncio
import contextlib
import sys
from typing import Awaitable, Sequence, Iterable, List, TypeVar

from trio import MultiError

from eth_utils.toolz import first


TReturn = TypeVar('TReturn')


def create_task(coro: Awaitable[TReturn], name: str) -> asyncio.Task[TReturn]:
    if sys.version_info >= (3, 8):
        # Our version of mypy doesn't know about the name argument to create_task()
        return asyncio.create_task(coro, name=name)  # type: ignore
    else:
        return asyncio.create_task(coro)


async def wait_first(futures: Sequence[asyncio.Future[None]]) -> None:
    """
    Wait for the first of the given futures to complete, then cancels all others.

    If the completed future raised an exception, re-raise it.

    If the task running us is cancelled, all futures will be cancelled.
    """
    for future in futures:
        if not isinstance(future, asyncio.Future):
            raise ValueError(f"{future} is not an asyncio.Future")

    try:
        done, pending = await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError:
        await cancel_futures(futures)
        raise
    else:
        if pending:
            await cancel_futures(pending)
        if len(done) != 1:
            raise Exception(
                "Invariant: asyncio.wait() returned more than one future even "
                "though we used return_when=asyncio.FIRST_COMPLETED: %s", done)
        done_future = first(done)
        if done_future.exception():
            raise done_future.exception()


async def cancel_futures(futures: Iterable[asyncio.Future[None]]) -> None:
    """
    Cancel and await for the given futures, ignoring any asyncio.CancelledErrors.
    """
    for fut in futures:
        fut.cancel()

    errors: List[BaseException] = []
    # Wait for all futures in parallel so if any of them catches CancelledError and performs a
    # slow cleanup the othders don't have to wait for it. The timeout is long as our component
    # tasks can do a lot of stuff during their cleanup.
    done, pending = await asyncio.wait(futures, timeout=5)
    if pending:
        errors.append(
            asyncio.TimeoutError("Tasks never returned after being cancelled: %s", pending))
    for task in done:
        # task.exception() will raise a CancelledError if the task was cancelled by us above, so
        # we must suppress that here.
        with contextlib.suppress(asyncio.CancelledError):
            if task.exception():
                errors.append(task.exception())
    if errors:
        raise MultiError(errors)
