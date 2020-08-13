from __future__ import annotations
import asyncio
import contextlib
import sys
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    cast,
    Iterable,
    List,
    Sequence,
    Set,
    TypeVar,
)

from trio import MultiError

from eth_utils.toolz import first

from p2p._utils import get_logger


TReturn = TypeVar('TReturn')


def create_task(coro: Awaitable[TReturn], name: str) -> asyncio.Task[TReturn]:
    if sys.version_info >= (3, 8):
        return asyncio.create_task(coro, name=name)
    else:
        return asyncio.create_task(coro)


async def wait_first(tasks: Sequence[asyncio.Task[Any]]) -> None:
    """
    Wait for the first of the given tasks to complete, then cancels all others.

    If the completed task raised an exception, re-raise it.

    If the task running us is cancelled, all tasks will be cancelled.
    """
    for task in tasks:
        if not isinstance(task, asyncio.Task):
            raise ValueError(f"{task} is not an asyncio.Task")

    logger = get_logger('p2p.asyncio_utils.wait_first')
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    except (KeyboardInterrupt, asyncio.CancelledError) as err:
        logger.debug("Got %r waiting for %s, cancelling them all", err, tasks)
        await cancel_tasks(tasks)
        raise
    except BaseException:
        logger.exception("Unexpected error waiting for %s", tasks)
        await cancel_tasks(tasks)
        raise
    else:
        logger.debug("Task %s finished, cancelling remaining ones: %s", done, pending)
        if pending:
            await cancel_tasks(cast(Set['asyncio.Task[Any]'], pending))
        if len(done) != 1:
            raise Exception(
                "Invariant: asyncio.wait() returned more than one task even "
                "though we used return_when=asyncio.FIRST_COMPLETED: %s", done)
        done_task = first(done)
        if done_task.exception():
            raise done_task.exception()


async def cancel_tasks(tasks: Iterable[asyncio.Task[Any]]) -> None:
    """
    Cancel and await for the given tasks, ignoring any asyncio.CancelledErrors.
    """
    for task in tasks:
        task.cancel()

    errors: List[BaseException] = []
    # Wait for all tasks in parallel so if any of them catches CancelledError and performs a
    # slow cleanup the othders don't have to wait for it. The timeout is long as our component
    # tasks can do a lot of stuff during their cleanup.
    done, pending = await asyncio.wait(tasks, timeout=10)
    if pending:
        errors.append(
            asyncio.TimeoutError("Tasks never returned after being cancelled: %s", pending))
    # We use future as the variable name here because that's what asyncio.wait returns above.
    for future in done:
        # future.exception() will raise a CancelledError if the future was cancelled by us above, so
        # we must suppress that here.
        with contextlib.suppress(asyncio.CancelledError):
            if future.exception():
                errors.append(future.exception())
    if errors:
        raise MultiError(errors)


# Ripped from async_service/asyncio.py at v0.1.0-alpha.8
@contextlib.asynccontextmanager
async def cleanup_tasks(*tasks: "asyncio.Future[Any]") -> AsyncIterator[None]:
    """
    Context manager that ensures that all tasks are properly cancelled and awaited.

    The order in which tasks are cleaned is such that the first task will be
    the last to be cancelled/awaited.

    This function **must** be called with at least one task.
    """
    try:
        task = tasks[0]
    except IndexError:
        raise TypeError("cleanup_tasks must be called with at least one task")

    try:
        if len(tasks) > 1:
            async with cleanup_tasks(*tasks[1:]):
                yield
        else:
            yield
    finally:
        if not task.done():
            task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass
