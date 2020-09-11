from __future__ import annotations
import asyncio
import contextlib
import sys
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    List,
    Sequence,
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


async def wait_first(
        tasks: Sequence[asyncio.Task[Any]], max_wait_after_cancellation: float) -> None:
    """
    Wait for the first of the given tasks to complete, then cancels all others.

    If the completed task raised an exception, that is re-raised.

    If the task running us is cancelled, all tasks will be cancelled, in no specific order.

    If we get an exception from any of the cancelled tasks, they are re-raised as a
    trio.MultiError, which will include the exception from the completed task (if any) in their
    context.

    If the cancelled tasks don't return in max_wait_after_cancellation seconds, a warning
    is logged.
    """
    for task in tasks:
        if not isinstance(task, asyncio.Task):
            raise ValueError(f"{task} is not an asyncio.Task")

    logger = get_logger('p2p.asyncio_utils.wait_first')
    async with cancel_pending_tasks(*tasks, timeout=max_wait_after_cancellation):
        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        except (KeyboardInterrupt, asyncio.CancelledError) as err:
            logger.debug("Got %r waiting for %s, cancelling them all", err, tasks)
            raise
        except BaseException:
            logger.exception("Unexpected error waiting for %s, cancelling them all", tasks)
            raise
        else:
            logger.debug("Task %s finished, cancelling pending ones: %s", done, pending)
            if len(done) != 1:
                raise Exception(
                    "Invariant: asyncio.wait() returned more than one task even "
                    "though we used return_when=asyncio.FIRST_COMPLETED: %s", done)
            done_task = first(done)
            if done_task.exception():
                raise done_task.exception()


@contextlib.asynccontextmanager
async def cancel_pending_tasks(*tasks: asyncio.Task[Any], timeout: int) -> AsyncIterator[None]:
    """
    Cancel and await for all of the given tasks that are still pending, in no specific order.

    If any cancelled tasks have not completed after the given timeout, log a warning.

    Ignores any asyncio.CancelledErrors.
    """
    try:
        yield
    finally:
        logger = get_logger('p2p.asyncio_utils.cancel_pending_tasks')
        cancelled: List[asyncio.Task[Any]] = []
        for task in tasks:
            if not task.done():
                task.cancel()
                cancelled.append(task)

        # It'd save us one indentation level on the block of code below if we had an early return
        # in case there are no cancelled tasks, but it turns out an early return inside a finally:
        # block silently cancels an active exception being raised, so we use an if/else to avoid
        # having to check if there is an active exception and re-raising it.
        if cancelled:
            logger.debug("Cancelled tasks %s, now waiting for them to return", task)
            errors: List[BaseException] = []
            # Wait for all tasks in parallel so if any of them catches CancelledError and performs a
            # slow cleanup the othders don't have to wait for it.
            done, pending = await asyncio.wait(cancelled, timeout=timeout)
            if pending:
                # These tasks might need to be pickled to pass across the process boundary
                unfinished = [repr(task) for task in pending]
                logger.warning(
                    "Timed out waiting for tasks to return after cancellation: %s", unfinished)
            # We use future as the variable name here because that's what asyncio.wait returns
            # above.
            for future in done:
                # future.exception() will raise a CancelledError if the future was cancelled by us
                # above, so we must suppress that here.
                with contextlib.suppress(asyncio.CancelledError):
                    if future.exception():
                        errors.append(future.exception())
            if errors:
                raise MultiError(errors)
        else:
            logger.debug("No pending tasks in %s, returning", task)


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
