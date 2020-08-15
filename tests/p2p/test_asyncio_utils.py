import asyncio

import pytest

from p2p.asyncio_utils import create_task, wait_first


class TaskException(Exception):
    pass


class AfterCancellationException(Exception):
    pass


def create_sleep0_task():
    return create_task(asyncio.sleep(0), 'sleep-0')


def create_raising_task():
    async def _raise():
        raise TaskException()

    return create_task(_raise(), 'raising-task')


def create_sleep_forever_task():
    async def _sleep_forever():
        await asyncio.Future()

    return create_task(_sleep_forever(), 'sleep-forever')


def create_raising_after_cancellation_task():
    async def _raise_after_cancellation():
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            raise AfterCancellationException()

    return create_task(_raise_after_cancellation(), 'raising-after-cancellation')


def create_rogue_task(sleep_after_cancellation):
    async def _rogue(sleep_after_cancellation):
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            await asyncio.sleep(sleep_after_cancellation)

    return create_task(_rogue(sleep_after_cancellation), 'rogue-task')


@pytest.mark.asyncio
async def test_wait_first():
    sleep0_task = create_sleep0_task()
    sleep_forever_task = create_sleep_forever_task()
    await wait_first([sleep0_task, sleep_forever_task], max_wait_after_cancellation=0.1)

    assert sleep0_task.done()
    assert not sleep0_task.cancelled()
    assert sleep_forever_task.done()
    assert sleep_forever_task.cancelled()


@pytest.mark.asyncio
async def test_wait_first_task_exception():
    raising_task = create_raising_task()
    with pytest.raises(TaskException):
        await wait_first([raising_task], max_wait_after_cancellation=0.1)

    raising_task = create_raising_task()
    sleep_forever_task = create_sleep_forever_task()
    with pytest.raises(TaskException):
        await wait_first([raising_task, sleep_forever_task], max_wait_after_cancellation=0.1)


@pytest.mark.asyncio
async def test_wait_first_cancelled_task_exception():
    raising_task = create_raising_task()
    raising_after_cancellation_task = create_raising_after_cancellation_task()
    with pytest.raises(AfterCancellationException) as exc:
        await wait_first(
            [raising_task, raising_after_cancellation_task],
            max_wait_after_cancellation=0.1
        )
    # Even though we get the exception from the cancelled task, it will have the exception from
    # the task that completed as its context.
    assert isinstance(exc.value.__context__, TaskException)


@pytest.mark.asyncio
async def test_wait_first_rogue_task():
    sleep_after_cancellation = 0.2
    sleep0_task = create_sleep0_task()
    rogue_task = create_rogue_task(sleep_after_cancellation)
    sleep_forever_task = create_sleep_forever_task()

    # If a task doesn't return after being cancelled, we get a TimeoutError
    with pytest.raises(asyncio.TimeoutError):
        await wait_first(
            [sleep0_task, rogue_task, sleep_forever_task],
            max_wait_after_cancellation=sleep_after_cancellation / 2,
        )

    # Await for our rogue task to finish otherwise we'll get asyncio warnings.
    await rogue_task
