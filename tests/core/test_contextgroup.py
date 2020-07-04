import asyncio
import contextlib

import pytest

from trio import MultiError

from trinity.contextgroup import AsyncContextGroup


@pytest.mark.asyncio
async def test_basic():
    exit_count = 0

    @contextlib.asynccontextmanager
    async def ctx(v):
        nonlocal exit_count
        await asyncio.sleep(0)
        yield v
        await asyncio.sleep(0)
        exit_count += 1

    group = AsyncContextGroup([ctx(i) for i in range(3)])
    async with group as yielded_values:
        assert yielded_values == tuple(range(3))

    assert exit_count == 3


@pytest.mark.asyncio
async def test_exception_entering_context():
    exit_count = 0

    @contextlib.asynccontextmanager
    async def ctx(should_raise=False):
        nonlocal exit_count
        await asyncio.sleep(0)
        if should_raise:
            raise ValueError()
        try:
            yield
        finally:
            await asyncio.sleep(0)
            exit_count += 1

    group = AsyncContextGroup([ctx(), ctx(True), ctx()])
    with pytest.raises(ValueError):
        async with group:
            # the body of the with block should never execute if an exception is raised when
            # entering the context group.
            assert False  # noqa: B011

    # One of our contexts was not entered so we didn't exit it either.
    assert exit_count == 2


@pytest.mark.asyncio
async def test_exception_inside_context_block():
    exit_count = 0

    async def f(should_raise):
        await asyncio.sleep(0)
        if should_raise:
            raise ValueError()

    @contextlib.asynccontextmanager
    async def ctx(should_raise=False):
        nonlocal exit_count
        await asyncio.sleep(0)
        try:
            yield f(should_raise)
        finally:
            await asyncio.sleep(0)
            exit_count += 1

    group = AsyncContextGroup([ctx(), ctx(True), ctx()])
    with pytest.raises(ValueError):
        async with group as awaitables:
            empty1, exception, empty2 = await asyncio.gather(*awaitables, return_exceptions=True)
            assert empty1 is None
            assert empty2 is None
            raise exception

    assert exit_count == 3


@pytest.mark.asyncio
async def test_exception_exiting():
    exit_count = 0

    @contextlib.asynccontextmanager
    async def ctx(should_raise=False):
        nonlocal exit_count
        await asyncio.sleep(0)
        try:
            yield
        finally:
            exit_count += 1
            if should_raise:
                raise ValueError()

    group = AsyncContextGroup([ctx(), ctx(True), ctx(True)])
    with pytest.raises(MultiError) as exc_info:
        async with group:
            pass

    exc = exc_info.value
    assert len(exc.exceptions) == 2
    assert isinstance(exc.exceptions[0], ValueError)
    assert isinstance(exc.exceptions[1], ValueError)
    assert exit_count == 3
