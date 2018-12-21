import itertools

import pytest

from eth_utils.toolz import (
    cons,
    take,
)

from trinity._utils.async_iter import (
    async_chain,
    async_cons,
    async_iterator,
    async_sliding_window,
    async_take,
)


@pytest.mark.asyncio
async def test_asyncify_iterable():
    original = [1, 2, 3]
    async_iterable = async_iterator(original)
    result = [i async for i in async_iterable]
    assert result == original


@pytest.mark.parametrize("iterables", [
    (),
    ([],),
    ([1],),
    ([1, 2], [3, 4]),
    ([1, 2], [], [3, 4, 5]),
])
@pytest.mark.asyncio
async def test_async_chain(iterables):
    chained = async_chain(*[async_iterator(iterable) for iterable in iterables])
    result = [i async for i in chained]
    expected = list(itertools.chain(*iterables))
    assert result == expected


@pytest.mark.parametrize("num, iterable", [
    (0, []),
    (0, [1, 2]),
    (1, [1]),
    (1, [1, 2]),
    (2, [1, 2, 3]),
    (3, [1]),
])
@pytest.mark.asyncio
async def test_async_take(num, iterable):
    taken = async_take(num, async_iterator(iterable))
    result = [i async for i in taken]
    expected = list(take(num, iterable))
    assert result == expected


@pytest.mark.parametrize("item, iterable", [
    (1, [1, 2, 3]),
    (1, [])
])
@pytest.mark.asyncio
async def test_async_cons(item, iterable):
    consed = async_cons(item, async_iterator(iterable))
    result = [i async for i in consed]
    expected = list(cons(item, iterable))
    assert result == expected


@pytest.mark.parametrize("window_size, iterable, expected", [
    (0, [], []),
    (2, [], []),
    (1, [1, 2, 3], [(1,), (2,), (3,)]),
    (2, [1, 2, 3], [(1, 2), (2, 3)]),
    (5, [1, 2, 3], []),
])
@pytest.mark.asyncio
async def test_async_sliding_window(window_size, iterable, expected):
    windowed = async_sliding_window(window_size, async_iterator(iterable))
    result = [i async for i in windowed]
    assert result == expected


@pytest.mark.asyncio
async def test_negative_sliding_window_size():
    with pytest.raises(ValueError):
        async_sliding_window(-1, async_iterator([1, 2, 3]))
