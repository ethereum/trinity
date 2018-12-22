import collections
import itertools
from typing import (
    AsyncIterable,
    AsyncIterator,
    Iterable,
    Set,
    Tuple,
    TypeVar,
)


T = TypeVar('T')


async def contains_all(async_gen: AsyncIterable[str], keywords: Set[str]) -> bool:
    """
    Check wether an ``AsyncIterable[str]`` contains all of the given keywords. The keywords
    can be embedded in some larger string. Return ``True`` as soon as all keywords were matched.
    If not all keywords were matched by the time that the async iterable is done, return ``False``.
    """
    seen_keywords: Set[str] = set()
    async for line in async_gen:
        for check in keywords - seen_keywords:
            if check in line:
                seen_keywords.add(check)
        if seen_keywords == keywords:
            return True

    return False


async def async_iterator(iterable: Iterable[T]) -> AsyncIterator[T]:
    for item in iterable:
        yield item


async def async_chain(*async_iterables: AsyncIterable[T]) -> AsyncIterator[T]:
    for async_iterable in async_iterables:
        async for item in async_iterable:
            yield item


def async_cons(first_item: T,
               async_iterable: AsyncIterable[T]) -> AsyncIterator[T]:
    return async_chain(
        async_iterator([first_item]),
        async_iterable,
    )


def async_take(num_items: int,
               async_iterable: AsyncIterable[T]) -> AsyncIterator[T]:
    if num_items < 0:
        raise ValueError("Number of elements to take must be non-negative")

    async def async_generator() -> AsyncIterator[T]:
        if num_items == 0:
            return

        counter = itertools.count(1)
        async for item in async_iterable:
            yield item

            if next(counter) >= num_items:
                break

    return async_generator()


def async_sliding_window(window_size: int,
                         async_iterable: AsyncIterable[T]) -> AsyncIterator[Tuple[T, ...]]:
    if window_size < 0:
        raise ValueError("size of sliding window must be non-negative")

    async def async_generator() -> AsyncIterator[Tuple[T, ...]]:
        if window_size == 0:
            return

        d = collections.deque(
            [item async for item in async_take(window_size, async_iterable)],
            maxlen=window_size,
        )

        if len(d) < window_size:
            return

        yield tuple(d)
        async for item in async_iterable:
            d.append(item)
            yield tuple(d)

    return async_generator()
