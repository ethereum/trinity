import collections
from typing import (
    AsyncIterable,
    AsyncIterator,
    Iterable,
    Set,
    Tuple,
    TypeVar,
    Union,
)


T = TypeVar('T')

AnyIterable = Union[Iterable[T], AsyncIterable[T]]


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


async def async_iter_wrapper(iterable: AnyIterable[T]) -> AsyncIterator[T]:
    if isinstance(iterable, Iterable):
        for item in iterable:
            yield item
    elif isinstance(iterable, AsyncIterable):
        async for item in iterable:
            yield item


async def async_chain(*iterables: AnyIterable[T]) -> AsyncIterator[T]:
    for iterable in iterables:
        async for item in async_iter_wrapper(iterable):
            yield item


def async_cons(first_item: T,
               iterable: AnyIterable[T]) -> AsyncIterator[T]:
    return async_chain([first_item], iterable)


def async_take(n: int,
               iterable: AnyIterable[T]) -> AsyncIterator[T]:
    if n < 0:
        raise ValueError("Number of elements to take must be non-negative")

    async def async_generator() -> AsyncIterator[T]:
        if n == 0:
            return

        yielded_items = 0
        async for item in async_iter_wrapper(iterable):
            yield item

            yielded_items += 1
            if yielded_items >= n:
                break

    return async_generator()


def async_sliding_window(window_size: int,
                         iterable: AnyIterable[T]) -> AsyncIterator[Tuple[T, ...]]:
    if window_size < 0:
        raise ValueError("size of sliding window must be non-negative")

    async def async_generator() -> AsyncIterator[Tuple[T, ...]]:
        if window_size == 0:
            return

        async_iterable = async_iter_wrapper(iterable)

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
