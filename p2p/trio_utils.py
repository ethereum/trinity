import functools
import operator
from typing import (
    Any,
    TypeVar,
    Awaitable,
    Callable,
    Tuple,
    cast,
)

import trio


TResult = TypeVar['TResult']
TArgs = TypeVar['TArgs']
TCoro = Callable[..., Awaitable[TResult]]
TChannelItem = Tuple[int, TResult]
TChannelPair = Tuple[
    trio.abc.SendChannel[TChannelItem],
    trio.abc.ReceiveChannel[TChannelItem],
]


async def run_and_send_result(coro: TCoro,
                              *args: Any,
                              index: int,
                              channel: trio.abc.SendChannel[TChannelItem],
                              ) -> None:
    """
    Run a coroutine and feed it's result and the provided index back through
    the provided `trio.abc.SendChannel`
    """
    result = await coro(*args)
    await channel.send((index, result))


async def gather(*coros_and_args: Tuple[Callable[..., Awaitable[TResult]], TArgs],
                 ) -> Tuple[TResult, ...]:
    (
        send_channel,
        receive_channel,
    ) = cast(TChannelPair, trio.open_memory_channel(len(coros_and_args)))

    # Run all of the coroutines concurrently.  As each completes it feeds the result into the
    # provided channel.  The results are then retrieved here, re-sorted by
    # their original index and returned.
    async with trio.open_nursery() as nursery:
        for index, (coro, args) in enumerate(coros_and_args):
            wrapper = functools.partial(run_and_send_result, index=index, channel=send_channel)
            nursery.start_soon(wrapper, coro, *args)

        results = tuple([
            (index, result)
            async for index, result
            in receive_channel
        ])
    await send_channel.aclose()
    sorted_result = tuple(
        result
        for index, result
        in sorted(results, key=operator.itemgetter(0))
    )
    return sorted_result
