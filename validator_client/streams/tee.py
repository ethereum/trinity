from typing import Any, AsyncIterable

import trio


class Stream:
    def __init__(self) -> None:
        self._send_channel, self._recv_channel = trio.open_memory_channel(0)

    async def accept(self, event: Any) -> None:
        await self._send_channel.send(event)

    def __aiter__(self) -> AsyncIterable:
        return self

    async def __anext__(self) -> Any:
        event = await self._recv_channel.receive()
        return event


class Tee:
    """
    ``Tee`` copies all events on a producing ``stream`` to any consumers
    who taken a clone of the ``stream`` via ``clone``.
    """

    def __init__(self, stream: AsyncIterable) -> None:
        self._stream = stream
        self._clones = ()
        self._clones_lock = trio.Lock()

    async def clone(self) -> Stream:
        new_clone = Stream()
        async with self._clones_lock:
            self._clones += (new_clone,)
        return new_clone

    async def run(self) -> None:
        async for event in self._stream:
            async with self._clones_lock:
                for clone in self._clones:
                    await clone.accept(event)
