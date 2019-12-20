import asyncio
from typing import AsyncIterator, Type

from async_generator import asynccontextmanager
from lahja import BaseEvent, EndpointAPI


@asynccontextmanager
async def mock_request_response(request_type: Type[BaseEvent],
                                response: BaseEvent,
                                event_bus: EndpointAPI) -> AsyncIterator[None]:
    ready = asyncio.Event()
    task = asyncio.ensure_future(_do_mock_response(request_type, response, event_bus, ready))
    await ready.wait()
    try:
        yield
    finally:
        if not task.done():
            task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass


async def _do_mock_response(request_type: Type[BaseEvent],
                            response: BaseEvent,
                            event_bus: EndpointAPI,
                            ready: asyncio.Event) -> None:
    ready.set()
    async for req in event_bus.stream(request_type):
        await event_bus.broadcast(response, req.broadcast_config())
        break
