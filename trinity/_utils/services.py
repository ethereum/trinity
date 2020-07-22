import asyncio
import contextlib
from typing import AsyncContextManager, Callable, Sequence

from async_service import (
    background_asyncio_service,
    background_trio_service,
    ManagerAPI,
    ServiceAPI,
)

from p2p.asyncio_utils import wait_first


async def run_background_asyncio_services(services: Sequence[ServiceAPI]) -> None:
    await _run_background_services(services, background_asyncio_service)


async def run_background_trio_services(services: Sequence[ServiceAPI]) -> None:
    await _run_background_services(services, background_trio_service)


async def _run_background_services(
        services: Sequence[ServiceAPI],
        runner: Callable[[ServiceAPI], AsyncContextManager[ManagerAPI]]
) -> None:
    async with contextlib.AsyncExitStack() as stack:
        managers = tuple([
            await stack.enter_async_context(runner(service))
            for service in services
        ])
        # If any of the services terminate, we do so as well.
        await wait_first([
            asyncio.create_task(manager.wait_finished())
            for manager in managers
        ])
