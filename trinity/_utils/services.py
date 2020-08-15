import asyncio
import contextlib
from typing import Sequence

from async_service import (
    background_asyncio_service,
    background_trio_service,
    ServiceAPI,
)

from p2p.asyncio_utils import wait_first as wait_first_asyncio
from trinity._utils.trio_utils import wait_first as wait_first_trio


async def run_background_asyncio_services(services: Sequence[ServiceAPI]) -> None:
    async with contextlib.AsyncExitStack() as stack:
        managers = tuple([
            await stack.enter_async_context(background_asyncio_service(service))
            for service in services
        ])
        # If any of the services terminate, we do so as well.
        await wait_first_asyncio(
            [asyncio.create_task(manager.wait_finished()) for manager in managers],
            max_wait_after_cancellation=2
        )


async def run_background_trio_services(services: Sequence[ServiceAPI]) -> None:
    async with contextlib.AsyncExitStack() as stack:
        managers = tuple([
            await stack.enter_async_context(background_trio_service(service))
            for service in services
        ])
        # If any of the services terminate, we do so as well.
        await wait_first_trio([
            manager.wait_finished
            for manager in managers
        ])
