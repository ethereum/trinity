from abc import abstractmethod
from typing import (
    Any,
    Callable,
)

from asyncio_run_in_process import run_in_process
from asyncio_run_in_process.typing import SubprocessKwargs
from async_service import background_asyncio_service
from lahja import EndpointAPI


from trinity._utils.logging import child_process_logging, get_logger
from trinity._utils.profiling import profiler

from .component import BaseIsolatedComponent, TReturn
from .event_bus import AsyncioEventBusService


class AsyncioIsolatedComponent(BaseIsolatedComponent):
    logger = get_logger('trinity.extensibility.asyncio.AsyncioIsolatedComponent')

    async def _run_in_process(
            self,
            async_fn: Callable[..., TReturn],
            *args: Any,
            subprocess_kwargs: 'SubprocessKwargs' = None,
    ) -> TReturn:
        return await run_in_process(async_fn, *args, subprocess_kwargs=subprocess_kwargs)

    async def _do_run(self) -> None:
        with child_process_logging(self._boot_info):
            endpoint_name = self.get_endpoint_name()
            event_bus_service = AsyncioEventBusService(
                self._boot_info.trinity_config,
                endpoint_name,
            )
            async with background_asyncio_service(event_bus_service):
                event_bus = await event_bus_service.get_event_bus()

                try:
                    if self._boot_info.profile:
                        with profiler(f'profile_{self.get_endpoint_name()}'):
                            await self.do_run(event_bus)
                    else:
                        # XXX: When open_in_process() injects a KeyboardInterrupt into us (via
                        # coro.throw()), we hang forever here, until open_in_process() times out
                        # and sends us a SIGTERM, at which point we exit without executing either
                        # the except or the finally blocks below.
                        # See https://github.com/ethereum/trinity/issues/1711 for more.
                        await self.do_run(event_bus)
                except KeyboardInterrupt:
                    # Currently we never reach this code path, but when we fix the issue above it
                    # will be needed.
                    return
                finally:
                    # Once we start seeing this in the logs after a Ctrl-C, we'll likely have
                    # figured out the issue above.
                    self.logger.debug("%s: do_run() finished", self)

    @abstractmethod
    async def do_run(self, event_bus: EndpointAPI) -> None:
        """
        Define the entry point of the component. Should be overwritten in subclasses.
        """
        ...
