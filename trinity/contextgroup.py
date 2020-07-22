import asyncio
import sys
from types import TracebackType
from typing import Any, AsyncContextManager, List, Optional, Sequence, Tuple, Type

from trio import MultiError

from p2p.asyncio_utils import create_task


class AsyncContextGroup:

    def __init__(self, context_managers: Sequence[AsyncContextManager[Any]]) -> None:
        self.cms = tuple(context_managers)
        self.cms_to_exit: Sequence[AsyncContextManager[Any]] = tuple()

    async def __aenter__(self) -> Tuple[Any, ...]:
        futures = [create_task(cm.__aenter__(), f'AsyncContextGroup/{repr(cm)}') for cm in self.cms]
        await asyncio.wait(futures)
        # Exclude futures not successfully entered from the list so that we don't attempt to exit
        # them.
        self.cms_to_exit = tuple(
            cm for cm, future in zip(self.cms, futures)
            if not future.cancelled() and not future.exception())
        try:
            return tuple(future.result() for future in futures)
        except:  # noqa: E722
            await self._exit(*sys.exc_info())
            raise

    async def _exit(self,
                    exc_type: Optional[Type[BaseException]],
                    exc_value: Optional[BaseException],
                    traceback: Optional[TracebackType],
                    ) -> None:
        if not self.cms_to_exit:
            return
        # don't use gather() to ensure that we wait for all __aexit__s
        # to complete even if one of them raises
        done, _pending = await asyncio.wait(
            [cm.__aexit__(exc_type, exc_value, traceback) for cm in self.cms_to_exit])
        # This is to ensure we re-raise any exceptions our coroutines raise when exiting.
        errors: List[Tuple[Type[BaseException], BaseException, TracebackType]] = []
        for d in done:
            try:
                d.result()
            except BaseException:
                errors.append(sys.exc_info())
        if errors:
            raise MultiError(
                tuple(exc_value.with_traceback(exc_tb) for _, exc_value, exc_tb in errors))

    async def __aexit__(self,
                        exc_type: Optional[Type[BaseException]],
                        exc_value: Optional[BaseException],
                        traceback: Optional[TracebackType],
                        ) -> None:
        # Since exits are running in parallel, they can't see each
        # other exceptions, so send exception info from `async with`
        # body to all.
        await self._exit(exc_type, exc_value, traceback)
