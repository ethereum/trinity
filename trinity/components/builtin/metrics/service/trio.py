import functools
from typing import (
    Callable,
    TypeVar,
)

from asks import Session

from p2p import trio_utils

from trinity.components.builtin.metrics.service.base import BaseMetricsService


T = TypeVar('T')


# temporary workaround to support decorator typing until we can use
# @functools.cached_property with python version >= 3.8
# https://github.com/python/mypy/issues/5858
def cache(func: Callable[..., T]) -> T:
    return functools.lru_cache()(func)  # type: ignore


class TrioMetricsService(BaseMetricsService):
    @property  # type: ignore
    @cache
    def session(self) -> Session:
        url = self.reporter._get_post_url()
        auth_header = self.reporter._generate_auth_header()
        return Session(url, headers=auth_header)

    async def async_post(self, data: str) -> None:
        # use trio-compatible asks library for async http calls
        await self.session.post(data=data)

    async def continuously_report(self) -> None:
        async for _ in trio_utils.every(self._reporting_frequency):
            await self.report_now()
