import aiohttp
import asyncio

from trinity.components.builtin.metrics.service.base import BaseMetricsService


class AsyncioMetricsService(BaseMetricsService):
    async def async_post(self, data: str) -> None:
        # use asyncio-compatible aiohttp for async http calls
        url = self.reporter._get_post_url()
        auth_header = self.reporter._generate_auth_header()
        async with aiohttp.ClientSession() as session:
            await session.post(url, data=data, headers=auth_header)

    async def continuously_report(self) -> None:
        while self.manager.is_running:
            await self.report_now()
            await asyncio.sleep(self._reporting_frequency)
