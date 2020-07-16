from p2p import trio_utils

from trinity.components.builtin.metrics.service.base import BaseMetricsService


class TrioMetricsService(BaseMetricsService):

    async def continuously_report(self) -> None:
        async for _ in trio_utils.every(self._reporting_frequency):
            super().report_now()
