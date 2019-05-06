import asyncio
import logging
import time

from eth_utils import (
    humanize_seconds,
)
from p2p.service import (
    BaseService,
)

from trinity.db.eth1.header import (
    BaseAsyncHeaderDB,
)
from trinity.endpoint import (
    TrinityEventBusEndpoint,
)


class PerformanceTracker(BaseService):
    """
    Continously track and report performance.
    """

    _tracking_startet_at: float = 0.0
    _first_block_started_at: float = 0.0
    _target_hit_at: float = 0.0

    def __init__(self,
                 header_db: BaseAsyncHeaderDB,
                 event_bus: TrinityEventBusEndpoint,
                 label: str,
                 target_block_number: int,
                 reporting_interval: int,
                 report_path: str) -> None:
        super().__init__()
        self.header_db = header_db
        self.event_bus = event_bus
        self.label = label
        self.target_block_number = target_block_number
        self.reporting_interval = reporting_interval
        file_handler = logging.handlers.RotatingFileHandler(report_path)
        file_handler.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)

    async def _run(self) -> None:
        self._tracking_startet_at = time.perf_counter()
        while self.is_operational:
            await self._check()
            await asyncio.sleep(self.reporting_interval)

    async def _check(self) -> None:
        current_head = await self.header_db.coro_get_canonical_head()
        block_number = current_head.block_number

        self.logger.debug("Current block: %s, target: %s", block_number, self.target_block_number)

        if block_number > 0 and self._first_block_started_at == 0.0:
            self._first_block_started_at = time.perf_counter()

        if block_number >= self.target_block_number:
            self._target_hit_at = time.perf_counter()
            self.report(block_number)
            self.event_bus.request_shutdown("Performance tracking ended")

    def report(self, last_block_number: int) -> None:
        total_duration = int(self._target_hit_at - self._tracking_startet_at)
        actual_duration = 0

        if self._first_block_started_at > 0.0:
            actual_duration = int(self._target_hit_at - self._first_block_started_at)

        blocks_per_second = 0 if actual_duration == 0 else last_block_number / actual_duration

        self.logger.info(f"Performance tracking finished run: {self.label}")
        self.logger.info(f"Hit target block {self.target_block_number} ({last_block_number})")
        self.logger.info(f"Total duration: {humanize_seconds(total_duration)}")
        self.logger.info(f"Actual duration: {humanize_seconds(actual_duration)}")
        self.logger.info(f"Blocks per second: {blocks_per_second}")
        self.logger.info("#####################")
