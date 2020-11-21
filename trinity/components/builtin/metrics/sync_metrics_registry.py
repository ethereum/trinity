import time

from eth_typing import BlockNumber

from trinity.components.builtin.metrics.abc import MetricsServiceAPI


class SyncMetricsRegistry:
    """
    Registry to track weighted moving average of pivot events, and report to InfluxDB.
    """
    def __init__(self, metrics_service: MetricsServiceAPI) -> None:
        self.metrics_service = metrics_service
        self.pivot_meter = metrics_service.registry.meter('trinity.p2p/sync/pivot_rate.meter')

    def record_lag(self, lag: int) -> None:
        self.metrics_service.registry.gauge('trinity.sync/chain_head_lag').set_value(lag)

    async def record_pivot(self, block_number: BlockNumber) -> None:
        # record pivot and send event annotation to influxdb
        self.pivot_meter.mark()
        pivot_time = int(time.time())
        post_data = (
            f'events title="beam pivot @ block {block_number}",'
            f'text="pivot event",tags="{self.metrics_service.registry.host}" {pivot_time}'
        )
        await self.metrics_service.send_annotation(post_data)
