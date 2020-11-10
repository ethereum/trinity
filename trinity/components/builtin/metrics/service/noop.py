from async_service import Service

from trinity.components.builtin.metrics.abc import MetricsServiceAPI
from trinity.components.builtin.metrics.registry import NoopMetricsRegistry
from trinity._utils.logging import get_logger


class NoopMetricsService(Service, MetricsServiceAPI):
    """
    A ``MetricsServiceAPI`` implementation that provides a stub registry where every action results
    in a noop. Intended to be used when collecting of metrics is disabled. Every collected metric
    will only incur the cost of a noop.
    """

    logger = get_logger('trinity.components.builtin.metrics.NoopMetricsService')

    def __init__(self,
                 influx_server: str = '',
                 influx_user: str = '',
                 influx_password: str = '',
                 influx_database: str = '',
                 host: str = '',
                 reporting_frequency: int = 10):

        self._registry = NoopMetricsRegistry()

    @property
    def registry(self) -> NoopMetricsRegistry:
        """
        Return the :class:`trinity.components.builtin.metrics.registry.NoopMetricsRegistry` at which
        metrics instruments can be registered and retrieved.
        """
        return self._registry

    async def run(self) -> None:
        self.logger.info("Running NoopMetricsService")
        await self.manager.wait_finished()

    async def continuously_report(self) -> None:
        pass

    async def send_annotation(self, annotation_data: str) -> None:
        pass


NOOP_METRICS_SERVICE = NoopMetricsService()
NOOP_METRICS_REGISTRY = NoopMetricsRegistry()
