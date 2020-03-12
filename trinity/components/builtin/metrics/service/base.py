from abc import abstractmethod

from async_service import Service
from eth_utils import get_extended_debug_logger
from pyformance.reporters import InfluxReporter

from trinity.components.builtin.metrics.abc import MetricsServiceAPI
from trinity.components.builtin.metrics.registry import HostMetricsRegistry


class BaseMetricsService(Service, MetricsServiceAPI):
    """
    A service to provide a registry where metrics instruments can be registered and retrieved from.
    It continuously reports metrics to the specified InfluxDB instance.
    """

    def __init__(self,
                 influx_server: str,
                 influx_user: str,
                 influx_password: str,
                 influx_database: str,
                 host: str,
                 reporting_frequency: int = 10):
        self._influx_server = influx_server
        self._reporting_frequency = reporting_frequency
        self._registry = HostMetricsRegistry(host)
        self._reporter = InfluxReporter(
            registry=self._registry,
            protocol='https',
            port=443,
            database=influx_database,
            username=influx_user,
            password=influx_password,
            server=influx_server
        )

    logger = get_extended_debug_logger('trinity.components.builtin.metrics.MetricsService')

    @property
    def registry(self) -> HostMetricsRegistry:
        """
        Return the :class:`trinity.components.builtin.metrics.registry.HostMetricsRegistry` at which
        metrics instruments can be registered and retrieved.
        """
        return self._registry

    async def run(self) -> None:
        self.logger.info("Reporting metrics to %s", self._influx_server)
        self.manager.run_daemon_task(self.continuously_report)
        await self.manager.wait_finished()

    @abstractmethod
    async def continuously_report(self) -> None:
        ...
