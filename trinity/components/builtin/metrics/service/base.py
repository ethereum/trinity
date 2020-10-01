from abc import abstractmethod
import base64
from http.client import HTTPException
import time
from typing import (
    Awaitable,
    Callable,
    Dict,
)
from urllib import parse

from async_service import Service
from pyformance.reporters.influx import InfluxReporter

from trinity.components.builtin.metrics.abc import MetricsServiceAPI
from trinity.components.builtin.metrics.registry import HostMetricsRegistry
from trinity._utils.logging import get_logger


class ExtendedInfluxReporter(InfluxReporter):
    """
    ``InfluxReporter`` extended to send annotations and metrics
    to InfluxDB asynchronously.
    """

    def __init__(self,
                 database: str,
                 username: str,
                 password: str,
                 protocol: str,
                 port: int,
                 server: str,
                 registry: HostMetricsRegistry,
                 async_post: Callable[[str], Awaitable[None]]) -> None:
        super().__init__(
            database=database,
            username=username,
            password=password,
            protocol=protocol,
            port=port,
            server=server,
            registry=registry,
        )
        self.async_post = async_post

    async def send_annotation(self, annotation_data: str) -> None:
        await self.async_post(annotation_data)

    def _generate_auth_header(self) -> Dict[str, str]:
        auth_string = ("%s:%s" % (self.username, self.password)).encode()
        auth = base64.b64encode(auth_string)
        return {"Authorization": f"Basic {auth.decode('utf-8')}"}

    def _get_post_url(self) -> str:
        parsed_url = parse.ParseResult(
            scheme=self.protocol,
            netloc=f"{self.server}:{self.port}",
            params="",
            path="/write",
            query=f"db={self.database}&precision=s",
            fragment="",
        )
        return parse.urlunparse(parsed_url)

    async def report_metrics(self,
                             registry: HostMetricsRegistry = None,
                             timestamp: int = None) -> None:
        # async implementation of InfluxReporter.report_now
        timestamp = timestamp if timestamp is not None else int(round(self.clock.time()))
        metrics = (registry or self.registry).dump_metrics()
        post_data = []
        for key, metric_values in metrics.items():
            if not self.prefix:
                table = key
            else:
                table = f"{self.prefix}.{key}"
            values = ",".join(["%s=%s" % (
                k, v if type(v) is not str
                else f'"{v}"')
                for (k, v) in metric_values.items()]
            )
            line = f"{table} {values} {timestamp}"
            post_data.append(line)
        data = "\n".join(post_data)
        await self.async_post(data)


class BaseMetricsService(Service, MetricsServiceAPI):
    """
    A service to provide a registry where metrics instruments can be registered and retrieved from.
    It continuously reports metrics to the specified InfluxDB instance.
    """

    MIN_SECONDS_BETWEEN_ERROR_LOGS = 60

    def __init__(self,
                 influx_server: str,
                 influx_user: str,
                 influx_password: str,
                 influx_database: str,
                 host: str,
                 port: int,
                 protocol: str,
                 reporting_frequency: int) -> None:
        self._unreported_error: Exception = None
        self._last_time_reported: float = 0.0
        self._influx_server = influx_server
        self._reporting_frequency = reporting_frequency
        self._registry = HostMetricsRegistry(host)
        self.reporter = ExtendedInfluxReporter(
            database=influx_database,
            username=influx_user,
            password=influx_password,
            protocol=protocol,
            port=port,
            server=self._influx_server,
            registry=self._registry,
            async_post=self.async_post,
        )

    logger = get_logger('trinity.components.builtin.metrics.MetricsService')

    @property
    def registry(self) -> HostMetricsRegistry:
        """
        Return the :class:`trinity.components.builtin.metrics.registry.HostMetricsRegistry` at which
        metrics instruments can be registered and retrieved.
        """
        return self._registry

    async def send_annotation(self, annotation_data: str) -> None:
        try:
            await self.reporter.send_annotation(annotation_data)
        except (HTTPException, ConnectionError) as exc:
            self.logger.warning("Unable to report annotations: %s", exc)

    async def run(self) -> None:
        self.logger.info("Reporting metrics to %s", self._influx_server)
        self.manager.run_daemon_task(self.continuously_report)
        await self.manager.wait_finished()

    async def report_now(self) -> None:
        try:
            await self.reporter.report_metrics()
        except (HTTPException, ConnectionError) as exc:

            # This method is usually called every few seconds. If there's an issue with the
            # connection we do not want to flood the log and tame down warnings.

            # 1. We log the first instance of an exception immediately
            # 2. We log follow up exceptions only after a minimum time has elapsed
            # This means that we also might overwrite exceptions for different errors

            if self._is_justified_to_log_error():
                self._log_and_clear(exc)
            else:
                self._unreported_error = exc
        else:
            # If errors disappear, we want to make sure we eventually report the last instance
            if self._unreported_error is not None and self._is_justified_to_log_error():
                self._log_and_clear(self._unreported_error)

    def _log_and_clear(self, error: Exception) -> None:
        self.logger.warning("Unable to report metrics: %s", error)
        self._unreported_error = None
        self._last_time_reported = time.monotonic()

    def _is_justified_to_log_error(self) -> bool:
        return (
            self._last_time_reported == 0.0 or
            time.monotonic() - self._last_time_reported > self.MIN_SECONDS_BETWEEN_ERROR_LOGS
        )

    @abstractmethod
    async def continuously_report(self) -> None:
        ...

    @abstractmethod
    async def async_post(self, data: str) -> None:
        ...
