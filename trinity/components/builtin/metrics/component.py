import os
from argparse import (
    ArgumentParser,
    _SubParsersAction,
)


from async_exit_stack import AsyncExitStack
from async_service import background_trio_service
from eth_utils import ValidationError

from lahja import EndpointAPI

from trinity.boot_info import BootInfo
from trinity.components.builtin.metrics.metrics_service import MetricsService
from trinity.components.builtin.metrics.system_metrics_collector import collect_process_metrics

from trinity.extensibility import (
    TrioIsolatedComponent,
)


class MetricsComponent(TrioIsolatedComponent):
    """
    A component to continuously collect and report system wide metrics.
    """
    name = "Metrics"

    @property
    def is_enabled(self) -> bool:
        return bool(self._boot_info.args.enable_metrics)

    @classmethod
    def configure_parser(cls,
                         arg_parser: ArgumentParser,
                         subparser: _SubParsersAction) -> None:

        metrics_parser = arg_parser.add_argument_group('metrics')

        metrics_parser.add_argument(
            "--enable-metrics",
            action="store_true",
            help="Enable metrics component",
        )

        metrics_parser.add_argument(
            '--metrics-host',
            help='Host name to tag the metrics data (e.g. trinity-bootnode-europe-pt',
            default=os.environ.get('TRINITY_METRICS_HOST'),
        )

        metrics_parser.add_argument(
            '--metrics-influx-user',
            help='Influx DB user. Defaults to `trinity`',
            default='trinity',
        )

        metrics_parser.add_argument(
            '--metrics-influx-database',
            help='Influx DB name. Defaults to `trinity`',
            default='trinity',
        )

        metrics_parser.add_argument(
            '--metrics-influx-password',
            help='Influx DB password. Defaults to ENV var TRINITY_METRICS_INFLUX_DB_PW',
            default=os.environ.get('TRINITY_METRICS_INFLUX_DB_PW'),
        )

        metrics_parser.add_argument(
            '--metrics-influx-server',
            help='Influx DB server. Defaults to ENV var TRINITY_METRICS_INFLUX_DB_SERVER',
            default=os.environ.get('TRINITY_METRICS_INFLUX_DB_SERVER'),
        )

        metrics_parser.add_argument(
            '--metrics-reporting-frequency',
            help='The frequency in seconds at which metrics are reported',
            default=10,
        )

        metrics_parser.add_argument(
            '--metrics-system-collector-frequency',
            help='The frequency in seconds at which system metrics are collected',
            default=3,
        )

    @classmethod
    def validate_cli(cls, boot_info: BootInfo) -> None:
        args = boot_info.args

        if not args.enable_metrics:
            return

        if not args.metrics_host:
            raise ValidationError(
                'You must provide the metrics host that is used '
                'to tag the data via `--metrics-host`'
            )

    @classmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:

        metrics_service = MetricsService(
            influx_server=boot_info.args.metrics_influx_server,
            influx_user=boot_info.args.metrics_influx_user,
            influx_password=boot_info.args.metrics_influx_password,
            influx_database=boot_info.args.metrics_influx_database,
            host=boot_info.args.metrics_host,
            reporting_frequency=boot_info.args.metrics_reporting_frequency,
        )

        # types ignored due to https://github.com/ethereum/async-service/issues/5
        system_metrics_collector = collect_process_metrics(  # type: ignore
            metrics_service.registry,
            frequency_seconds=boot_info.args.metrics_system_collector_frequency
        )

        services_to_exit = (metrics_service, system_metrics_collector,)

        async with AsyncExitStack() as stack:
            managers = tuple([
                await stack.enter_async_context(background_trio_service(service))
                for service in services_to_exit
            ])
            await managers[0].wait_finished()
