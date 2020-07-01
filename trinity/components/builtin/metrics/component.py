import os
from argparse import (
    ArgumentParser,
    Namespace,
    _SubParsersAction,
)
from typing import Type

from eth_utils import ValidationError

from lahja import EndpointAPI

from trinity.boot_info import BootInfo
from trinity.components.builtin.metrics.abc import MetricsServiceAPI
from trinity.components.builtin.metrics.service.trio import TrioMetricsService
from trinity.components.builtin.metrics.blockchain_metrics_collector import (
    collect_blockchain_metrics,
)
from trinity.components.builtin.metrics.system_metrics_collector import collect_process_metrics

from trinity.extensibility import (
    TrioIsolatedComponent,
)
from trinity._utils.services import run_background_trio_services


def metrics_service_from_args(
        args: Namespace,
        metrics_service_class: Type[MetricsServiceAPI] = TrioMetricsService) -> MetricsServiceAPI:
    return metrics_service_class(
        influx_server=args.metrics_influx_server,
        influx_user=args.metrics_influx_user,
        influx_password=args.metrics_influx_password,
        influx_database=args.metrics_influx_database,
        host=args.metrics_host,
        port=args.metrics_influx_port,
        protocol=args.metrics_influx_protocol,
        reporting_frequency=args.metrics_reporting_frequency,
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
            help='Host name to tag the metrics data (e.g. trinity-bootnode-europe-pt)',
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
            '--metrics-influx-port',
            help='Influx DB port. Defaults to ENV var TRINITY_METRICS_INFLUX_DB_PORT or 8086',
            default=os.environ.get('TRINITY_METRICS_INFLUX_DB_PORT', 8086),
        )

        metrics_parser.add_argument(
            '--metrics-influx-protocol',
            help=(
                'Influx DB protocol. Defaults to ENV var '
                'TRINITY_METRICS_INFLUX_DB_PROTOCOL or http'
            ),
            default=os.environ.get('TRINITY_METRICS_INFLUX_DB_PROTOCOL', 'http'),
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

        metrics_parser.add_argument(
            '--metrics-blockchain-collector-frequency',
            help='The frequency in seconds at which blockchain metrics are collected',
            default=15,
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

    async def do_run(self, event_bus: EndpointAPI) -> None:
        boot_info = self._boot_info
        metrics_service = metrics_service_from_args(boot_info.args)

        # types ignored due to https://github.com/ethereum/async-service/issues/5
        system_metrics_collector = collect_process_metrics(  # type: ignore
            metrics_service.registry,
            frequency_seconds=boot_info.args.metrics_system_collector_frequency,
        )

        # types ignored due to https://github.com/ethereum/async-service/issues/5
        blockchain_metrics_collector = collect_blockchain_metrics(  # type: ignore
            boot_info,
            event_bus,
            metrics_service.registry,
            frequency_seconds=boot_info.args.metrics_blockchain_collector_frequency,
        )

        await run_background_trio_services([
            metrics_service,
            system_metrics_collector,
            blockchain_metrics_collector,
        ])
