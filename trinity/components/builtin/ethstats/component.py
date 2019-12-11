import os
import platform

from argparse import (
    ArgumentParser,
    _SubParsersAction,
)

from async_service import run_asyncio_service
from eth_utils import ValidationError

from lahja.base import EndpointAPI

from trinity.boot_info import BootInfo
from trinity.constants import (
    MAINNET_NETWORK_ID,
    ROPSTEN_NETWORK_ID,
)
from trinity.extensibility import (
    AsyncioIsolatedComponent,
)

from trinity.components.builtin.ethstats.ethstats_service import (
    EthstatsService,
)

DEFAULT_SERVERS_URLS = {
    MAINNET_NETWORK_ID: 'wss://ethstats.net/api',
    ROPSTEN_NETWORK_ID: 'wss://ropsten-stats.parity.io/api',
}


def get_default_server_url(network_id: int) -> str:
    return DEFAULT_SERVERS_URLS.get(network_id, '')


class EthstatsComponent(AsyncioIsolatedComponent):
    name = 'ethstats'

    server_url: str
    server_secret: str
    stats_interval: int
    node_id: str
    node_contact: str

    @classmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        ethstats_parser = arg_parser.add_argument_group('ethstats (experimental)')

        ethstats_parser.add_argument(
            '--ethstats',
            action='store_true',
            help='Enable node stats reporting service',
        )

        ethstats_parser.add_argument(
            '--ethstats-server-url',
            help='Node stats server URL (e. g. wss://example.com/api)',
            default=os.environ.get('ETHSTATS_SERVER_URL'),
        )
        ethstats_parser.add_argument(
            '--ethstats-server-secret',
            help='Node stats server secret',
            default=os.environ.get('ETHSTATS_SERVER_SECRET'),
        )
        ethstats_parser.add_argument(
            '--ethstats-node-id',
            help='Node ID for stats server',
            default=os.environ.get('ETHSTATS_NODE_ID', platform.node()),
        )
        ethstats_parser.add_argument(
            '--ethstats-node-contact',
            help='Node contact information for stats server',
            default=os.environ.get('ETHSTATS_NODE_CONTACT', ''),
        )
        ethstats_parser.add_argument(
            '--ethstats-interval',
            help='The interval at which data is reported back',
            default=10,
        )

    @classmethod
    def validate_cli(cls, boot_info: BootInfo) -> None:
        args = boot_info.args

        if not args.ethstats:
            return

        network_id = boot_info.trinity_config.network_id

        if not (args.ethstats_server_url or get_default_server_url(network_id)):
            raise ValidationError(
                'You must provide ethstats server url using the `--ethstats-server-url`'
            )

        if not args.ethstats_server_secret:
            raise ValidationError(
                'You must provide ethstats server secret using `--ethstats-server-secret`'
            )

    @property
    def is_enabled(self) -> bool:
        return bool(self._boot_info.args.ethstats)

    @classmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        args = boot_info.args

        if args.ethstats_server_url:
            server_url = args.ethstats_server_url
        else:
            server_url = get_default_server_url(boot_info.trinity_config.network_id)

        service = EthstatsService(
            boot_info,
            event_bus,
            server_url,
            args.ethstats_server_secret,
            args.ethstats_node_id,
            args.ethstats_node_contact,
            args.ethstats_interval,
        )

        await run_asyncio_service(service)
