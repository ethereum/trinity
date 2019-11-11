import os
import platform

from argparse import (
    ArgumentParser,
    _SubParsersAction,
)

from asyncio_run_in_process import run_in_process

from eth_utils import (
    ValidationError,
)

from p2p.service import AsyncioManager

from trinity.constants import (
    MAINNET_NETWORK_ID,
    ROPSTEN_NETWORK_ID,
)
from trinity.extensibility import (
    BaseApplicationComponent,
    TrinityBootInfo,
    ComponentService,
)
from trinity.components.builtin.ethstats.ethstats_service import (
    EthstatsService,
)

DEFAULT_SERVERS_URLS = {
    MAINNET_NETWORK_ID: 'wss://ethstats.net/api',
    ROPSTEN_NETWORK_ID: 'wss://ropsten-stats.parity.io/api',
}


def _get_default_server_url(network_id: int) -> str:
    return DEFAULT_SERVERS_URLS.get(network_id, '')


class EthstatsComponent(BaseApplicationComponent):
    server_url: str
    server_secret: str
    stats_interval: int
    node_id: str
    node_contact: str

    name = 'Ethstats'

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

    @property
    def is_enabled(self) -> bool:
        return bool(self._boot_info.args.ethstats)

    @classmethod
    def validate_cli(self, boot_info: TrinityBootInfo) -> None:
        args = self._boot_info.args
        has_server_url = (
            args.ethstats_server_url or
            _get_default_server_url(self._boot_info.trinity_config.network_id)
        )

        if not has_server_url:
            raise ValidationError(
                'You must provide ethstats server url using the `--ethstats-server-url`'
            )
        elif not args.ethstats_server_secret:
            raise ValidationError(
                'You must provide ethstats server secret using `--ethstats-server-secret`'
            )

    async def run(self) -> None:
        service = EthstatsComponentService(self._boot_info, self.name)

        await run_in_process(AsyncioManager.run_service, service)


class EthstatsComponentService(ComponentService):

    async def run_component_service(self) -> None:
        args = self.boot_info.args

        server_url: str

        if (args.ethstats_server_url):
            server_url = args.ethstats_server_url
        else:
            server_url = _get_default_server_url(self.boot_info.trinity_config.network_id)

        server_secret = args.ethstats_server_secret

        node_id = args.ethstats_node_id
        node_contact = args.ethstats_node_contact
        stats_interval = args.ethstats_interval

        service = EthstatsService(
            self.boot_info,
            self.event_bus,
            server_url,
            server_secret,
            node_id,
            node_contact,
            stats_interval,
        )

        self.manager.run_daemon_child_service(service)
