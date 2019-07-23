from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
import asyncio

from lahja import EndpointAPI

from trinity.config import (
    Eth1AppConfig,
)
from trinity.constants import (
    NETWORKING_EVENTBUS_ENDPOINT,
)
from trinity.extensibility.asyncio import (
    AsyncioIsolatedPlugin
)
from trinity._utils.shutdown import (
    exit_with_services,
)


class PeerPoolPlugin(AsyncioIsolatedPlugin):

    @property
    def name(self) -> str:
        return "PeerPool"

    @property
    def normalized_name(self) -> str:
        return NETWORKING_EVENTBUS_ENDPOINT

    @classmethod
    def configure_parser(cls,
                         arg_parser: ArgumentParser,
                         subparser: _SubParsersAction) -> None:
        peer_pool_parser = arg_parser.add_argument_group('peer pool')

        peer_pool_parser.add_argument(
            '--disable-peer-pool',
            help=(
                "Disables the builtin 'Peer Pool' plugin. "
                "**WARNING**: disabling this API without a proper replacement "
                "will cause your trinity node to crash."
            ),
            action='store_true',
        )

    def on_ready(self, manager_eventbus: EndpointAPI) -> None:
        if self.boot_info.args.disable_peer_pool:
            self.logger.warning("Peer Pool disabled via CLI flag")
            # Allow this plugin to be disabled for extreme cases such as the
            # user swapping in an equivalent experimental version.
            return
        self.start()

    def do_start(self) -> None:

        trinity_config = self.boot_info.trinity_config

        # TODO: Eventually, we can probably get rid of the NodeClass entirely and run
        # the PeerPool directly
        NodeClass = trinity_config.get_app_config(Eth1AppConfig).node_class
        node = NodeClass(self.event_bus, trinity_config)

        asyncio.ensure_future(exit_with_services(self._event_bus_service, node))
        asyncio.ensure_future(node.run())
