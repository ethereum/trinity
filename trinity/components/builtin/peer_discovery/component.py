from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
from typing import (
    Type,
)

import trio

from async_service import Service, TrioManager

from lahja import EndpointAPI

from p2p.abc import ProtocolAPI
from p2p.constants import (
    DISCOVERY_EVENTBUS_ENDPOINT,
)
from p2p.discovery import (
    PreferredNodeDiscoveryService,
    StaticDiscoveryService,
)
from p2p.kademlia import (
    Address,
)

from trinity.config import (
    Eth1AppConfig,
    Eth1DbMode,
    TrinityConfig,
)
from trinity.events import ShutdownRequest
from trinity.extensibility import (
    TrioIsolatedComponent,
)
from trinity.protocol.eth.proto import (
    ETHProtocol,
)
from trinity.protocol.les.proto import (
    LESProtocolV2,
)


def get_protocol(trinity_config: TrinityConfig) -> Type[ProtocolAPI]:
    # For now DiscoveryByTopicProtocol supports a single topic, so we use the latest
    # version of our supported protocols. Maybe this could be more generic?
    # TODO: This needs to support the beacon protocol when we have a way to
    # check the config, if trinity is being run as a beacon node.

    eth1_config = trinity_config.get_app_config(Eth1AppConfig)
    if eth1_config.database_mode is Eth1DbMode.LIGHT:
        return LESProtocolV2
    else:
        return ETHProtocol


class PeerDiscoveryComponent(TrioIsolatedComponent):
    """
    Continously discover other Ethereum nodes.
    """

    @property
    def name(self) -> str:
        return "Discovery"

    @property
    def normalized_name(self) -> str:
        return DISCOVERY_EVENTBUS_ENDPOINT

    def on_ready(self, manager_eventbus: EndpointAPI) -> None:
        self.start()

    @classmethod
    def configure_parser(cls,
                         arg_parser: ArgumentParser,
                         subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--disable-discovery",
            action="store_true",
            help="Disable peer discovery",
        )

    async def run(self) -> None:
        config = self.boot_info.trinity_config

        if self.boot_info.args.disable_discovery:
            discovery_service: Service = StaticDiscoveryService(
                self.event_bus,
                config.preferred_nodes,
            )
        else:
            external_ip = "0.0.0.0"
            socket = trio.socket.socket(family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM)
            await socket.bind((external_ip, config.port))
            discovery_service = PreferredNodeDiscoveryService(
                self.boot_info.trinity_config.nodekey,
                Address(external_ip, config.port, config.port),
                config.bootstrap_nodes,
                config.preferred_nodes,
                self.event_bus,
                socket,
            )

        try:
            await TrioManager.run_service(discovery_service)
        except Exception:
            await self.event_bus.broadcast(ShutdownRequest("Discovery ended unexpectedly"))
            raise
