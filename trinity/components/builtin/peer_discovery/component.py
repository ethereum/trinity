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

from trinity.boot_info import BootInfo
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
    name = "Discovery"
    endpoint_name = DISCOVERY_EVENTBUS_ENDPOINT

    @property
    def is_enabled(self) -> bool:
        return True

    @classmethod
    def configure_parser(cls,
                         arg_parser: ArgumentParser,
                         subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--disable-discovery",
            action="store_true",
            help="Disable peer discovery",
        )

    @classmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        config = boot_info.trinity_config
        external_ip = "0.0.0.0"
        address = Address(external_ip, config.port, config.port)

        if boot_info.args.disable_discovery:
            discovery_service: Service = StaticDiscoveryService(
                event_bus,
                config.preferred_nodes,
            )
        else:
            external_ip = "0.0.0.0"
            socket = trio.socket.socket(family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM)
            await socket.bind((external_ip, config.port))
            discovery_service = PreferredNodeDiscoveryService(
                boot_info.trinity_config.nodekey,
                address,
                config.bootstrap_nodes,
                config.preferred_nodes,
                event_bus,
                socket,
            )

        try:
            await TrioManager.run_service(discovery_service)
        except Exception:
            await event_bus.broadcast(ShutdownRequest("Discovery ended unexpectedly"))
            raise
