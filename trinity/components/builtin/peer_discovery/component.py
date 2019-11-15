from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
import asyncio
from typing import (
    Type,
)

from lahja import EndpointAPI

from eth_typing import (
    BlockNumber,
)
from eth.constants import (
    GENESIS_BLOCK_NUMBER
)
from eth.db.header import HeaderDB

from p2p.abc import ProtocolAPI
from p2p.constants import (
    DISCOVERY_EVENTBUS_ENDPOINT,
)
from p2p.discovery import (
    get_v5_topic,
    DiscoveryByTopicProtocol,
    DiscoveryProtocol,
    DiscoveryService,
    PreferredNodeDiscoveryProtocol,
    StaticDiscoveryService,
)
from p2p.kademlia import (
    Address,
)
from p2p.legacy_service import BaseService

from trinity.config import (
    Eth1AppConfig,
    Eth1DbMode,
    TrinityConfig,
)
from trinity.db.manager import DBClient
from trinity.events import ShutdownRequest
from trinity.extensibility import (
    AsyncioIsolatedComponent,
)
from trinity.protocol.eth.proto import (
    ETHProtocol,
)
from trinity.protocol.les.proto import (
    LESProtocolV2,
)
from trinity._utils.shutdown import (
    exit_with_services,
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


def get_discv5_topic(trinity_config: TrinityConfig, protocol: Type[ProtocolAPI]) -> bytes:
    db = DBClient.connect(trinity_config.database_ipc_path)

    header_db = HeaderDB(db)
    genesis_hash = header_db.get_canonical_block_hash(BlockNumber(GENESIS_BLOCK_NUMBER))

    return get_v5_topic(protocol, genesis_hash)


class DiscoveryBootstrapService(BaseService):
    """
    Bootstrap discovery to provide a parent ``CancellationToken``
    """

    def __init__(self,
                 disable_discovery: bool,
                 event_bus: EndpointAPI,
                 trinity_config: TrinityConfig) -> None:
        super().__init__()
        self.is_discovery_disabled = disable_discovery
        self.event_bus = event_bus
        self.trinity_config = trinity_config

    async def _run(self) -> None:
        external_ip = "0.0.0.0"
        address = Address(external_ip, self.trinity_config.port, self.trinity_config.port)

        if self.trinity_config.use_discv5:
            protocol = get_protocol(self.trinity_config)
            topic = get_discv5_topic(self.trinity_config, protocol)

            discovery_protocol: DiscoveryProtocol = DiscoveryByTopicProtocol(
                topic,
                self.trinity_config.nodekey,
                address,
                self.trinity_config.bootstrap_nodes,
                self.cancel_token,
            )
        else:
            discovery_protocol = PreferredNodeDiscoveryProtocol(
                self.trinity_config.nodekey,
                address,
                self.trinity_config.bootstrap_nodes,
                self.trinity_config.preferred_nodes,
                self.cancel_token,
            )

        if self.is_discovery_disabled:
            discovery_service: BaseService = StaticDiscoveryService(
                self.event_bus,
                self.trinity_config.preferred_nodes,
                self.cancel_token,
            )
        else:
            discovery_service = DiscoveryService(
                discovery_protocol,
                self.trinity_config.port,
                self.event_bus,
                self.cancel_token,
            )

        try:
            await discovery_service.run()
        except Exception:
            await self.event_bus.broadcast(ShutdownRequest("Discovery ended unexpectedly"))


class PeerDiscoveryComponent(AsyncioIsolatedComponent):
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

    def do_start(self) -> None:
        discovery_bootstrap = DiscoveryBootstrapService(
            self.boot_info.args.disable_discovery,
            self.event_bus,
            self.boot_info.trinity_config
        )
        asyncio.ensure_future(exit_with_services(
            discovery_bootstrap,
            self._event_bus_service,
        ))
        asyncio.ensure_future(discovery_bootstrap.run())
