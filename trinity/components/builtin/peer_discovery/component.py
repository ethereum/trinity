from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
from typing import (
    Type,
)

from asyncio_run_in_process import run_in_process

from eth_typing import (
    BlockNumber,
)
from eth.constants import (
    GENESIS_BLOCK_NUMBER
)
from eth.db.header import HeaderDB
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
from p2p.protocol import (
    Protocol,
)
from p2p.service import AsyncioManager, ServiceAPI

from trinity.config import (
    Eth1AppConfig,
    Eth1DbMode,
    TrinityConfig,
)
from trinity.db.manager import DBClient
from trinity.events import ShutdownRequest
from trinity.extensibility import BaseApplicationComponent, ComponentService
from trinity.protocol.eth.proto import (
    ETHProtocol,
)
from trinity.protocol.les.proto import (
    LESProtocolV2,
)


def get_protocol(trinity_config: TrinityConfig) -> Type[Protocol]:
    # For now DiscoveryByTopicProtocol supports a single topic, so we use the latest
    # version of our supported protocols. Maybe this could be more generic?
    eth1_config = trinity_config.get_app_config(Eth1AppConfig)
    if eth1_config.database_mode is Eth1DbMode.LIGHT:
        return LESProtocolV2
    else:
        return ETHProtocol


def get_discv5_topic(trinity_config: TrinityConfig, protocol: Type[Protocol]) -> bytes:
    db = DBClient.connect(trinity_config.database_ipc_path)

    header_db = HeaderDB(db)
    genesis_hash = header_db.get_canonical_block_hash(BlockNumber(GENESIS_BLOCK_NUMBER))

    return get_v5_topic(protocol, genesis_hash)


class DiscoveryBootstrapService(ComponentService):
    """
    Bootstrap discovery to provide a parent ``CancellationToken``
    """
    _explicit_ipc_filename = DISCOVERY_EVENTBUS_ENDPOINT

    async def run_component_service(self) -> None:
        external_ip = "0.0.0.0"
        trinity_config = self.boot_info.trinity_config
        address = Address(external_ip, trinity_config.port, trinity_config.port)
        is_discovery_disabled = self.boot_info.args.disable_discovery

        if trinity_config.use_discv5:
            protocol = get_protocol(trinity_config)
            topic = get_discv5_topic(trinity_config, protocol)

            discovery_protocol: DiscoveryProtocol = DiscoveryByTopicProtocol(
                topic,
                trinity_config.nodekey,
                address,
                trinity_config.bootstrap_nodes,
            )
        else:
            discovery_protocol = PreferredNodeDiscoveryProtocol(
                trinity_config.nodekey,
                address,
                trinity_config.bootstrap_nodes,
                trinity_config.preferred_nodes,
            )

        if is_discovery_disabled:
            discovery_service: ServiceAPI = StaticDiscoveryService(
                self.event_bus,
                trinity_config.preferred_nodes,
            )
        else:
            discovery_service = DiscoveryService(
                discovery_protocol,
                trinity_config.port,
                self.event_bus,
            )

        manager = self.manager.run_child_service(discovery_service)

        try:
            await manager.wait_stopped()
        finally:
            if manager.did_error:
                await self.event_bus.broadcast(ShutdownRequest("Discovery ended unexpectedly"))


class PeerDiscoveryComponent(BaseApplicationComponent):
    """
    Continously discover other Ethereum nodes.
    """
    name = "Discovery"

    @classmethod
    def configure_parser(cls,
                         arg_parser: ArgumentParser,
                         subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--disable-discovery",
            action="store_true",
            help="Disable peer discovery",
        )

    @property
    def is_enabled(self) -> bool:
        return True

    async def run(self) -> None:
        service = DiscoveryBootstrapService(self._boot_info, self.name)

        await run_in_process(AsyncioManager.run_service, service)
