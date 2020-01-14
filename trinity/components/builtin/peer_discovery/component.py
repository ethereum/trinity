from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
import functools

import trio

import async_service

from lahja import EndpointAPI

from p2p.constants import (
    DISCOVERY_EVENTBUS_ENDPOINT,
)
from p2p.discovery import (
    generate_eth_cap_enr_field,
    PreferredNodeDiscoveryService,
    StaticDiscoveryService,
)
from p2p.discv5.enr_db import FileEnrDb
from p2p.discv5.identity_schemes import default_identity_scheme_registry
from p2p.kademlia import (
    Address,
)

from trinity.boot_info import BootInfo
from trinity.config import Eth1AppConfig
from trinity.db.manager import DBClient
from trinity.db.eth1.header import TrioHeaderDB
from trinity.events import ShutdownRequest
from trinity.extensibility import (
    TrioIsolatedComponent,
)


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
        db = DBClient.connect(config.database_ipc_path)
        external_ip = "0.0.0.0"
        address = Address(external_ip, config.port, config.port)

        if boot_info.args.disable_discovery:
            discovery_service: async_service.Service = StaticDiscoveryService(
                event_bus,
                config.preferred_nodes,
            )
        else:
            vm_config = config.get_app_config(Eth1AppConfig).get_chain_config().vm_configuration
            headerdb = TrioHeaderDB(db)
            eth_cap_provider = functools.partial(generate_eth_cap_enr_field, vm_config, headerdb)
            external_ip = "0.0.0.0"
            socket = trio.socket.socket(family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM)
            enr_db = FileEnrDb(default_identity_scheme_registry, config.enr_db_dir)
            await socket.bind((external_ip, config.port))
            discovery_service = PreferredNodeDiscoveryService(
                config.nodekey,
                address,
                config.bootstrap_nodes,
                config.preferred_nodes,
                event_bus,
                socket,
                enr_db,
                (eth_cap_provider,),
            )

        try:
            with db:
                await async_service.run_trio_service(discovery_service)
        except Exception:
            await event_bus.broadcast(ShutdownRequest("Discovery ended unexpectedly"))
            raise
