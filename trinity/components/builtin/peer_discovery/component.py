from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
import functools
from typing import (
    Tuple,
    Type,
)
from typing_extensions import Literal

import trio

from rlp import sedes

import async_service

from lahja import EndpointAPI

from eth_typing import BlockNumber

from eth.abc import VirtualMachineAPI
from eth.constants import GENESIS_BLOCK_NUMBER
from eth.db.backends.level import LevelDB

from p2p.constants import (
    DISCOVERY_EVENTBUS_ENDPOINT,
)
from p2p.discovery import (
    PreferredNodeDiscoveryService,
    StaticDiscoveryService,
)
from p2p.discv5.enr_db import NodeDB
from p2p.discv5.identity_schemes import default_identity_scheme_registry

from trinity.boot_info import BootInfo
from trinity.config import Eth1AppConfig
from trinity.db.eth1.header import BaseAsyncHeaderDB
from trinity.db.manager import DBClient
from trinity.db.eth1.header import TrioHeaderDB
from trinity.events import ShutdownRequest
from trinity.extensibility import (
    TrioIsolatedComponent,
)
from trinity.protocol.eth import forkid


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

        if boot_info.args.disable_discovery:
            discovery_service: async_service.Service = StaticDiscoveryService(
                event_bus,
                config.preferred_nodes,
            )
        else:
            vm_config = config.get_app_config(Eth1AppConfig).get_chain_config().vm_configuration
            headerdb = TrioHeaderDB(db)
            eth_cap_provider = functools.partial(generate_eth_cap_enr_field, vm_config, headerdb)
            socket = trio.socket.socket(family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM)
            await socket.bind(("0.0.0.0", config.port))
            base_db = LevelDB(config.node_db_dir)
            node_db = NodeDB(default_identity_scheme_registry, base_db)
            discovery_service = PreferredNodeDiscoveryService(
                config.nodekey,
                config.port,
                config.port,
                config.bootstrap_nodes,
                config.preferred_nodes,
                event_bus,
                socket,
                node_db,
                (eth_cap_provider,),
            )

        try:
            with db:
                await async_service.run_trio_service(discovery_service)
        except Exception:
            await event_bus.broadcast(ShutdownRequest("Discovery ended unexpectedly"))
            raise


async def generate_eth_cap_enr_field(
        vm_config: Tuple[Tuple[BlockNumber, Type[VirtualMachineAPI]], ...],
        headerdb: BaseAsyncHeaderDB,
) -> Tuple[Literal[b'eth'], Tuple[bytes, bytes]]:
    head = await headerdb.coro_get_canonical_head()
    genesis_hash = await headerdb.coro_get_canonical_block_hash(GENESIS_BLOCK_NUMBER)
    fork_blocks = forkid.extract_fork_blocks(vm_config)
    our_forkid = forkid.make_forkid(genesis_hash, head.block_number, fork_blocks)
    return (b'eth', sedes.List([forkid.ForkID]).serialize([our_forkid]))


async def main() -> None:
    from trinity.extensibility.component import run_standalone_eth1_component
    await run_standalone_eth1_component(PeerDiscoveryComponent)


if __name__ == "__main__":
    trio.run(main)
