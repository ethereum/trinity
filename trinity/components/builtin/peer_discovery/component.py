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

from async_service import run_trio_service, ServiceAPI

from lahja import EndpointAPI

from ddht.boot_info import BootInfo
from ddht.constants import ProtocolVersion
from ddht.xdg import get_xdg_ddht_root
from ddht.v5.constants import DEFAULT_BOOTNODES
from eth_enr import ENRDB, ENR
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

from trinity.config import Eth1AppConfig
from trinity.db.eth1.header import BaseAsyncHeaderDB
from trinity.db.manager import DBClient
from trinity.db.eth1.header import TrioHeaderDB
from trinity.extensibility import (
    TrioIsolatedComponent,
)
from trinity.protocol.eth import forkid

from .discv5 import DiscoveryV5Service


DEFAULT_DISCV5_PORT = 30304


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
            "--disable-discv4",
            action="store_true",
            help="Disable peer discovery",
        )
        arg_parser.add_argument(
            "--enable-discv5",
            action="store_true",
            help="Enable v5 peer discovery",
        )
        arg_parser.add_argument(
            "--discv5-port",
            type=int,
            help="The port number that should be used for discovery v5",
        )

    async def do_run(self, event_bus: EndpointAPI) -> None:
        boot_info = self._boot_info
        config = boot_info.trinity_config
        db = DBClient.connect(config.database_ipc_path)

        discv4_service: ServiceAPI

        if boot_info.args.disable_discv4:
            discv4_service = StaticDiscoveryService(
                event_bus,
                config.preferred_nodes,
            )
        else:
            vm_config = config.get_app_config(Eth1AppConfig).get_chain_config().vm_configuration
            headerdb = TrioHeaderDB(db)
            eth_cap_provider = functools.partial(generate_eth_cap_enr_field, vm_config, headerdb)
            socket = trio.socket.socket(family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM)
            await socket.bind(("0.0.0.0", config.port))
            base_db = LevelDB(config.enr_db_dir)
            enr_db = ENRDB(base_db)
            discv4_service = PreferredNodeDiscoveryService(
                config.nodekey,
                config.port,
                config.port,
                config.bootstrap_nodes,
                config.preferred_nodes,
                event_bus,
                socket,
                enr_db,
                (eth_cap_provider,),
            )

        with db:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(run_trio_service, discv4_service)
                if self._boot_info.args.enable_discv5:
                    discv5_service = self.get_discv5_service(event_bus)
                    nursery.start_soon(run_trio_service, discv5_service)

    def get_discv5_service(self, event_bus: EndpointAPI) -> ServiceAPI:
        base_dir = get_xdg_ddht_root()

        boot_info = BootInfo(
            protocol_version=ProtocolVersion.v5,
            base_dir=base_dir,
            port=self._boot_info.args.discv5_port or DEFAULT_DISCV5_PORT,
            listen_on=None,
            bootnodes=tuple(ENR.from_repr(enr) for enr in DEFAULT_BOOTNODES),
            private_key=self._boot_info.trinity_config.nodekey,
            is_ephemeral=False,
            is_upnp_enabled=True,
        )
        service = DiscoveryV5Service(
            event_bus=event_bus,
            trinity_config=self._boot_info.trinity_config,
            boot_info=boot_info,
        )
        return service


async def generate_eth_cap_enr_field(
        vm_config: Tuple[Tuple[BlockNumber, Type[VirtualMachineAPI]], ...],
        headerdb: BaseAsyncHeaderDB,
) -> Tuple[Literal[b'eth'], Tuple[bytes, bytes]]:
    head = await headerdb.coro_get_canonical_head()
    genesis_hash = await headerdb.coro_get_canonical_block_hash(GENESIS_BLOCK_NUMBER)
    fork_blocks = forkid.extract_fork_blocks(vm_config)
    our_forkid = forkid.make_forkid(genesis_hash, head.block_number, fork_blocks)
    return (b'eth', sedes.List([forkid.ForkID]).serialize([our_forkid]))


if __name__ == "__main__":
    from trinity.extensibility.component import run_trio_eth1_component
    run_trio_eth1_component(PeerDiscoveryComponent)
