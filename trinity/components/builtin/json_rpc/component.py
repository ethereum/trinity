from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
from typing import Union

from eth.db.header import (
    HeaderDB,
)

from asyncio_run_in_process import run_in_process

from p2p.service import AsyncioManager

from trinity.config import (
    Eth1AppConfig,
    Eth1DbMode,
    BeaconAppConfig,
    TrinityConfig
)
from trinity.chains.base import AsyncChainAPI
from trinity.chains.light_eventbus import (
    EventBusLightPeerChain,
)
from trinity.db.beacon.chain import AsyncBeaconChainDB
from trinity.db.manager import DBClient
from trinity.extensibility import (
    BaseApplicationComponent,
    ComponentService,
)
from trinity.rpc.main import (
    RPCServer,
)
from trinity.rpc.modules import (
    initialize_beacon_modules,
    initialize_eth1_modules,
)
from trinity.rpc.ipc import (
    IPCServer,
)
from trinity.rpc.http import (
    HTTPServer,
)


class JsonRpcServerComponent(BaseApplicationComponent):
    name = "JSON-RPC API"

    @property
    def is_enabled(self) -> bool:
        return not self._boot_info.args.disable_rpc

    @classmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--disable-rpc",
            action="store_true",
            help="Disables the JSON-RPC Server",
        )
        arg_parser.add_argument(
            "--enable-http",
            action="store_true",
            help="Enables the HTTP Server",
        )
        arg_parser.add_argument(
            "--rpcport",
            type=int,
            help="JSON-RPC server port",
            default=8545,
        )

    async def run(self) -> None:
        service = JSONRPCService(self._boot_info, self.name)

        await run_in_process(AsyncioManager.run_service, service)


class JSONRPCService(ComponentService):
    def _chain_for_eth1_config(self, trinity_config: TrinityConfig,
                               eth1_app_config: Eth1AppConfig) -> AsyncChainAPI:
        chain_config = eth1_app_config.get_chain_config()

        db = DBClient.connect(trinity_config.database_ipc_path)

        if eth1_app_config.database_mode is Eth1DbMode.LIGHT:
            header_db = HeaderDB(db)
            event_bus_light_peer_chain = EventBusLightPeerChain(self.event_bus)
            return chain_config.light_chain_class(
                header_db, peer_chain=event_bus_light_peer_chain
            )
        elif eth1_app_config.database_mode is Eth1DbMode.FULL:
            return chain_config.full_chain_class(db)
        else:
            raise Exception(f"Unsupported Database Mode: {eth1_app_config.database_mode}")

    def _chain_for_beacon_config(self, trinity_config: TrinityConfig,
                                 beacon_app_config: BeaconAppConfig) -> AsyncBeaconChainDB:
        chain_config = beacon_app_config.get_chain_config()
        db = DBClient.connect(trinity_config.database_ipc_path)
        return AsyncBeaconChainDB(db, chain_config.genesis_config)

    def _chain_for_config(
        self,
        trinity_config: TrinityConfig
    ) -> Union[AsyncChainAPI, AsyncBeaconChainDB]:
        if trinity_config.has_app_config(BeaconAppConfig):
            beacon_app_config = trinity_config.get_app_config(BeaconAppConfig)
            return self._chain_for_beacon_config(trinity_config, beacon_app_config)
        elif trinity_config.has_app_config(Eth1AppConfig):
            eth1_app_config = trinity_config.get_app_config(Eth1AppConfig)
            return self._chain_for_eth1_config(trinity_config, eth1_app_config)
        else:
            raise Exception("Unsupported Node Type")

    async def run_component_service(self) -> None:
        trinity_config = self.boot_info.trinity_config
        chain = self._chain_for_config(trinity_config)

        if trinity_config.has_app_config(Eth1AppConfig):
            modules = initialize_eth1_modules(chain, self.event_bus)
        elif trinity_config.has_app_config(BeaconAppConfig):
            modules = initialize_beacon_modules(chain, self.event_bus)
        else:
            raise Exception("Unsupported Node Type")

        rpc = RPCServer(modules, chain, self.event_bus)

        # Run IPC Server
        ipc_server = IPCServer(rpc, self.boot_info.trinity_config.jsonrpc_ipc_path)

        self.manager.run_child_service(ipc_server)

        # Run HTTP Server
        if self.boot_info.args.enable_http:
            http_server = HTTPServer(rpc, port=self.boot_info.args.rpcport)
            self.manager.run_child_service(http_server)

        await self.manager.wait_forever()
