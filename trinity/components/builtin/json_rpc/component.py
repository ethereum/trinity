from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
from typing import Union, Tuple

from async_exit_stack import AsyncExitStack

from lahja import EndpointAPI

from eth.db.header import (
    HeaderDB,
)

from p2p.service import run_service

from trinity.boot_info import BootInfo
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
    AsyncioIsolatedComponent,
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
from p2p.service import BaseService


def chain_for_eth1_config(trinity_config: TrinityConfig,
                          eth1_app_config: Eth1AppConfig,
                          event_bus: EndpointAPI) -> AsyncChainAPI:
    chain_config = eth1_app_config.get_chain_config()

    db = DBClient.connect(trinity_config.database_ipc_path)

    if eth1_app_config.database_mode is Eth1DbMode.LIGHT:
        header_db = HeaderDB(db)
        event_bus_light_peer_chain = EventBusLightPeerChain(event_bus)
        return chain_config.light_chain_class(
            header_db, peer_chain=event_bus_light_peer_chain
        )
    elif eth1_app_config.database_mode is Eth1DbMode.FULL:
        return chain_config.full_chain_class(db)
    else:
        raise Exception(f"Unsupported Database Mode: {eth1_app_config.database_mode}")


def chain_for_beacon_config(trinity_config: TrinityConfig,
                            beacon_app_config: BeaconAppConfig) -> AsyncBeaconChainDB:
    chain_config = beacon_app_config.get_chain_config()
    db = DBClient.connect(trinity_config.database_ipc_path)
    return AsyncBeaconChainDB(db, chain_config.genesis_config)


def chain_for_config(trinity_config: TrinityConfig,
                     event_bus: EndpointAPI,
                     ) -> Union[AsyncChainAPI, AsyncBeaconChainDB]:
    if trinity_config.has_app_config(BeaconAppConfig):
        beacon_app_config = trinity_config.get_app_config(BeaconAppConfig)
        return chain_for_beacon_config(trinity_config, beacon_app_config)
    elif trinity_config.has_app_config(Eth1AppConfig):
        eth1_app_config = trinity_config.get_app_config(Eth1AppConfig)
        return chain_for_eth1_config(trinity_config, eth1_app_config, event_bus)
    else:
        raise Exception("Unsupported Node Type")


class JsonRpcServerComponent(AsyncioIsolatedComponent):
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

    @classmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        trinity_config = boot_info.trinity_config

        chain = chain_for_config(trinity_config, event_bus)

        if trinity_config.has_app_config(Eth1AppConfig):
            modules = initialize_eth1_modules(chain, event_bus)
        elif trinity_config.has_app_config(BeaconAppConfig):
            modules = initialize_beacon_modules(chain, event_bus)
        else:
            raise Exception("Unsupported Node Type")

        rpc = RPCServer(modules, chain, event_bus)

        # Run IPC Server
        ipc_server = IPCServer(rpc, boot_info.trinity_config.jsonrpc_ipc_path)
        services_to_exit: Tuple[BaseService, ...] = (
            ipc_server,
        )

        # Run HTTP Server
        if boot_info.args.enable_http:
            http_server = HTTPServer(rpc, port=boot_info.args.rpcport)
            services_to_exit += (http_server,)

        async with AsyncExitStack() as stack:
            for service in services_to_exit:
                await stack.enter_async_context(run_service(service))
            await ipc_server.cancellation()
