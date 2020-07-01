from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
import contextlib
from typing import Iterator, Tuple, Union

from async_service import Service

from lahja import EndpointAPI

from eth.db.header import (
    HeaderDB,
)

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
from trinity.http.handlers.rpc_handler import RPCHandler
from trinity.http.main import (
    HTTPServer,
)
from trinity._utils.services import run_background_asyncio_services


@contextlib.contextmanager
def chain_for_eth1_config(trinity_config: TrinityConfig,
                          eth1_app_config: Eth1AppConfig,
                          event_bus: EndpointAPI) -> Iterator[AsyncChainAPI]:
    chain_config = eth1_app_config.get_chain_config()

    db = DBClient.connect(trinity_config.database_ipc_path)

    with db:
        if eth1_app_config.database_mode is Eth1DbMode.LIGHT:
            header_db = HeaderDB(db)
            event_bus_light_peer_chain = EventBusLightPeerChain(event_bus)
            yield chain_config.light_chain_class(
                header_db, peer_chain=event_bus_light_peer_chain
            )
        elif eth1_app_config.database_mode is Eth1DbMode.FULL:
            yield chain_config.full_chain_class(db)
        else:
            raise Exception(f"Unsupported Database Mode: {eth1_app_config.database_mode}")


@contextlib.contextmanager
def chain_for_beacon_config(trinity_config: TrinityConfig,
                            beacon_app_config: BeaconAppConfig,
                            ) -> Iterator[AsyncBeaconChainDB]:
    db = DBClient.connect(trinity_config.database_ipc_path)
    with db:
        yield AsyncBeaconChainDB(db)


@contextlib.contextmanager
def chain_for_config(trinity_config: TrinityConfig,
                     event_bus: EndpointAPI,
                     ) -> Iterator[Union[AsyncChainAPI, AsyncBeaconChainDB]]:
    if trinity_config.has_app_config(BeaconAppConfig):
        beacon_app_config = trinity_config.get_app_config(BeaconAppConfig)
        with chain_for_beacon_config(trinity_config, beacon_app_config) as beacon_chain:
            yield beacon_chain
    elif trinity_config.has_app_config(Eth1AppConfig):
        eth1_app_config = trinity_config.get_app_config(Eth1AppConfig)
        with chain_for_eth1_config(trinity_config, eth1_app_config, event_bus) as eth1_chain:
            yield eth1_chain
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
            help="Disables the JSON-RPC server",
        )
        arg_parser.add_argument(
            "--enable-http",
            action="store_true",
            help="Enables the HTTP server",
        )
        arg_parser.add_argument(
            "--http-listen-address",
            type=str,
            help="Address for the HTTP server to listen on",
            default="127.0.0.1",
        )
        arg_parser.add_argument(
            "--http-port",
            type=int,
            help="JSON-RPC server port",
            default=8545,
        )

    async def do_run(self, event_bus: EndpointAPI) -> None:
        boot_info = self._boot_info
        trinity_config = boot_info.trinity_config

        with chain_for_config(trinity_config, event_bus) as chain:
            if trinity_config.has_app_config(Eth1AppConfig):
                modules = initialize_eth1_modules(chain, event_bus, trinity_config)
            elif trinity_config.has_app_config(BeaconAppConfig):
                modules = initialize_beacon_modules(chain, event_bus)
            else:
                raise Exception("Unsupported Node Type")

            rpc = RPCServer(modules, chain, event_bus)

            # Run IPC Server
            ipc_server = IPCServer(rpc, boot_info.trinity_config.jsonrpc_ipc_path)
            services_to_exit: Tuple[Service, ...] = (
                ipc_server,
            )

            # Run HTTP Server
            if boot_info.args.enable_http:
                http_server = HTTPServer(
                    host=boot_info.args.http_listen_address,
                    handler=RPCHandler.handle(rpc.execute),
                    port=boot_info.args.http_port,
                )
                services_to_exit += (http_server,)

            await run_background_asyncio_services(services_to_exit)
