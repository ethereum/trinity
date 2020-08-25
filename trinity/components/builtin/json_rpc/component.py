from argparse import (
    Action,
    ArgumentParser,
    Namespace,
    _SubParsersAction,
)
import contextlib
from typing import Iterator, Tuple, Sequence, Type, Any

from async_service import Service
from eth_utils import ValidationError, to_tuple

from lahja import EndpointAPI

from eth.db.header import (
    HeaderDB,
)

from trinity.config import (
    Eth1AppConfig,
    Eth1DbMode,
    TrinityConfig
)
from trinity.chains.base import AsyncChainAPI
from trinity.chains.light_eventbus import (
    EventBusLightPeerChain,
)
from trinity.db.manager import DBClient
from trinity.extensibility import (
    AsyncioIsolatedComponent,
)
from trinity.rpc.main import (
    RPCServer,
)
from trinity.rpc.modules import (
    BaseRPCModule,
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
def chain_for_config(trinity_config: TrinityConfig,
                     event_bus: EndpointAPI,
                     ) -> Iterator[AsyncChainAPI]:
    if trinity_config.has_app_config(Eth1AppConfig):
        eth1_app_config = trinity_config.get_app_config(Eth1AppConfig)
        with chain_for_eth1_config(trinity_config, eth1_app_config, event_bus) as eth1_chain:
            yield eth1_chain
    else:
        raise Exception("Unsupported Node Type")


ALLOW_ALL_MODULES: Tuple[str, ...] = ('*',)


class NormalizeRPCModulesConfig(Action):
    def __call__(self,
                 parser: ArgumentParser,
                 namespace: Namespace,
                 value: Any,
                 option_string: str = None) -> None:

        normalized_str = value.lower().strip()

        if normalized_str == '*':
            parsed = ALLOW_ALL_MODULES
        else:
            parsed = tuple(module_name.strip() for module_name in normalized_str.split(','))
        setattr(namespace, self.dest, parsed)


@to_tuple
def get_http_enabled_modules(
        enabled_modules: Sequence[str],
        available_modules: Sequence[BaseRPCModule]) -> Iterator[Type[BaseRPCModule]]:
    all_module_types = set(type(mod) for mod in available_modules)

    if enabled_modules == ALLOW_ALL_MODULES:
        yield from all_module_types
    else:
        for module_name in enabled_modules:
            match = tuple(
                mod for mod in available_modules if mod.get_name() == module_name
            )
            if len(match) == 0:
                raise ValidationError(f"Unknown module {module_name}")
            elif len(match) > 1:
                raise ValidationError(
                    f"Invalid, {match} all share identifier {module_name}"
                )
            else:
                yield type(match[0])


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
            "--enable-http-apis",
            type=str,
            action=NormalizeRPCModulesConfig,
            default="",
            help=(
                "Enable HTTP access to specified JSON-RPC APIs (e.g. 'eth,net'). "
                "Use '*' to enable HTTP access to all modules (including eth_admin)."
            )
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
            else:
                raise Exception("Unsupported Node Type")

            rpc = RPCServer(modules, chain, event_bus)

            # Run IPC Server
            ipc_server = IPCServer(rpc, boot_info.trinity_config.jsonrpc_ipc_path)
            services_to_exit: Tuple[Service, ...] = (
                ipc_server,
            )
            try:
                http_modules = get_http_enabled_modules(boot_info.args.enable_http_apis, modules)
            except ValidationError as error:
                self.logger.error(error)
                return

            # Run HTTP Server if there are http enabled APIs
            if len(http_modules) > 0:
                enabled_module_names = tuple(mod.get_name() for mod in http_modules)
                self.logger.info("JSON-RPC modules exposed via HTTP: %s", enabled_module_names)
                non_http_modules = set(type(mod) for mod in modules) - set(http_modules)
                exec = rpc.execute_with_access_control(non_http_modules)
                http_server = HTTPServer(
                    host=boot_info.args.http_listen_address,
                    handler=RPCHandler.handle(exec),
                    port=boot_info.args.http_port,
                )
                services_to_exit += (http_server,)

            await run_background_asyncio_services(services_to_exit)
