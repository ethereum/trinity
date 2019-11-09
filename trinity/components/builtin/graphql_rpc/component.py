from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
import asyncio
from typing import (
    Tuple,
)
from lahja import EndpointAPI
from p2p.service import BaseService

from trinity.config import (
    Eth1AppConfig,
)

from trinity.db.manager import DBClient
from trinity.extensibility import (
    AsyncioIsolatedComponent,
)
from trinity.rpc.http import (
    HTTPServer,
)
from trinity.rpc.ipc import (
    IPCServer,
)
from trinity._utils.shutdown import (
    exit_with_services,
)

from trinity.rpc.graph_ql.server import GraphQlServer
from trinity.rpc.json_rpc.modules import Eth1ChainRPCModule


class GraphQLRpcServerPlugin(AsyncioIsolatedComponent):

    @property
    def name(self) -> str:
        return "GRAPHQL-RPC API"

    def on_ready(self, manager_eventbus: EndpointAPI) -> None:
        if self.boot_info.args.graphql:
            self.start()

    @classmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--graphql",
            action="store_true",
            help="Enables the GRAPHQL-RPC Server",
        )
        arg_parser.add_argument(
            "--enable-graphiql",
            action="store_true",
            help="Enables the HTTP Server that can be used to serve graphiql",
        )
        arg_parser.add_argument(
            "--graphql-port",
            default=8547,
            help="Enables the GRAPHQL-RPC Server",
        )

    def do_start(self) -> None:
        trinity_config = self.boot_info.trinity_config
        eth1_app_config = trinity_config.get_app_config(Eth1AppConfig)
        chain_config = eth1_app_config.get_chain_config()

        db = DBClient.connect(trinity_config.database_ipc_path)
        chain = chain_config.full_chain_class(db)
        rpc = GraphQlServer(Eth1ChainRPCModule(chain, self.event_bus))
        ipc_server = IPCServer(rpc, self.boot_info.trinity_config.graphql_ipc_path)
        asyncio.ensure_future(ipc_server.run())
        services_to_exit: Tuple[BaseService, ...] = (
            ipc_server,
            self._event_bus_service,
        )
        if self.boot_info.args.enable_graphiql:
            http_server = HTTPServer(rpc, port=self.boot_info.args.graphql_port)
            asyncio.ensure_future(http_server.run())
            services_to_exit += (http_server,)

        asyncio.ensure_future(exit_with_services(*services_to_exit))
