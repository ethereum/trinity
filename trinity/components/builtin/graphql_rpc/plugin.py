from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
import asyncio

from lahja import EndpointAPI

from eth.db.chain import (
    ChainDB,
)

from trinity.config import (
    Eth1AppConfig,
)
from trinity.chains.light_eventbus import (
    EventBusLightPeerChain,
)
from trinity.db.eth1.manager import (
    create_db_consumer_manager
)
from trinity.extensibility import (
    AsyncioIsolatedPlugin,
)
from trinity.rpc.ipc import (
    IPCServer,
)
from trinity._utils.shutdown import (
    exit_with_services,
)

from trinity.graph_ql.server import GraphQlServer


class GraphQLRpcServerPlugin(AsyncioIsolatedPlugin):

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

    def do_start(self) -> None:
        trinity_config = self.boot_info.trinity_config

        db_manager = create_db_consumer_manager(trinity_config.database_ipc_path)

        eth1_app_config = trinity_config.get_app_config(Eth1AppConfig)
        chain_config = eth1_app_config.get_chain_config()

        db = db_manager.get_db()  # type: ignore
        chain = chain_config.full_chain_class(db)
        rpc = GraphQlServer(chain)
        ipc_server = IPCServer(rpc, self.boot_info.trinity_config.graphql_ipc_path)
        asyncio.ensure_future(exit_with_services(
            ipc_server,
            self._event_bus_service,
        ))
        asyncio.ensure_future(ipc_server.run())
