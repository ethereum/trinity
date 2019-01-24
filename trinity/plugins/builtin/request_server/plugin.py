from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
import asyncio

from lahja import (
    Endpoint,
)
from p2p.service import (
    BaseService,
)

from trinity.config import (
    Eth1AppConfig,
    Eth1DbMode,
)
from trinity.constants import (
    TO_NETWORKING_BROADCAST_CONFIG,
)
from trinity.db.eth1.manager import (
    create_db_consumer_manager
)
from trinity.extensibility import (
    BaseIsolatedPlugin,
)
from trinity.protocol.eth.servers import (
    ETHIsolatedRequestServer
)
from trinity.protocol.les.servers import (
    LightIsolatedRequestServer,
)
from trinity._utils.shutdown import (
    exit_with_service_and_endpoint,
)


class RequestServerPlugin(BaseIsolatedPlugin):

    @property
    def name(self) -> str:
        return "Request Server"

    def on_ready(self, manager_eventbus: Endpoint) -> None:
        if not self.context.args.disable_request_server:
            self.start()

    def configure_parser(self, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--disable-request-server",
            action="store_true",
            help="Disables the Request Server",
        )

    def do_start(self) -> None:

        trinity_config = self.context.trinity_config

        db_manager = create_db_consumer_manager(trinity_config.database_ipc_path)

        eth1_app_config = trinity_config.get_app_config(Eth1AppConfig)

        if eth1_app_config.database_mode is Eth1DbMode.LIGHT:
            header_db = db_manager.get_headerdb()  # type: ignore
            server: BaseService = LightIsolatedRequestServer(
                self.event_bus,
                TO_NETWORKING_BROADCAST_CONFIG,
                header_db
            )
        elif eth1_app_config.database_mode is Eth1DbMode.FULL:
            chain_db = db_manager.get_chaindb()  # type: ignore
            server = ETHIsolatedRequestServer(
                self.event_bus,
                TO_NETWORKING_BROADCAST_CONFIG,
                chain_db
            )
        else:
            raise Exception(f"Unsupported Database Mode: {eth1_app_config.database_mode}")

        loop = asyncio.get_event_loop()
        asyncio.ensure_future(exit_with_service_and_endpoint(server, self.event_bus))
        asyncio.ensure_future(server.run())
        loop.run_forever()
        loop.close()
