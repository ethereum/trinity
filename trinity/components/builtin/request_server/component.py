from argparse import (
    ArgumentParser,
    _SubParsersAction,
)

from async_service import Service, run_asyncio_service
from lahja import EndpointAPI

from eth.db.backends.base import BaseAtomicDB

from trinity.boot_info import BootInfo
from trinity.config import (
    Eth1AppConfig,
    Eth1DbMode,
)
from trinity.constants import (
    TO_NETWORKING_BROADCAST_CONFIG,
)
from trinity.db.manager import DBClient
from trinity.db.eth1.chain import AsyncChainDB
from trinity.db.eth1.header import AsyncHeaderDB
from trinity.extensibility import (
    AsyncioIsolatedComponent,
)
from trinity.protocol.eth.servers import ETHRequestServer
from trinity.protocol.les.servers import LightRequestServer


class RequestServerComponent(AsyncioIsolatedComponent):
    name = "Request Server"

    @property
    def is_enabled(self) -> bool:
        return not self._boot_info.args.disable_request_server

    @classmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--disable-request-server",
            action="store_true",
            help="Disables the Request Server",
        )

    @classmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        trinity_config = boot_info.trinity_config
        base_db = DBClient.connect(trinity_config.database_ipc_path)
        with base_db:
            if trinity_config.has_app_config(Eth1AppConfig):
                server = cls.make_eth1_request_server(
                    trinity_config.get_app_config(Eth1AppConfig),
                    base_db,
                    event_bus,
                )
            else:
                raise Exception("Trinity config must have eth1 config")

            await run_asyncio_service(server)

    @classmethod
    def make_eth1_request_server(cls,
                                 app_config: Eth1AppConfig,
                                 base_db: BaseAtomicDB,
                                 event_bus: EndpointAPI) -> Service:

        server: Service

        if app_config.database_mode is Eth1DbMode.LIGHT:
            header_db = AsyncHeaderDB(base_db)
            server = LightRequestServer(
                event_bus,
                TO_NETWORKING_BROADCAST_CONFIG,
                header_db
            )
        elif app_config.database_mode is Eth1DbMode.FULL:
            chain_db = AsyncChainDB(base_db)
            server = ETHRequestServer(
                event_bus,
                TO_NETWORKING_BROADCAST_CONFIG,
                chain_db
            )
        else:
            raise Exception(f"Unsupported Database Mode: {app_config.database_mode}")

        return server
