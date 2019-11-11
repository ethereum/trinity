from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
from eth.db.backends.base import BaseAtomicDB

from asyncio_run_in_process import run_in_process

from p2p.service import AsyncioManager, ServiceAPI, background_asyncio_service

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
from trinity.extensibility import BaseApplicationComponent, ComponentService
from trinity.protocol.eth.servers import ETHRequestServer
from trinity.protocol.les.servers import LightRequestServer


class RequestServerComponent(BaseApplicationComponent):
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

    async def run(self) -> None:
        service = RequestServerService(self._boot_info, self.name)

        await run_in_process(AsyncioManager.run_service, service)


class RequestServerService(ComponentService):
    async def run_component_service(self) -> None:
        trinity_config = self.boot_info.trinity_config
        base_db = DBClient.connect(trinity_config.database_ipc_path)

        if trinity_config.has_app_config(Eth1AppConfig):
            server = self._make_eth1_request_server(
                trinity_config.get_app_config(Eth1AppConfig),
                base_db,
            )
        else:
            raise Exception("Trinity config must have eth1 config")

        async with background_asyncio_service(server) as manager:
            await manager.wait_forever()

    def _make_eth1_request_server(self,
                                  app_config: Eth1AppConfig,
                                  base_db: BaseAtomicDB) -> ServiceAPI:

        server: ServiceAPI

        if app_config.database_mode is Eth1DbMode.LIGHT:
            header_db = AsyncHeaderDB(base_db)
            server = LightRequestServer(
                self.event_bus,
                TO_NETWORKING_BROADCAST_CONFIG,
                header_db
            )
        elif app_config.database_mode is Eth1DbMode.FULL:
            chain_db = AsyncChainDB(base_db)
            server = ETHRequestServer(
                self.event_bus,
                TO_NETWORKING_BROADCAST_CONFIG,
                chain_db
            )
        else:
            raise Exception(f"Unsupported Database Mode: {app_config.database_mode}")

        return server
