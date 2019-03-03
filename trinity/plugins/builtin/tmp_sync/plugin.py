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
from trinity.sync.full.chain import (
    FastChainSyncer,
)
from trinity._utils.shutdown import (
    exit_with_service_and_endpoint,
)


# Temp plugin for easier development
class TmpSyncPlugin(BaseIsolatedPlugin):

    @property
    def name(self) -> str:
        return "TmpSync"

    def on_ready(self, manager_eventbus: Endpoint) -> None:
        if not self.context.args.disable_request_server:
            self.start()

    def configure_parser(self, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--tmp-sync",
            action="store_true",
            help="Enable futuristic isolated process sync",
        )

    def do_start(self) -> None:

        trinity_config = self.context.trinity_config

        db_manager = create_db_consumer_manager(trinity_config.database_ipc_path)

        eth1_app_config = trinity_config.get_app_config(Eth1AppConfig)
        chain_config = trinity_config.get_chain_config()


        if eth1_app_config.database_mode is Eth1DbMode.LIGHT:
            header_db = db_manager.get_headerdb()  # type: ignore
            server: BaseService = LightIsolatedRequestServer(
                self.event_bus,
                TO_NETWORKING_BROADCAST_CONFIG,
                header_db
            )
        elif eth1_app_config.database_mode is Eth1DbMode.FULL:
            chain_db = db_manager.get_chaindb()  # type: ignore
            db = db_manager.get_db()
            chain_db = db_manager.get_chaindb()
            chain = chain_config.full_chain_class(db)
            syncer = FastChainSyncer(chain, chain_db, self.event_bus)
        else:
            raise Exception(f"Unsupported Database Mode: {eth1_app_config.database_mode}")

        loop = asyncio.get_event_loop()
        asyncio.ensure_future(exit_with_service_and_endpoint(syncer, self.event_bus))
        asyncio.ensure_future(syncer.run())
        loop.run_forever()
        loop.close()
