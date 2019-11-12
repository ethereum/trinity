import asyncio
import logging
from typing import (
    Tuple,
    Type,
)

from eth_utils import get_extended_debug_logger

from asyncio_run_in_process import open_in_process

from eth.db.backends.level import LevelDB
from eth.db.chain import ChainDB

from p2p.service import Service

from trinity.bootstrap import (
    main_entry,
)
from trinity.config import (
    Eth1AppConfig,
)
from trinity.constants import (
    APP_IDENTIFIER_ETH1,
)
from trinity.db.manager import DBManager
from trinity.initialization import (
    is_database_initialized,
    initialize_database,
    ensure_eth1_dirs,
)
from trinity.components.registry import (
    get_components_for_eth1_client,
)
from trinity.event_bus import ComponentManager
from trinity.extensibility import TrinityBootInfo, ApplicationComponentAPI
from trinity._utils.ipc import (
    wait_for_ipc,
)
from trinity._utils.logging import (
    setup_log_levels,
    setup_queue_logging,
)


def main() -> None:
    main_entry(
        TrinityMain,
        APP_IDENTIFIER_ETH1,
        get_components_for_eth1_client(),
        (Eth1AppConfig,)
    )


async def run_database_process(boot_info: TrinityBootInfo, db_class: Type[LevelDB]) -> None:
    # setup cross process logging
    log_queue = boot_info.log_queue
    level = boot_info.log_level or logging.INFO
    setup_queue_logging(log_queue, level)
    if boot_info.args.log_levels:
        setup_log_levels(boot_info.args.log_levels)

    trinity_config = boot_info.trinity_config

    loop = asyncio.get_event_loop()

    with trinity_config.process_id_file('database'):
        app_config = trinity_config.get_app_config(Eth1AppConfig)

        base_db = db_class(db_path=app_config.database_dir)
        chaindb = ChainDB(base_db)

        if not is_database_initialized(chaindb):
            chain_config = app_config.get_chain_config()
            initialize_database(chain_config, chaindb, base_db)

        manager = DBManager(base_db)
        with manager.run(trinity_config.database_ipc_path):
            try:
                await loop.run_in_executor(None, manager.wait_stopped)
            except KeyboardInterrupt:
                pass


class TrinityMain(Service):
    logger = get_extended_debug_logger('trinity.TrinityMain')

    def __init__(self,
                 boot_info: TrinityBootInfo,
                 component_types: Tuple[Type[ApplicationComponentAPI], ...],
                 listener: logging.handlers.QueueListener) -> None:
        self.boot_info = boot_info
        self.component_types = component_types
        self.listener = listener

    def ensure_dirs(self) -> None:
        ensure_eth1_dirs(self.boot_info.trinity_config.get_app_config(Eth1AppConfig))

    run_database_process = staticmethod(run_database_process)

    async def run(self) -> None:
        trinity_config = self.boot_info.trinity_config

        loop = asyncio.get_event_loop()

        self.logger.debug("Starting logging listener")
        # start the listener thread which handles logs produced in other
        # processes in the local logger.
        self.listener.start()

        self.ensure_dirs()

        import multiprocessing
        multiprocessing.set_start_method('spawn')

        async with open_in_process(self.run_database_process, self.boot_info, LevelDB) as db_proc:
            self.logger.debug("started database process")
            await loop.run_in_executor(None, wait_for_ipc, trinity_config.database_ipc_path)
            self.logger.debug("database process IPC path available")

            component_manager_service = ComponentManager(
                self.boot_info,
                self.component_types,
            )
            self.logger.debug("running component manager")
            manager = self.manager.run_child_service(component_manager_service)
            try:
                await manager.wait_forever()
            finally:
                db_proc.terminate()
                await db_proc.wait()
