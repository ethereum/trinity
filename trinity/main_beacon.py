import logging
from typing import (
    Type,
)

from eth.db.backends.level import (
    LevelDB,
)

from eth2.beacon.db.chain import BeaconChainDB
from eth2.beacon.types.blocks import BeaconBlock

from trinity.bootstrap import (
    main_entry,
)
from trinity.config import (
    BeaconAppConfig
)
from trinity.constants import (
    APP_IDENTIFIER_BEACON,
)
from trinity.db.manager import DBManager
from trinity.initialization import (
    ensure_beacon_dirs,
    initialize_beacon_database,
    is_beacon_database_initialized,
)
from trinity.main import TrinityMain
from trinity.components.registry import (
    get_components_for_beacon_client,
)
from trinit.boot_info import TrinityBootInfo
from trinity._utils.logging import (
    setup_log_levels,
    setup_queue_logging,
)


def main_beacon() -> None:
    main_entry(
        BeaconMain,
        APP_IDENTIFIER_BEACON,
        get_components_for_beacon_client(),
        (BeaconAppConfig,)
    )


def run_database_process(boot_info: TrinityBootInfo, db_class: Type[LevelDB]) -> None:
    # setup cross process logging
    log_queue = boot_info.log_queue
    level = boot_info.log_level or logging.INFO
    setup_queue_logging(log_queue, level)
    if boot_info.args.log_levels:
        setup_log_levels(boot_info.args.log_levels)

    trinity_config = boot_info.trinity_config

    with trinity_config.process_id_file('database'):
        app_config = trinity_config.get_app_config(BeaconAppConfig)
        chain_config = app_config.get_chain_config()

        base_db = db_class(db_path=app_config.database_dir)
        chaindb = BeaconChainDB(base_db, chain_config.genesis_config)

        if not is_beacon_database_initialized(chaindb, BeaconBlock):
            initialize_beacon_database(chain_config, chaindb, base_db, BeaconBlock)

        manager = DBManager(base_db)
        with manager.run(trinity_config.database_ipc_path):
            try:
                manager.wait_stopped()
            except KeyboardInterrupt:
                pass


class BeaconMain(TrinityMain):
    run_database_process = staticmethod(run_database_process)

    def ensure_dirs(self) -> None:
        ensure_beacon_dirs(self.boot_info.trinity_config.get_app_config(BeaconAppConfig))
