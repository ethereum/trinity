from eth.db.backends.level import LevelDB
from eth2.beacon.db.chain import BeaconChainDB

from trinity.bootstrap import (
    main_entry,
)
from trinity.boot_info import BootInfo
from trinity.config import (
    BeaconAppConfig
)
from trinity.constants import (
    APP_IDENTIFIER_BEACON,
)
from trinity.initialization import (
    ensure_beacon_dirs,
    initialize_beacon_database,
    is_beacon_database_initialized,
)
from trinity.components.registry import (
    get_components_for_beacon_client,
)


def main_beacon() -> None:
    main_entry(
        trinity_boot,
        get_base_db,
        APP_IDENTIFIER_BEACON,
        get_components_for_beacon_client(),
        (BeaconAppConfig,)
    )


def get_base_db(boot_info: BootInfo) -> LevelDB:
    app_config = boot_info.trinity_config.get_app_config(BeaconAppConfig)
    chain_config = app_config.get_chain_config()
    base_db = LevelDB(db_path=app_config.database_dir)
    chaindb = BeaconChainDB(base_db)
    if not is_beacon_database_initialized(chaindb):
        initialize_beacon_database(chain_config, chaindb, base_db)
    return base_db


def trinity_boot(boot_info: BootInfo) -> None:
    trinity_config = boot_info.trinity_config
    ensure_beacon_dirs(trinity_config.get_app_config(BeaconAppConfig))
