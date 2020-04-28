from eth.db.backends.level import LevelDB
from eth.db.chain import ChainDB

from trinity.boot_info import BootInfo
from trinity.bootstrap import (
    main_entry,
)
from trinity.config import (
    Eth1AppConfig,
)
from trinity.constants import (
    APP_IDENTIFIER_ETH1,
)
from trinity.components.registry import (
    get_components_for_eth1_client,
)
from trinity.initialization import (
    ensure_eth1_dirs,
    initialize_database,
    is_database_initialized,
)


def main() -> None:
    main_entry(
        trinity_boot,
        get_base_db,
        APP_IDENTIFIER_ETH1,
        get_components_for_eth1_client(),
        (Eth1AppConfig,)
    )


def get_base_db(boot_info: BootInfo) -> LevelDB:
    app_config = boot_info.trinity_config.get_app_config(Eth1AppConfig)
    base_db = LevelDB(db_path=app_config.database_dir)
    chaindb = ChainDB(base_db)
    if not is_database_initialized(chaindb):
        chain_config = app_config.get_chain_config()
        initialize_database(chain_config, chaindb, base_db)
    return base_db


def trinity_boot(boot_info: BootInfo) -> None:
    trinity_config = boot_info.trinity_config
    ensure_eth1_dirs(trinity_config.get_app_config(Eth1AppConfig))
