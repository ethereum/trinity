from typing import Iterator, Type
import contextlib

from eth.abc import ChainAPI

from lahja import EndpointAPI
from trinity.boot_info import BootInfo
from trinity.db.eth1.header import AsyncHeaderDB
from trinity.chains.light_eventbus import (
    EventBusLightPeerChain,
)
from trinity.config import TAppConfig
from trinity.constants import (
    SYNC_LIGHT,
)
from trinity.db.manager import DBClient


@contextlib.contextmanager
def get_chain(app_config_type: Type[TAppConfig],
              boot_info: BootInfo,
              event_bus: EndpointAPI) -> Iterator[ChainAPI]:
    app_config = boot_info.trinity_config.get_app_config(app_config_type)
    # ignore b/c TAppConfig has no attribute get_chain_config
    chain_config = app_config.get_chain_config()  # type: ignore

    chain: ChainAPI
    base_db = DBClient.connect(boot_info.trinity_config.database_ipc_path)
    with base_db:
        if boot_info.args.sync_mode == SYNC_LIGHT:
            header_db = AsyncHeaderDB(base_db)
            chain = chain_config.light_chain_class(
                header_db,
                peer_chain=EventBusLightPeerChain(event_bus)
            )
        else:
            chain = chain_config.full_chain_class(base_db)

        yield chain
