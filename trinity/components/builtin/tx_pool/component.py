from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
import logging

from asyncio_run_in_process import run_in_process

from eth.chains.mainnet import (
    PETERSBURG_MAINNET_BLOCK,
)
from eth.chains.ropsten import (
    PETERSBURG_ROPSTEN_BLOCK,
)

from p2p.service import AsyncioManager

from trinity.config import (
    Eth1AppConfig,
)
from trinity.constants import (
    SYNC_LIGHT,
    TO_NETWORKING_BROADCAST_CONFIG,
    MAINNET_NETWORK_ID,
    ROPSTEN_NETWORK_ID,
)
from trinity.db.manager import DBClient
from trinity.extensibility import (
    BaseApplicationComponent,
    ComponentService,
)
from trinity.components.builtin.tx_pool.pool import (
    TxPool,
)
from trinity.components.builtin.tx_pool.validators import (
    DefaultTransactionValidator
)
from trinity.protocol.eth.peer import ETHProxyPeerPool


class TxComponent(BaseApplicationComponent):
    tx_pool: TxPool = None
    name = "TxComponent"

    logger = logging.getLogger('trinity.components.TxComponent')

    @classmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--disable-tx-pool",
            action="store_true",
            help="Disables the Transaction Pool",
        )

    @property
    def is_enabled(self) -> bool:
        light_mode = self._boot_info.args.sync_mode == SYNC_LIGHT
        is_disable = self._boot_info.args.disable_tx_pool
        is_supported = not light_mode
        is_enabled = not is_disable and is_supported
        if not is_supported:
            self.logger.warning("Transaction pool disabled.  Not supported in light mode.")
        return is_enabled

    async def run(self) -> None:
        service = TxPoolService(self._boot_info, self.name)

        await run_in_process(AsyncioManager.run_service, service)


class TxPoolService(ComponentService):
    async def run_component_service(self) -> None:
        trinity_config = self.boot_info.trinity_config
        db = DBClient.connect(trinity_config.database_ipc_path)

        app_config = trinity_config.get_app_config(Eth1AppConfig)
        chain_config = app_config.get_chain_config()

        chain = chain_config.full_chain_class(db)

        if self.boot_info.trinity_config.network_id == MAINNET_NETWORK_ID:
            validator = DefaultTransactionValidator(chain, PETERSBURG_MAINNET_BLOCK)
        elif self.boot_info.trinity_config.network_id == ROPSTEN_NETWORK_ID:
            validator = DefaultTransactionValidator(chain, PETERSBURG_ROPSTEN_BLOCK)
        else:
            raise ValueError("The TxPool component only supports MainnetChain or RopstenChain")

        proxy_peer_pool = ETHProxyPeerPool(self.event_bus, TO_NETWORKING_BROADCAST_CONFIG)

        tx_pool = TxPool(self.event_bus, proxy_peer_pool, validator)
        self.manager.run_daemon_child_service(tx_pool)
