from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
import logging

from async_service import run_asyncio_service
from eth_utils import ValidationError
from eth.chains.mainnet import ISTANBUL_MAINNET_BLOCK
from eth.chains.ropsten import ISTANBUL_ROPSTEN_BLOCK
from eth.chains.goerli import ISTANBUL_GOERLI_BLOCK
from lahja import EndpointAPI

from trinity.boot_info import BootInfo
from trinity.config import (
    Eth1AppConfig,
)
from trinity.constants import (
    SYNC_LIGHT,
    TO_NETWORKING_BROADCAST_CONFIG,
    GOERLI_NETWORK_ID,
    MAINNET_NETWORK_ID,
    ROPSTEN_NETWORK_ID,
)
from trinity.db.manager import DBClient
from trinity.extensibility import (
    AsyncioIsolatedComponent,
)
from trinity.components.builtin.tx_pool.pool import (
    TxPool,
)
from trinity.components.builtin.tx_pool.validators import (
    DefaultTransactionValidator
)
from trinity.protocol.eth.peer import ETHProxyPeerPool


class TxComponent(AsyncioIsolatedComponent):
    name = "TxComponent"

    logger = logging.getLogger('trinity.components.tx_pool.TxPool')

    @classmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--disable-tx-pool",
            action="store_true",
            help="Disables the Transaction Pool",
        )

    @classmethod
    def validate_cli(cls, boot_info: BootInfo) -> None:
        network_id = boot_info.trinity_config.network_id
        if network_id not in {MAINNET_NETWORK_ID, ROPSTEN_NETWORK_ID, GOERLI_NETWORK_ID}:
            if not boot_info.args.disable_tx_pool:
                raise ValidationError(
                    "The TxPool component only supports Mainnet, Ropsten and Goerli."
                    "You can run with the transaction pool disabled using "
                    "--disable-tx-pool"
                )

    @property
    def is_enabled(self) -> bool:
        light_mode = self._boot_info.args.sync_mode == SYNC_LIGHT
        is_disable = self._boot_info.args.disable_tx_pool
        is_supported = not light_mode
        is_enabled = not is_disable and is_supported

        if not is_disable and not is_supported:
            self.logger.warning("Transaction pool disabled.  Not supported in light mode.")

        return is_enabled

    @classmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        trinity_config = boot_info.trinity_config
        db = DBClient.connect(trinity_config.database_ipc_path)
        with db:
            app_config = trinity_config.get_app_config(Eth1AppConfig)
            chain_config = app_config.get_chain_config()

            chain = chain_config.full_chain_class(db)

            if boot_info.trinity_config.network_id == MAINNET_NETWORK_ID:
                validator = DefaultTransactionValidator(chain, ISTANBUL_MAINNET_BLOCK)
            elif boot_info.trinity_config.network_id == ROPSTEN_NETWORK_ID:
                validator = DefaultTransactionValidator(chain, ISTANBUL_ROPSTEN_BLOCK)
            elif boot_info.trinity_config.network_id == GOERLI_NETWORK_ID:
                validator = DefaultTransactionValidator(chain, ISTANBUL_GOERLI_BLOCK)
            else:
                raise Exception("This code path should not be reachable")

            proxy_peer_pool = ETHProxyPeerPool(event_bus, TO_NETWORKING_BROADCAST_CONFIG)

            tx_pool = TxPool(event_bus, proxy_peer_pool, validator)

            try:
                await run_asyncio_service(tx_pool)
            finally:
                cls.logger.info("Stopping Tx Pool...")
