from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
from asyncio_run_in_process import open_in_process

from eth.chains.mainnet import (
    PETERSBURG_MAINNET_BLOCK,
)
from eth.chains.ropsten import (
    PETERSBURG_ROPSTEN_BLOCK,
)

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
    BaseComponent,
)
from trinity.components.builtin.tx_pool.pool import (
    TxPool,
)
from trinity.components.builtin.tx_pool.validators import (
    DefaultTransactionValidator
)
from trinity.protocol.eth.peer import ETHProxyPeerPool


class TxComponent(BaseComponent):
    tx_pool: TxPool = None
    name: "TxComponent"

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
        light_mode = self.boot_info.args.sync_mode == SYNC_LIGHT
        is_disable = self.boot_info.args.disable_tx_pool
        is_supported = not light_mode
        is_enabled = not is_disable and is_supported
        if not is_supported:
            self.logger.warning("Transaction pool disabled.  Not supported in light mode.")
        return is_enabled

    async def run(self) -> None:
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

        async with open_in_process(tx_pool.run) as proc:
            self._proc = proc
            await proc.wait()


    async def stop(self) -> None:
        self._proc.terminate()


class ComponentService(Service):
    def __init__(self, boot_info: TrinityBootInfo, endpoint_name: str) -> None:
        self._boot_info = boot_info
        self._endpoint_name = endpoint_name

    async def run(self, manager: ManagerAPI) -> None:
        # setup cross process logging
        log_queue = self._boot_info.boot_kwargs['log_queue']
        level = self._boot_info.boot_kwargs.get('log_level', logging.INFO)
        setup_queue_logging(log_queue, level)
        if self.boot_info.args.log_levels:
            setup_log_levels(self.boot_info.args.log_levels)

        # setup the lahja endpoint
        self._connection_config = ConnectionConfig.from_name(
            self._endpoint_name,
            boot_info.trinity_config.ipc_dir
        )

        async with AsyncioEndpoint.serve(self._connection_config) as endpoint:
            await self.run_component_service(endpoint)

    @abstractmethod
    async def run_component_service(self, endpoint: EndpointAPI) -> None:
        ...


class TxPoolService(ComponentService):
    async def run_component_service(self, endpoint: EndpointAPI) -> None:
        trinity_config = self._boot_info.trinity_config
        db = DBClient.connect(trinity_config.database_ipc_path)

        app_config = trinity_config.get_app_config(Eth1AppConfig)
        chain_config = app_config.get_chain_config()

        chain = chain_config.full_chain_class(db)

        if self._boot_info.trinity_config.network_id == MAINNET_NETWORK_ID:
            validator = DefaultTransactionValidator(chain, PETERSBURG_MAINNET_BLOCK)
        elif self._boot_info.trinity_config.network_id == ROPSTEN_NETWORK_ID:
            validator = DefaultTransactionValidator(chain, PETERSBURG_ROPSTEN_BLOCK)
        else:
            raise ValueError("The TxPool component only supports MainnetChain or RopstenChain")

        proxy_peer_pool = ETHProxyPeerPool(endpoint, TO_NETWORKING_BROADCAST_CONFIG)

        tx_pool = TxPool(endpoint, proxy_peer_pool, validator)
        await Manager(tx_pool).run()
