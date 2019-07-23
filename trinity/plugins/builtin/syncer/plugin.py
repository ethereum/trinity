from abc import (
    ABC,
    abstractmethod,
)
from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
import asyncio
from logging import (
    Logger,
)
from multiprocessing.managers import (
    BaseManager,
)
from typing import (
    Any,
    cast,
    Iterable,
    Type,
)

from lahja import EndpointAPI

from cancel_token import CancelToken
from eth.chains.base import (
    BaseChain
)
from eth_utils import (
    to_tuple,
    ValidationError,
)

from trinity.config import (
    Eth1AppConfig,
)
from trinity.constants import (
    SYNC_FAST,
    SYNC_FULL,
    SYNC_LIGHT,
    SYNC_BEAM,
    TO_NETWORKING_BROADCAST_CONFIG,
)
from trinity.db.eth1.manager import (
    create_db_consumer_manager,
)
from trinity.events import (
    ShutdownRequest,
)
from trinity.extensibility.asyncio import (
    AsyncioIsolatedPlugin
)
from trinity.protocol.common.peer_pool_event_bus import (
    BaseProxyPeerPool,
)
from trinity.protocol.eth.peer import (
    BaseChainPeerPool,
    ETHProxyPeerPool,
)
from trinity.protocol.les.peer import (
    LESProxyPeerPool,
)
from trinity.sync.full.service import (
    FastThenFullChainSyncer,
    FullChainSyncer,
)
from trinity.sync.beam.service import (
    BeamSyncService,
)
from trinity.sync.light.chain import (
    LightChainSyncer,
)
from trinity._utils.shutdown import (
    exit_with_services,
)


class BaseSyncStrategy(ABC):

    @property
    def shutdown_node_on_halt(self) -> bool:
        """
        Return ``False`` if the `sync` is allowed to complete without causing
        the node to fully shut down.
        """
        return True

    @classmethod
    @abstractmethod
    def get_sync_mode(cls) -> str:
        ...

    @abstractmethod
    async def sync(self,
                   logger: Logger,
                   chain: BaseChain,
                   db_manager: BaseManager,
                   peer_pool: BaseProxyPeerPool[Any],
                   event_bus: EndpointAPI,
                   cancel_token: CancelToken) -> None:
        ...


class NoopSyncStrategy(BaseSyncStrategy):

    @property
    def shutdown_node_on_halt(self) -> bool:
        return False

    @classmethod
    def get_sync_mode(cls) -> str:
        return 'none'

    async def sync(self,
                   logger: Logger,
                   chain: BaseChain,
                   db_manager: BaseManager,
                   peer_pool: BaseProxyPeerPool[Any],
                   event_bus: EndpointAPI,
                   cancel_token: CancelToken) -> None:

        logger.info("Node running without sync (--sync-mode=%s)", self.get_sync_mode())


class FullSyncStrategy(BaseSyncStrategy):

    @classmethod
    def get_sync_mode(cls) -> str:
        return SYNC_FULL

    async def sync(self,
                   logger: Logger,
                   chain: BaseChain,
                   db_manager: BaseManager,
                   peer_pool: BaseProxyPeerPool[Any],
                   event_bus: EndpointAPI,
                   cancel_token: CancelToken) -> None:

        syncer = FullChainSyncer(
            chain,
            db_manager.get_chaindb(),  # type: ignore
            db_manager.get_db(),  # type: ignore
            cast(ETHProxyPeerPool, peer_pool),
            cancel_token,
        )

        await syncer.run()


class FastThenFullSyncStrategy(BaseSyncStrategy):

    @classmethod
    def get_sync_mode(cls) -> str:
        return SYNC_FAST

    async def sync(self,
                   logger: Logger,
                   chain: BaseChain,
                   db_manager: BaseManager,
                   peer_pool: BaseProxyPeerPool[Any],
                   event_bus: EndpointAPI,
                   cancel_token: CancelToken) -> None:

        syncer = FastThenFullChainSyncer(
            chain,
            db_manager.get_chaindb(),  # type: ignore
            db_manager.get_db(),  # type: ignore
            cast(ETHProxyPeerPool, peer_pool),
            cancel_token,
        )

        await syncer.run()


class BeamSyncStrategy(BaseSyncStrategy):

    @classmethod
    def get_sync_mode(cls) -> str:
        return SYNC_BEAM

    async def sync(self,
                   logger: Logger,
                   chain: BaseChain,
                   db_manager: BaseManager,
                   peer_pool: BaseProxyPeerPool[Any],
                   event_bus: EndpointAPI,
                   cancel_token: CancelToken) -> None:

        syncer = BeamSyncService(
            chain,
            db_manager.get_chaindb(),  # type: ignore
            db_manager.get_db(),  # type: ignore
            cast(ETHProxyPeerPool, peer_pool),
            event_bus,
            cancel_token,
        )

        await syncer.run()


class LightSyncStrategy(BaseSyncStrategy):

    @classmethod
    def get_sync_mode(cls) -> str:
        return SYNC_LIGHT

    async def sync(self,
                   logger: Logger,
                   chain: BaseChain,
                   db_manager: BaseManager,
                   peer_pool: BaseProxyPeerPool[Any],
                   event_bus: EndpointAPI,
                   cancel_token: CancelToken) -> None:

        syncer = LightChainSyncer(
            chain,
            db_manager.get_headerdb(),  # type: ignore
            cast(LESProxyPeerPool, peer_pool),
            cancel_token,
        )

        await syncer.run()


class SyncerPlugin(AsyncioIsolatedPlugin):
    peer_pool: BaseChainPeerPool = None
    cancel_token: CancelToken = None
    chain: BaseChain = None
    db_manager: BaseManager = None

    active_strategy: BaseSyncStrategy = None
    strategies: Iterable[BaseSyncStrategy] = (
        FastThenFullSyncStrategy(),
        FullSyncStrategy(),
        BeamSyncStrategy(),
        LightSyncStrategy(),
        NoopSyncStrategy(),
    )

    default_strategy = FastThenFullSyncStrategy

    @property
    def name(self) -> str:
        return "Sync"

    @classmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:

        if cls.default_strategy not in cls.extract_strategy_types():
            raise ValidationError(f"Default strategy {cls.default_strategy} not in strategies")

        syncing_parser = arg_parser.add_argument_group('sync mode')
        mode_parser = syncing_parser.add_mutually_exclusive_group()
        mode_parser.add_argument(
            '--sync-mode',
            choices=cls.extract_modes(),
            default=cls.default_strategy.get_sync_mode(),
        )

    @classmethod
    @to_tuple
    def extract_modes(cls) -> Iterable[str]:
        for strategy in cls.strategies:
            yield strategy.get_sync_mode()

    @classmethod
    @to_tuple
    def extract_strategy_types(cls) -> Iterable[Type[BaseSyncStrategy]]:
        for strategy in cls.strategies:
            yield type(strategy)

    def on_ready(self, manager_eventbus: EndpointAPI) -> None:
        for strategy in self.strategies:
            if strategy.get_sync_mode().lower() == self.boot_info.args.sync_mode.lower():
                if self.active_strategy is not None:
                    raise ValidationError(
                        f"Ambiguous sync strategy. Both {self.active_strategy} and {strategy} apply"
                    )
                self.active_strategy = strategy

        if not self.active_strategy:
            self.logger.warn(
                "No sync strategy matches --sync-mode=%s",
                self.boot_info.args.sync_mode
            )
            return

        self.start()

    def do_start(self) -> None:
        asyncio.ensure_future(self.launch_sync())

    async def launch_sync(self) -> None:
        trinity_config = self.boot_info.trinity_config
        app_config = trinity_config.get_app_config(Eth1AppConfig)
        chain_config = app_config.get_chain_config()
        db_manager = create_db_consumer_manager(trinity_config.database_ipc_path)

        chain = chain_config.full_chain_class(db_manager.get_db())  # type: ignore

        proxy_peer_pool = ETHProxyPeerPool(self.event_bus, TO_NETWORKING_BROADCAST_CONFIG)
        asyncio.ensure_future(exit_with_services(self._event_bus_service, proxy_peer_pool))
        asyncio.ensure_future(proxy_peer_pool.run())

        await self.active_strategy.sync(
            self.logger,
            chain,
            db_manager,
            proxy_peer_pool,
            self.event_bus,
            proxy_peer_pool.cancel_token
        )

        if self.active_strategy.shutdown_node_on_halt:
            self.logger.error("Sync ended unexpectedly. Shutting down trinity")
            await self.event_bus.broadcast(ShutdownRequest("Sync ended unexpectedly"))
