from abc import (
    ABC,
    abstractmethod,
)
from argparse import (
    ArgumentError,
    ArgumentParser,
    Namespace,
    _ArgumentGroup,
    _SubParsersAction,
)
import logging
from typing import (
    cast,
    Iterable,
    Tuple,
    Type,
)

from async_service import background_asyncio_service
from lahja import EndpointAPI

from eth.abc import AtomicDatabaseAPI
from eth_utils import (
    to_tuple,
    ValidationError,
)

from trinity.boot_info import BootInfo
from trinity.config import (
    Eth1AppConfig,
)
from trinity.components.builtin.metrics.component import metrics_service_from_args
from trinity.components.builtin.metrics.service.asyncio import AsyncioMetricsService
from trinity.components.builtin.metrics.service.noop import NOOP_METRICS_SERVICE
from trinity.constants import (
    NETWORKING_EVENTBUS_ENDPOINT,
    SYNC_FULL,
    SYNC_LIGHT,
    SYNC_BEAM,
)
from trinity.chains.base import AsyncChainAPI
from trinity.db.eth1.chain import AsyncChainDB
from trinity.db.eth1.header import AsyncHeaderDB
from trinity.extensibility.asyncio import (
    AsyncioIsolatedComponent
)
from trinity.nodes.base import (
    Node,
)
from trinity.events import ShutdownRequest
from trinity.protocol.common.peer import (
    BasePeer,
    BasePeerPool,
)
from trinity.protocol.eth.peer import (
    ETHPeerPool,
)
from trinity.protocol.les.peer import (
    LESPeerPool,
)
from trinity.sync.full.service import (
    FullChainSyncer,
)
from trinity.sync.beam.service import (
    BeamSyncService,
)
from trinity.sync.header.chain import HeaderChainSyncer
from trinity.sync.light.chain import (
    LightChainSyncer,
)
from trinity.components.builtin.syncer.cli import NormalizeCheckpointURI


def add_sync_from_checkpoint_arg(arg_parser: _ArgumentGroup) -> None:
    try:
        arg_parser.add_argument(
            '--sync-from-checkpoint',
            action=NormalizeCheckpointURI,
            help=(
                "Start syncing from a trusted checkpoint specified using URI syntax:"
                "By specific block, eth://block/byhash/<hash>?score=<score>"
                "Let etherscan pick a block near the tip, eth://block/byetherscan/latest"
            ),
            default=None,
        )
    except ArgumentError as err:
        if "conflicting option string: --sync-from-checkpoint" in str(err):
            # --sync-from-checkpoint is used for multiple strategies but only one of them can
            # add the flag. We do not want strategies to rely on other strategies to add the flag
            # so we have to catch the error and silence it.
            pass
        else:
            # Re-raise in case we caught a different error than we expected.
            raise


class BaseSyncStrategy(ABC):

    @property
    def shutdown_node_on_halt(self) -> bool:
        """
        Return ``False`` if the `sync` is allowed to complete without causing
        the node to fully shut down.
        """
        return True

    @classmethod
    def configure_parser(cls, arg_group: _ArgumentGroup) -> None:
        """
        Configure the argument parser for the specific sync strategy.
        """
        pass

    @classmethod
    @abstractmethod
    def get_sync_mode(cls) -> str:
        ...

    @abstractmethod
    async def sync(self,
                   args: Namespace,
                   logger: logging.Logger,
                   chain: AsyncChainAPI,
                   base_db: AtomicDatabaseAPI,
                   peer_pool: BasePeerPool,
                   event_bus: EndpointAPI) -> None:
        ...


class NoopSyncStrategy(BaseSyncStrategy):

    @property
    def shutdown_node_on_halt(self) -> bool:
        return False

    @classmethod
    def get_sync_mode(cls) -> str:
        return 'none'

    async def sync(self,
                   args: Namespace,
                   logger: logging.Logger,
                   chain: AsyncChainAPI,
                   base_db: AtomicDatabaseAPI,
                   peer_pool: BasePeerPool,
                   event_bus: EndpointAPI) -> None:

        logger.info("Node running without sync (--sync-mode=%s)", self.get_sync_mode())


class FullSyncStrategy(BaseSyncStrategy):

    @classmethod
    def get_sync_mode(cls) -> str:
        return SYNC_FULL

    async def sync(self,
                   args: Namespace,
                   logger: logging.Logger,
                   chain: AsyncChainAPI,
                   base_db: AtomicDatabaseAPI,
                   peer_pool: BasePeerPool,
                   event_bus: EndpointAPI) -> None:

        syncer = FullChainSyncer(
            chain,
            AsyncChainDB(base_db),
            base_db,
            cast(ETHPeerPool, peer_pool),
        )

        await syncer.run()


class BeamSyncStrategy(BaseSyncStrategy):

    @classmethod
    def get_sync_mode(cls) -> str:
        return SYNC_BEAM

    @classmethod
    def configure_parser(cls, arg_group: _ArgumentGroup) -> None:
        arg_group.add_argument(
            '--force-beam-block-number',
            type=int,
            help="Force beam sync to activate on a specific block number (for testing)",
            default=None,
        )

        add_sync_from_checkpoint_arg(arg_group)

    async def sync(self,
                   args: Namespace,
                   logger: logging.Logger,
                   chain: AsyncChainAPI,
                   base_db: AtomicDatabaseAPI,
                   peer_pool: BasePeerPool,
                   event_bus: EndpointAPI) -> None:

        syncer = BeamSyncService(
            chain,
            AsyncChainDB(base_db),
            base_db,
            cast(ETHPeerPool, peer_pool),
            event_bus,
            args.sync_from_checkpoint,
            args.force_beam_block_number,
        )

        async with background_asyncio_service(syncer) as manager:
            await manager.wait_finished()


class HeaderSyncStrategy(BaseSyncStrategy):

    @classmethod
    def get_sync_mode(cls) -> str:
        return 'header'

    @classmethod
    def configure_parser(cls, arg_parser: _ArgumentGroup) -> None:
        add_sync_from_checkpoint_arg(arg_parser)

    async def sync(self,
                   args: Namespace,
                   logger: logging.Logger,
                   chain: AsyncChainAPI,
                   base_db: AtomicDatabaseAPI,
                   peer_pool: BasePeerPool,
                   event_bus: EndpointAPI) -> None:

        syncer = HeaderChainSyncer(
            chain,
            AsyncChainDB(base_db),
            cast(ETHPeerPool, peer_pool),
            args.sync_from_checkpoint,
        )

        async with background_asyncio_service(syncer) as manager:
            await manager.wait_finished()


class LightSyncStrategy(BaseSyncStrategy):

    @classmethod
    def get_sync_mode(cls) -> str:
        return SYNC_LIGHT

    async def sync(self,
                   args: Namespace,
                   logger: logging.Logger,
                   chain: AsyncChainAPI,
                   base_db: AtomicDatabaseAPI,
                   peer_pool: BasePeerPool,
                   event_bus: EndpointAPI) -> None:

        syncer = LightChainSyncer(
            chain,
            AsyncHeaderDB(base_db),
            cast(LESPeerPool, peer_pool),
        )

        async with background_asyncio_service(syncer) as manager:
            await manager.wait_finished()


class SyncerComponent(AsyncioIsolatedComponent):
    default_strategy = BeamSyncStrategy()
    strategies: Tuple[BaseSyncStrategy, ...] = (
        HeaderSyncStrategy(),
        FullSyncStrategy(),
        default_strategy,
        LightSyncStrategy(),
        NoopSyncStrategy(),
    )

    name = "Sync / PeerPool"

    endpoint_name = NETWORKING_EVENTBUS_ENDPOINT

    logger = logging.getLogger('trinity.components.sync.Sync')

    @property
    def is_enabled(self) -> bool:
        return True

    @classmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        if type(cls.default_strategy) not in cls.extract_strategy_types():
            raise ValidationError(f"Default strategy {cls.default_strategy} not in strategies")

        syncing_parser = arg_parser.add_argument_group('sync mode')
        mode_parser = syncing_parser.add_mutually_exclusive_group()
        mode_parser.add_argument(
            '--sync-mode',
            choices=cls.extract_modes(),
            default=cls.default_strategy.get_sync_mode(),
        )

        for sync_strategy in cls.strategies:
            sync_strategy.configure_parser(syncing_parser)

    @classmethod
    def validate_cli(cls, boot_info: BootInfo) -> None:
        # this will trigger a ValidationError if the specified strategy isn't known.
        cls.get_active_strategy(boot_info)

        # This will trigger a ValidationError if the loaded EIP1085 file
        # has errors such as an unsupported mining method
        boot_info.trinity_config.get_app_config(Eth1AppConfig).get_chain_config()

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

    @classmethod
    def get_active_strategy(cls, boot_info: BootInfo) -> BaseSyncStrategy:
        active_strategy: BaseSyncStrategy = None

        for strategy in cls.strategies:
            if strategy.get_sync_mode().lower() == boot_info.args.sync_mode.lower():
                if active_strategy is not None:
                    raise ValidationError(
                        f"Ambiguous sync strategy. Both {active_strategy} and {strategy} apply"
                    )
                active_strategy = strategy

        if active_strategy is None:
            if boot_info.args.sync_mode is not None:
                raise ValidationError(
                    f"No matching sync mode for: --sync-mode={boot_info.args.sync_mode}"
                )
            return cls.default_strategy
        else:
            return active_strategy

    async def do_run(self, event_bus: EndpointAPI) -> None:
        boot_info = self._boot_info

        if boot_info.args.enable_metrics:
            metrics_service = metrics_service_from_args(boot_info.args, AsyncioMetricsService)
        else:
            # Use a NoopMetricsService so that no code branches need to be taken if metrics
            # are disabled
            metrics_service = NOOP_METRICS_SERVICE

        trinity_config = boot_info.trinity_config
        NodeClass = trinity_config.get_app_config(Eth1AppConfig).node_class
        node = NodeClass(event_bus, metrics_service, trinity_config)
        strategy = self.get_active_strategy(boot_info)

        async with background_asyncio_service(node) as manager:
            await self.launch_sync(node, strategy, boot_info, event_bus)
            await manager.wait_finished()

    @classmethod
    async def launch_sync(cls,
                          node: Node[BasePeer],
                          strategy: BaseSyncStrategy,
                          boot_info: BootInfo,
                          event_bus: EndpointAPI) -> None:
        await node.get_manager().wait_started()
        await strategy.sync(
            boot_info.args,
            cls.logger,
            node.get_chain(),
            node.base_db,
            node.get_peer_pool(),
            event_bus,
        )

        if strategy.shutdown_node_on_halt:
            cls.logger.error("Sync ended unexpectedly. Shutting down trinity")
            event_bus.broadcast_nowait(ShutdownRequest("Sync ended unexpectedly"))


if __name__ == "__main__":
    # SyncerComponent depends on a separate component to get peer candidates, so when running it
    # you must pass the path to the discovery component's IPC file, like:
    # $ python .../syncer/component.py --trinity-root-dir /tmp/syncer \
    #        --connect-to-endpoints /tmp/syncer/mainnet/ipcs-eth1/discovery.ipc
    from trinity.extensibility.component import run_asyncio_eth1_component
    run_asyncio_eth1_component(SyncerComponent)
