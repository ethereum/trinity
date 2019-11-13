from abc import (
    ABC,
    abstractmethod,
)
from argparse import (
    ArgumentParser,
    Namespace,
    _SubParsersAction,
)
from logging import (
    Logger,
)
from multiprocessing.managers import (
    BaseManager,
)
from typing import (
    cast,
    Dict,
    Tuple,
)

from lahja import EndpointAPI

from eth.abc import AtomicDatabaseAPI
from eth_utils import (
    ValidationError,
)

from asyncio_run_in_process import run_in_process

from p2p.service import background_asyncio_service, AsyncioManager

from trinity.config import (
    Eth1AppConfig,
)
from trinity.constants import (
    NETWORKING_EVENTBUS_ENDPOINT,
    SYNC_FULL,
    SYNC_LIGHT,
    SYNC_BEAM,
)
from trinity.chains.base import AsyncChainAPI
from trinity.db.eth1.chain import AsyncChainDB
from trinity.db.eth1.header import AsyncHeaderDB
from trinity.extensibility import BaseApplicationComponent, ComponentService
from trinity.events import ShutdownRequest
from trinity.protocol.common.peer import (
    BasePeerPool,
)
from trinity.protocol.eth.peer import (
    BaseChainPeerPool,
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
from trinity.sync.light.chain import (
    LightChainSyncer,
)
from .cli import NormalizeCheckpointURI


class BaseSyncStrategy(ABC):

    @property
    def shutdown_node_on_halt(self) -> bool:
        """
        Return ``False`` if the `sync` is allowed to complete without causing
        the node to fully shut down.
        """
        return True

    @classmethod
    def configure_parser(cls, arg_parser: ArgumentParser) -> None:
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
                   logger: Logger,
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
                   logger: Logger,
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
                   logger: Logger,
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
    def configure_parser(cls, arg_parser: ArgumentParser) -> None:
        arg_parser.add_argument(
            '--force-beam-block-number',
            type=int,
            help="Force beam sync to activate on a specific block number (for testing)",
            default=None,
        )

        arg_parser.add_argument(
            '--beam-from-checkpoint',
            action=NormalizeCheckpointURI,
            help=(
                "Start beam sync from a trusted checkpoint specified using URI syntax:"
                "By specific block, eth://block/byhash/<hash>?score=<score>"
                "Let etherscan pick a block near the tip, eth://block/byetherscan/latest"
            ),
            default=None,
        )

    async def sync(self,
                   args: Namespace,
                   logger: Logger,
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
            args.beam_from_checkpoint,
            args.force_beam_block_number,
        )

        await AsyncioManager.run_service(syncer)


class LightSyncStrategy(BaseSyncStrategy):

    @classmethod
    def get_sync_mode(cls) -> str:
        return SYNC_LIGHT

    async def sync(self,
                   args: Namespace,
                   logger: Logger,
                   chain: AsyncChainAPI,
                   base_db: AtomicDatabaseAPI,
                   peer_pool: BasePeerPool,
                   event_bus: EndpointAPI) -> None:

        syncer = LightChainSyncer(
            chain,
            AsyncHeaderDB(base_db),
            cast(LESPeerPool, peer_pool),
        )

        await syncer.run()


DEFAULT_SYNC_STRATEGY = BeamSyncStrategy()
DEFAULT_SYNC_MODE = DEFAULT_SYNC_STRATEGY.get_sync_mode().lower()

SYNC_STRATEGIES: Tuple[BaseSyncStrategy, ...] = (
    FullSyncStrategy(),
    DEFAULT_SYNC_STRATEGY,
    LightSyncStrategy(),
    NoopSyncStrategy(),
)
SYNC_MODES: Dict[str, BaseSyncStrategy] = {
    strategy.get_sync_mode().lower(): strategy
    for strategy
    in SYNC_STRATEGIES
}

if len(SYNC_STRATEGIES) != len(SYNC_MODES):
    raise ValidationError("Maybe duplicate sync mode?")
elif DEFAULT_SYNC_MODE not in SYNC_MODES:
    raise ValidationError("Defauult sync mode not found in sync mode options")


class SyncerComponent(BaseApplicationComponent):
    name = "Sync / PeerPool"

    peer_pool: BaseChainPeerPool = None
    chain: AsyncChainAPI = None
    db_manager: BaseManager = None

    @classmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        for sync_strategy in SYNC_STRATEGIES:
            sync_strategy.configure_parser(arg_parser)

        syncing_parser = arg_parser.add_argument_group('sync mode')
        syncing_parser.add_argument(
            '--sync-mode',
            choices=tuple(sorted(SYNC_MODES.keys())),
            default=DEFAULT_SYNC_MODE,
        )

    @property
    def is_enabled(self) -> bool:
        return self._boot_info.args.sync_mode.lower() in SYNC_MODES

    async def run(self) -> None:
        service = SyncComponentService(self._boot_info, self.name)

        await run_in_process(AsyncioManager.run_service, service)


class SyncComponentService(ComponentService):
    _explicit_ipc_filename = NETWORKING_EVENTBUS_ENDPOINT

    async def run_component_service(self) -> None:
        active_strategy = SYNC_MODES[self.boot_info.args.sync_mode.lower()]

        trinity_config = self.boot_info.trinity_config
        NodeClass = trinity_config.get_app_config(Eth1AppConfig).node_class
        node = NodeClass(self.event_bus, trinity_config)

        async with background_asyncio_service(node):
            await active_strategy.sync(
                self.boot_info.args,
                self.logger,
                node.get_chain(),
                node.base_db,
                node.get_peer_pool(),
                self.event_bus,
            )

            if active_strategy.shutdown_node_on_halt:
                self.logger.error("Sync ended unexpectedly. Shutting down trinity")
                await self.event_bus.broadcast(ShutdownRequest("Sync ended unexpectedly"))
