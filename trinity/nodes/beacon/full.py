import logging
from pathlib import Path
from typing import Set, Type

from eth.db.backends.level import LevelDB
from eth_keys.datatypes import PrivateKey
from libp2p.crypto.secp256k1 import create_new_key_pair
import trio
from trio_typing import TaskStatus

from eth2.api.http.validator import Context
from eth2.api.http.validator import ServerHandlers as ValidatorAPIHandlers
from eth2.api.http.validator import SyncerAPI, SyncStatus
from eth2.beacon.chains.base import BaseBeaconChain
from eth2.beacon.chains.exceptions import ParentNotFoundError, SlashableBlockError
from eth2.beacon.db.chain import BeaconChainDB
from eth2.beacon.types.blocks import SignedBeaconBlock
from eth2.beacon.typing import Root, Slot
from eth2.clock import Clock, Tick, TimeProvider, get_unix_time
from eth2.configs import Eth2Config
from trinity._utils.trio_utils import JSONHTTPServer
from trinity.config import BeaconChainConfig
from trinity.initialization import (
    initialize_beacon_database,
    is_beacon_database_initialized,
)
from trinity.nodes.beacon.config import BeaconNodeConfig


def _mk_clock(
    config: Eth2Config, genesis_time: int, time_provider: TimeProvider
) -> Clock:
    return Clock(
        config.SECONDS_PER_SLOT,
        genesis_time,
        config.SLOTS_PER_EPOCH,
        config.SECONDS_PER_SLOT * config.SLOTS_PER_EPOCH,
        time_provider,
    )


def _mk_syncer() -> SyncerAPI:
    class _sync(SyncerAPI):
        async def get_status(self) -> SyncStatus:
            return SyncStatus(False, Slot(0), Slot(0), Slot(0))

    return _sync()


def _mk_validator_api_server(
    validator_api_port: int, context: Context
) -> JSONHTTPServer[Context]:
    # NOTE: `mypy` claims the handlers are not typed correctly although it does determine
    # the async callable to be a subtype of the declared type so it seems like a bug
    # and we will ignore for now...
    # See https://mypy.readthedocs.io/en/stable/more_types.html#typing-async-await
    return JSONHTTPServer(
        ValidatorAPIHandlers, context, validator_api_port  # type: ignore
    )


class BeaconNode:
    logger = logging.getLogger("trinity.nodes.beacon.full.BeaconNode")

    def __init__(
        self,
        local_node_key: PrivateKey,
        eth2_config: Eth2Config,
        chain_config: BeaconChainConfig,
        database_dir: Path,
        chain_class: Type[BaseBeaconChain],
        clock: Clock,
        validator_api_port: int,
        client_identifier: str,
    ) -> None:
        self._local_key_pair = create_new_key_pair(local_node_key.to_bytes())
        self._eth2_config = eth2_config

        self._clock = clock

        self._syncer = _mk_syncer()

        self._base_db = LevelDB(db_path=database_dir)
        self._chain_db = BeaconChainDB(self._base_db, eth2_config)

        if not is_beacon_database_initialized(self._chain_db):
            initialize_beacon_database(chain_config, self._chain_db, self._base_db)

        self._chain = chain_class(self._base_db, eth2_config)

        self._block_pool: Set[SignedBeaconBlock] = set()

        api_context = Context(
            client_identifier,
            chain_config.genesis_time,
            eth2_config,
            self._syncer,
            self._chain,
            self._clock,
        )
        self._api_context = api_context
        self._validator_api_port = validator_api_port
        self._validator_api_server = _mk_validator_api_server(
            validator_api_port, api_context
        )

    @classmethod
    def from_config(
        cls, config: BeaconNodeConfig, time_provider: TimeProvider = get_unix_time
    ) -> "BeaconNode":
        clock = _mk_clock(
            config.eth2_config, config.chain_config.genesis_time, time_provider
        )
        return cls(
            config.local_node_key,
            config.eth2_config,
            config.chain_config,
            config.database_dir,
            config.chain_class,
            clock,
            config.validator_api_port,
            config.client_identifier,
        )

    @property
    def current_tick(self) -> Tick:
        return self._clock.compute_current_tick()

    async def _iterate_clock(
        self, task_status: TaskStatus[None] = trio.TASK_STATUS_IGNORED
    ) -> None:
        task_status.started()
        async for tick in self._clock:
            self.logger.debug(
                "slot %d [number %d in epoch %d] (tick %d)",
                tick.slot,
                tick.slot_in_epoch(self._eth2_config.SLOTS_PER_EPOCH),
                tick.epoch,
                tick.count,
            )
            self._chain.on_tick(tick)

    async def _run_validator_api(
        self, task_status: TaskStatus[None] = trio.TASK_STATUS_IGNORED
    ) -> None:
        server = self._validator_api_server
        async with trio.open_nursery() as nursery:
            self.validator_api_port = await nursery.start(server.serve)
            self.logger.info(
                "validator HTTP API server listening on %d", self.validator_api_port
            )
            task_status.started()

    def on_block(self, block: SignedBeaconBlock) -> None:
        try:
            self._chain.import_block(block)
        except ParentNotFoundError as exc:
            self._on_orphan_block(block, exc)
        except SlashableBlockError as exc:
            self._on_slashable_block(block, exc)
        except Exception as exc:
            self._on_block_failure(block, exc)
        else:
            self._on_block_imported(block)

    def _try_import_orphan(self, imported_parent_root: Root) -> None:
        for orphan in self._block_pool:
            if orphan.message.parent_root == imported_parent_root:
                self._block_pool.discard(orphan)
                return self.on_block(orphan)

    def _on_block_imported(self, block: SignedBeaconBlock) -> None:
        self.logger.debug("successfully imported block %s", block)
        self._try_import_orphan(block.message.hash_tree_root)
        # TODO: synchronize any operations from pools that are now on-chain

    def _on_block_failure(self, block: SignedBeaconBlock, exc: Exception) -> None:
        self.logger.exception("failed to import block %s: %s", block, exc)
        # TODO: do not drop block?

    def _on_orphan_block(
        self, block: SignedBeaconBlock, exc: ParentNotFoundError
    ) -> None:
        self.logger.debug("failed to import block %s: %s", block, exc)
        self._block_pool.add(block)

    def _on_slashable_block(
        self, block: SignedBeaconBlock, exc: SlashableBlockError
    ) -> None:
        self.logger.warning("failed to import block %s: %s", block, exc)
        # NOTE: chain will write the block in ``import_block`` but not the block's state
        # See the place that exception is raised for further rationale.
        # TODO: pipe to "slasher" software...

    async def run(
        self, task_status: TaskStatus[None] = trio.TASK_STATUS_IGNORED
    ) -> None:
        tasks = (self._iterate_clock, self._run_validator_api)

        async with trio.open_nursery() as nursery:
            for task in tasks:
                await nursery.start(task)
            task_status.started()
