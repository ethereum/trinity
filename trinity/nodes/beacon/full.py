import logging
from pathlib import Path
from typing import Type

from eth.db.backends.level import LevelDB
from eth_keys.datatypes import PrivateKey
from libp2p.crypto.secp256k1 import create_new_key_pair
import trio

from eth2.beacon.chains.base import BaseBeaconChain
from eth2.beacon.db.chain import BeaconChainDB
from eth2.clock import Clock, TimeProvider, get_unix_time
from eth2.configs import Eth2Config
from trinity.config import BeaconChainConfig
from trinity.initialization import (
    initialize_beacon_database,
    is_beacon_database_initialized,
)
from trinity.nodes.beacon.config import BeaconNodeConfig


def _mk_clock(config: Eth2Config, genesis_time: int, time_provider: TimeProvider):
    return Clock(
        config.SECONDS_PER_SLOT,
        genesis_time,
        config.SLOTS_PER_EPOCH,
        config.SECONDS_PER_SLOT * config.SLOTS_PER_EPOCH,
        time_provider,
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
        time_provider: TimeProvider = get_unix_time,
    ) -> None:
        self._local_key_pair = create_new_key_pair(local_node_key.to_bytes())
        self._eth2_config = eth2_config

        self._clock = _mk_clock(eth2_config, chain_config.genesis_time, time_provider)
        self._tick_lock = trio.Lock()
        self._current_tick = self._clock.compute_current_tick()

        self._base_db = LevelDB(db_path=database_dir)
        self._chain_db = BeaconChainDB(self._base_db, eth2_config)

        if not is_beacon_database_initialized(self._chain_db):
            initialize_beacon_database(chain_config, self._chain_db, self._base_db)

        self._chain = chain_class(self._base_db, eth2_config)

    @classmethod
    def from_config(cls, config: BeaconNodeConfig) -> "BeaconNode":
        return cls(
            config.local_node_key,
            config.eth2_config,
            config.chain_config,
            config.database_dir,
            config.chain_class,
        )

    async def _iterate_clock(self):
        async for tick in self._clock:
            async with self._tick_lock:
                self._current_tick = tick
            self.logger.debug(
                "slot %d [number %d in epoch %d] (tick %d)",
                tick.slot,
                tick.slot_in_epoch(self._eth2_config.SLOTS_PER_EPOCH),
                tick.epoch,
                tick.count,
            )

    def _stop(self):
        # NOTE: update the node's internal state in the event that the
        # node is canceled while having cached a previous tick
        self._current_tick = self._clock.compute_current_tick()

    async def run(self):
        tasks = (self._iterate_clock,)

        try:
            async with trio.open_nursery() as nursery:
                for task in tasks:
                    nursery.start_soon(task)
        finally:
            self._stop()
