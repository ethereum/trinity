import logging

from eth.db.backends.level import LevelDB
from eth_utils.toolz import curry, valmap
from libp2p.crypto.secp256k1 import create_new_key_pair
import trio

from eth2.beacon.db.chain import BeaconChainDB
from eth2.validator_client.clock import Clock
from trinity._utils.trio_http_server import http_serve, make_router
from trinity.initialization import (
    initialize_beacon_database,
    is_beacon_database_initialized,
)
from trinity.nodes.beacon.api import ValidatorAPIContext, validator_api
from trinity.nodes.beacon.config import BeaconNodeConfig


def _mk_discovery():
    # construct discovery
    # connect to bootstrap peers
    return None


def discovery_adapter(discovery):
    # TODO extract discv5 from component, wrap in libp2p adapter, provide to libp2p construction
    return discovery


def _mk_host():
    # NOTE:
    # BeaconHost
    # - has "peer pool"
    # - has "libp2p.host" to drive implementation
    # - has discovery running
    # yields (validated) network messages
    # including peer "status" messages

    # self._discovery = _mk_discovery()
    # self.network = libp2p.mk_host(router=discovery_adapter(self._discovery))

    # connect to preferred peers
    # connect to any saved peers
    # if we don't have enough peers, discover more and try to connect
    return None


def _mk_clock(config, genesis_time):
    return Clock(
        config.SECONDS_PER_SLOT,
        genesis_time,
        config.SLOTS_PER_EPOCH,
        config.SECONDS_PER_SLOT * config.SLOTS_PER_EPOCH,
    )


@curry
def _mk_trio_handler(context, handler):
    def _handler(target, method, headers, body):
        request = {"target": target, "method": method, "headers": headers, "body": body}
        return handler.server_handler(context, request)

    return _handler


def _mk_validator_api_router(config, chain):
    context = ValidatorAPIContext(config.client_identifier)
    trio_router = valmap(
        lambda handlers: valmap(_mk_trio_handler(context), handlers), validator_api
    )
    return make_router(trio_router)


class BeaconNode:
    logger = logging.getLogger("trinity.nodes.beacon.full.BeaconNode")

    def __init__(self, config: BeaconNodeConfig) -> None:
        self._config = config
        self._local_key_pair = create_new_key_pair(config._local_node_key.to_bytes())

        self._clock = _mk_clock(config._eth2_config, config._chain_config.genesis_time)

        self._base_db = LevelDB(db_path=config._database_dir)
        self._chain_db = BeaconChainDB(self._base_db, config._eth2_config)

        if not is_beacon_database_initialized(self._chain_db):
            initialize_beacon_database(
                config._chain_config, self._chain_db, self._base_db
            )

        self._chain = config._chain_class(self._base_db, config._eth2_config)

        # self._operation_pool = _mk_pool_for_operations(
        #     Attestation, SignedVoluntaryExit, AttesterSlashing, ProposerSlashing
        # )

        # self._host = _mk_host(router=discovery_adapter(self._discovery))

        # self._eth1_provider = _mk_web3_provider()

        self._validator_api_router = _mk_validator_api_router(config, self._chain)

        # NOTE: easiest to run a JSON-over-ipc service for validation API for now?
        # TODO run API Server (over HTTP, IPC)
        # TODO run metrics server (over HTTP)

    @classmethod
    def from_config(cls, config: BeaconNodeConfig) -> "BeaconNode":
        return cls(config)

    async def _launch_sync(self, sync_request):
        # just do plain sync
        pass

    async def _process_msg(self, msg):
        # dispatch msg...
        # ... protocol msg
        # ... sync request
        # ...
        #  -> do stuff to self._chain
        #  then may be send stuff back out to the network!
        pass

    async def _iterate_clock(self):
        async for tick in self._clock:
            self.logger.debug(
                "slot %d [number %d in epoch %d] (tick %d)",
                tick.slot,
                tick.slot_in_epoch(self._config._eth2_config.SLOTS_PER_EPOCH),
                tick.epoch,
                tick.count,
            )

    async def _start_validator_api(self):
        self.logger.info(
            "validator API exposed on %d", self._config._validator_api_port
        )
        await trio.serve_tcp(
            http_serve(self._validator_api_router), self._config._validator_api_port
        )

    async def run(self):
        tasks = (self._iterate_clock, self._start_validator_api)

        async with trio.open_nursery() as nursery:
            for task in tasks:
                nursery.start_soon(task)
