import secrets

from eth_keys.datatypes import PrivateKey
import pytest
import trio

from eth2.beacon.chains.testnet import SkeletonLakeChain
from eth2.beacon.state_machines.forks.skeleton_lake.config import (
    MINIMAL_SERENITY_CONFIG,
)
from eth2.beacon.tools.misc.ssz_vector import override_lengths
from eth2.beacon.types.states import BeaconState
from trinity.config import BeaconChainConfig


@pytest.fixture
def node_key():
    key_bytes = secrets.token_bytes(32)
    return PrivateKey(key_bytes)


@pytest.fixture
def eth2_config():
    config = MINIMAL_SERENITY_CONFIG
    # NOTE: have to ``override_lengths`` before we can parse ssz objects, like the BeaconState
    override_lengths(config)
    return config


@pytest.fixture
async def current_time():
    return trio.current_time()


@pytest.fixture
def genesis_time(current_time, eth2_config):
    slots_after_genesis = 10
    return int(current_time - slots_after_genesis * eth2_config.SECONDS_PER_SLOT)


@pytest.fixture
def genesis_state(eth2_config, genesis_time):
    state = BeaconState.create(config=eth2_config)
    state.genesis_time = genesis_time
    return state


@pytest.fixture
def chain_config(genesis_state, eth2_config):
    return BeaconChainConfig(genesis_state, eth2_config, {})


@pytest.fixture
def database_dir(tmp_path):
    return tmp_path


@pytest.fixture
def chain_class():
    return SkeletonLakeChain
