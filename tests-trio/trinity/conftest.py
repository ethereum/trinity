import secrets

from eth_keys.datatypes import PrivateKey
import pytest

from eth2.beacon.chains.testnet import SkeletonLakeChain
from eth2.beacon.state_machines.forks.skeleton_lake.config import (
    MINIMAL_SERENITY_CONFIG,
)
from trinity.config import BeaconChainConfig


@pytest.fixture
def node_key():
    key_bytes = secrets.token_bytes(32)
    return PrivateKey(key_bytes)


@pytest.fixture
def eth2_config():
    return MINIMAL_SERENITY_CONFIG


@pytest.fixture
def chain_config():
    return BeaconChainConfig.from_genesis_config()


@pytest.fixture
def database_dir(tmp_path):
    return tmp_path


@pytest.fixture
def chain_class():
    return SkeletonLakeChain
