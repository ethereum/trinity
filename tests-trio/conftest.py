from pathlib import Path
import secrets
import tempfile
import uuid

from eth_keys.datatypes import PrivateKey
from lahja import ConnectionConfig
from lahja.trio.endpoint import TrioEndpoint
import pytest
import trio

from eth2._utils.bls import Eth2BLS, bls
from eth2.beacon.chains.testnet import SkeletonLakeChain
from eth2.beacon.constants import FAR_FUTURE_EPOCH
from eth2.beacon.state_machines.forks.skeleton_lake.config import (
    MINIMAL_SERENITY_CONFIG,
)
from eth2.beacon.tools.builder.initializer import create_key_pairs_for
from eth2.beacon.tools.misc.ssz_vector import override_lengths
from eth2.beacon.types.blocks import BeaconBlockBody, BeaconBlockHeader
from eth2.beacon.types.states import BeaconState
from eth2.beacon.types.validators import Validator
from trinity.config import BeaconChainConfig

# Fixtures below are copied from https://github.com/ethereum/lahja/blob/f0b7ead13298de82c02ed92cfb2d32a8bc00b42a/tests/core/trio/conftest.py  # noqa: E501


@pytest.fixture
def ipc_base_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


def generate_unique_name() -> str:
    # We use unique names to avoid clashing of IPC pipes
    return str(uuid.uuid4())


@pytest.fixture
def endpoint_server_config(ipc_base_path):
    config = ConnectionConfig.from_name(generate_unique_name(), base_path=ipc_base_path)
    return config


@pytest.fixture
async def endpoint_server(endpoint_server_config):
    async with TrioEndpoint.serve(endpoint_server_config) as endpoint:
        yield endpoint


@pytest.fixture
async def endpoint_client(endpoint_server_config, endpoint_server):
    async with TrioEndpoint("client-for-testing").run() as client:
        await client.connect_to_endpoints(endpoint_server_config)
        while not endpoint_server.is_connected_to("client-for-testing"):
            await trio.sleep(0)
        yield client


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
def genesis_state(eth2_config, genesis_time, genesis_validators):
    balances = tuple(validator.effective_balance for validator in genesis_validators)
    state = BeaconState.create(
        latest_block_header=BeaconBlockHeader.create(
            body_root=BeaconBlockBody.create().hash_tree_root
        ),
        validators=genesis_validators,
        balances=balances,
        config=eth2_config,
    )
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


@pytest.fixture
def get_trio_time():
    def _f():
        return trio.current_time()

    return _f


@pytest.fixture
def seconds_per_epoch(eth2_config):
    return eth2_config.SECONDS_PER_SLOT * eth2_config.SLOTS_PER_EPOCH


@pytest.fixture
def sample_bls_private_key():
    return 42


@pytest.fixture
def sample_bls_public_key(sample_bls_private_key):
    return bls.privtopub(sample_bls_private_key)


@pytest.fixture
def validator_count():
    return 16


@pytest.fixture
def genesis_validators(eth2_config, sample_bls_key_pairs):
    return tuple(
        Validator.create(
            pubkey=public_key,
            effective_balance=eth2_config.MAX_EFFECTIVE_BALANCE,
            exit_epoch=FAR_FUTURE_EPOCH,
            withdrawable_epoch=FAR_FUTURE_EPOCH,
        )
        for public_key in sample_bls_key_pairs
    )


@pytest.fixture
def sample_bls_key_pairs(validator_count):
    return create_key_pairs_for(validator_count)


@pytest.fixture
def no_op_bls():
    """
    Disables BLS cryptography across the entire process.
    """
    Eth2BLS.use_noop_backend()
