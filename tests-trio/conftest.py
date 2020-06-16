from pathlib import Path
import secrets
import tempfile
import uuid

from eth_keys.datatypes import PrivateKey
from lahja import ConnectionConfig
from lahja.trio.endpoint import TrioEndpoint
from libp2p.crypto.secp256k1 import create_new_key_pair
from libp2p.peer.id import ID as PeerID
from multiaddr import Multiaddr
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
from eth2.beacon.types.blocks import BeaconBlockBody, BeaconBlockHeader, BeaconBlock
from eth2.beacon.genesis import get_genesis_block
from eth2.beacon.types.states import BeaconState
from eth2.beacon.types.validators import Validator
from eth2.beacon.typing import ForkDigest
from eth2.clock import Clock
from trinity._utils.version import construct_trinity_client_identifier
from trinity.config import BeaconChainConfig
from trinity.nodes.beacon.full import BeaconNode
from trinity.nodes.beacon.host import Host
from trinity.nodes.beacon.metadata import MetaData, SeqNumber
from trinity.nodes.beacon.status import Status

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
def genesis_block(genesis_state):
    return get_genesis_block(genesis_state.hash_tree_root, BeaconBlock)


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
    return bls.sk_to_pk(sample_bls_private_key)


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


@pytest.fixture
def clock(eth2_config, chain_config, seconds_per_epoch, get_trio_time):
    return Clock(
        eth2_config.SECONDS_PER_SLOT,
        chain_config.genesis_time,
        eth2_config.SLOTS_PER_EPOCH,
        seconds_per_epoch,
        time_provider=get_trio_time,
    )


@pytest.fixture
def client_id():
    return construct_trinity_client_identifier()


@pytest.fixture
def p2p_maddr():
    return Multiaddr("/ip4/127.0.0.1/tcp/13000")


@pytest.fixture
def beacon_node(
    eth2_config,
    chain_config,
    node_key,
    database_dir,
    chain_class,
    clock,
    client_id,
    p2p_maddr,
):
    validator_api_port = 0
    return BeaconNode(
        node_key,
        eth2_config,
        chain_config,
        database_dir,
        chain_class,
        clock,
        validator_api_port,
        client_id,
        p2p_maddr,
        "a",
        (),
    )


def node_key_pair(seed=b""):
    if isinstance(seed, str):
        seed = seed.encode()
    return create_new_key_pair(seed.rjust(32, b"\x00"))


def node_peer_id(node_key_pair):
    return PeerID.from_pubkey(node_key_pair.public_key)


async def _null_peer_updates(peer_id, update) -> None:
    return


@pytest.fixture
def host_factory(eth2_config, genesis_block):
    def _factory(seed, block_pool):
        key_pair = node_key_pair(seed)
        peer_id = node_peer_id(key_pair)
        listen_maddr = Multiaddr(
            f"/ip4/127.0.0.1/tcp/13{ord(seed[:1]) % 1000}/p2p/{peer_id}"
        )

        def _block_by_slot(slot):
            for block in block_pool:
                if block.slot == slot:
                    return block
            return None

        def _block_by_root(root):
            for block in block_pool:
                if block.message.hash_tree_root == root:
                    return block
            return None

        host = Host(
            key_pair,
            peer_id,
            _null_peer_updates,
            lambda: Status.create(),
            lambda _epoch: genesis_block.hash_tree_root,
            _block_by_slot,
            _block_by_root,
            lambda: MetaData.create(seq_number=SeqNumber(ord(seed[:1]))),
            lambda: ForkDigest(b"\x00\x00\x00\x00"),
            eth2_config,
        )

        return host, listen_maddr

    return _factory
