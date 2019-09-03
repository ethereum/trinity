from pathlib import Path

import pytest

from eth2._utils.bls import bls
from eth2._utils.tuple import update_tuple_item
from eth2.beacon.tools.builder.initializer import create_mock_genesis_state
from eth2.beacon.tools.fixtures.loading import load_config_at_path
from eth2.beacon.tools.fixtures.parser import _find_project_root_dir
from eth2.beacon.tools.misc.ssz_vector import override_lengths


VALIDATOR_COUNT = 1000


def get_hash_tree_root(ssz_obj):
    return ssz_obj.hash_tree_root


def update_and_rehash(state):
    index = 1
    return state.copy(
        slot=100,
        historical_roots=state.historical_roots + (b'\x56' * 32,),
        state_roots=update_tuple_item(
            state.state_roots,
            index,
            b'\x56' * 32,
        ),
        block_roots=update_tuple_item(
            state.block_roots,
            index,
            b'\x56' * 32,
        ),
        balances=update_tuple_item(
            state.balances,
            index,
            state.balances[index] + 10,
        ),
        randao_mixes=update_tuple_item(
            state.randao_mixes,
            index,
            b'\x56' * 32,
        ),
    ).hash_tree_root


@pytest.mark.parametrize(
    ("validator_count", "config_name"),
    (
        (VALIDATOR_COUNT, "minimal.yaml"),
        (VALIDATOR_COUNT, "mainnet.yaml"),
    )
)
def test_benchmark_ssz_init(benchmark, pubkeys, keymap, validator_count, config_name):
    bls.use_noop_backend()
    root_dir_path = Path("eth2")
    root_dir = _find_project_root_dir(root_dir_path)
    path = root_dir / 'tests/eth2/fixtures' / config_name
    config = load_config_at_path(path)
    override_lengths(config)
    genesis_state = create_mock_genesis_state(
        pubkeys=pubkeys[:validator_count],
        config=config,
        keymap=keymap,
    )

    args = (genesis_state,)
    benchmark.pedantic(get_hash_tree_root, args=args, rounds=1, iterations=1)


@pytest.mark.parametrize(
    ("validator_count", "config_name"),
    (
        (VALIDATOR_COUNT, "minimal.yaml"),
        (VALIDATOR_COUNT, "mainnet.yaml"),
    )
)
def test_benchmark_ssz_with_cache(benchmark, pubkeys, keymap, validator_count, config_name):
    bls.use_noop_backend()
    root_dir_path = Path("eth2")
    root_dir = _find_project_root_dir(root_dir_path)
    path = root_dir / 'tests/eth2/fixtures' / config_name
    config = load_config_at_path(path)
    override_lengths(config)
    genesis_state = create_mock_genesis_state(
        pubkeys=pubkeys[:validator_count],
        config=config,
        keymap=keymap,
    )

    args = (genesis_state,)
    genesis_state.hash_tree_root  # init hash_tree_root
    benchmark.pedantic(
        update_and_rehash,
        args=args,
        rounds=1,
        iterations=1,
    )
