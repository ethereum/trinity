from pathlib import Path

import ssz

from eth2.beacon.state_machines.forks.serenity.configs import SERENITY_CONFIG
from eth2.beacon.state_machines.forks.serenity.state_transitions import (
    SerenityStateTransition,
)
from eth2.beacon.tools.fixtures.config_types import Minimal
from eth2.beacon.tools.fixtures.loading import load_config_at_path
from eth2.beacon.tools.fixtures.parser import _find_project_root_dir
from eth2.beacon.tools.misc.ssz_vector import override_lengths
from eth2.beacon.types.blocks import BeaconBlock
from eth2.beacon.types.states import BeaconState

# https://github.com/ethereum/eth2.0-specs/tree/dev/test_libs
from eth2spec.fuzzing.decoder import translate_value
from eth2spec.phase0 import spec as spec_phase0
from preset_loader import loader

CONFIG_NAME = Minimal.name

# Path for config and pyspec, different loading
TESTS_ROOT_PATH = Path("eth2-fixtures")
TESTS_PATH = Path("tests")
PROJECT_ROOT_DIR = _find_project_root_dir(TESTS_ROOT_PATH)
TESTS_PATH = PROJECT_ROOT_DIR / TESTS_ROOT_PATH / TESTS_PATH
CONFIG_DIR = TESTS_PATH / CONFIG_NAME
FILE_NAME = 'config'
CONFIG_PATH = CONFIG_DIR / (FILE_NAME + '.yaml')


INTEROP_DIR = Path("tests/eth2/interop")

VALIDATE_STATE_ROOT = False

presets = loader.load_presets(CONFIG_DIR, FILE_NAME)
spec_phase0.apply_constants_preset(presets)


def run_trinity_state_transition(pre_state, blocks):
    state = pre_state.copy()
    state_transition = SerenityStateTransition(config)
    for block in blocks:
        state = state_transition.apply_state_transition(state, block)
        if VALIDATE_STATE_ROOT:
            if block.state_root != state.hash_tree_root:
                raise Exception(
                    "block's state root did not match computed state root"
                )
    return state


def run_pyspec_state_transition(pre_state, blocks):
    state = translate_value(pre_state, spec_phase0.BeaconState)
    pyspec_blocks = tuple(
        translate_value(block, spec_phase0.BeaconBlock)
        for block in blocks
    )
    for block in pyspec_blocks:
        state = spec_phase0.state_transition(
            state,
            block,
            validate_state_root=VALIDATE_STATE_ROOT,
        )
    return state


if __name__ == '__main__':
    config = SERENITY_CONFIG

    config = load_config_at_path(CONFIG_PATH)
    override_lengths(config)

    state_15_path = INTEROP_DIR / 'state_15.ssz'
    with open(state_15_path, 'rb') as f:
        encoded = f.read()
    pre_state = ssz.decode(encoded, sedes=BeaconState)
    state = pre_state.copy()

    block_16_path = INTEROP_DIR / 'block_16.ssz'
    with open(block_16_path, 'rb') as f:
        encoded = f.read()
    block = ssz.decode(encoded, sedes=BeaconBlock)
    blocks = (block,)

    # Execute state_transtion in Trinity
    trinity_post_state = run_pyspec_state_transition(pre_state, blocks)

    # Execute state_transtion in Pyspec
    pyspec_post_state = run_pyspec_state_transition(pre_state, blocks)
    for index in range(len(pyspec_post_state.balances)):
        print(
            f"trinity balances[{index}]: \t"
            f"{trinity_post_state.balances[index].to_bytes(8, 'big').hex()}"
        )
        print(
            f"pyspec balances[{index}]: \t"
            f"{pyspec_post_state.balances[index].to_bytes(8, 'big').hex()}"
        )
