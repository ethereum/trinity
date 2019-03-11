import pytest

from eth2.beacon.state_machines.forks.serenity.blocks import (
    SerenityBeaconBlock,
)
from eth2.beacon.tools.builder.initializer import (
    create_mock_genesis,
)


@pytest.fixture()
def demo_genesis_state_and_block(config, keymap, num_validators):
    genesis_state, genesis_block = create_mock_genesis(
        num_validators=num_validators,
        config=config,
        keymap=keymap,
        genesis_block_class=SerenityBeaconBlock,
    )
    return (genesis_state, genesis_block)
