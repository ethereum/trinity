
from eth2.beacon.tools.builder.proposer import (
    create_mock_block,
)
from eth2.beacon.chains.testnet.constants import (
    GENESIS_SLOT,
)
from eth2.beacon.state_machines.forks.serenity.blocks import (
    SerenityBeaconBlock,
)
from .testing_config import (
    keymap,
)


def get_ten_blocks_context(chain, gen_blocks):
    chaindb = chain.chaindb
    genesis_slot = GENESIS_SLOT
    # genesis
    block = chain.get_canonical_block_by_slot(genesis_slot)
    sm = chain.get_state_machine(block)
    state = sm.state
    blocks = (block,)
    if gen_blocks:
        chain_length = 3
        for current_slot in range(genesis_slot + 1, genesis_slot + chain_length):
            print(f'current_slot: {current_slot}')
            attestations = ()

            block = create_mock_block(
                state=state,
                config=sm.config,
                state_machine=sm,
                block_class=SerenityBeaconBlock,
                parent_block=block,
                keymap=keymap,
                slot=current_slot,
                attestations=attestations,
            )

            state, _ = sm.import_block(block)

            chaindb.persist_state(state)
            chaindb.persist_block(block, SerenityBeaconBlock)

            blocks += (block,)

    return blocks
