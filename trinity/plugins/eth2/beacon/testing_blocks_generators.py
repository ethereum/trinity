from eth.db.atomic import AtomicDB

from py_ecc import bls
from eth2.beacon._utils.hash import (
    hash_eth2,
)
from eth2.beacon.db.chain import BeaconChainDB
from eth2.beacon.state_machines.forks.serenity.blocks import (
    SerenityBeaconBlock,
)
from eth2.beacon.tools.builder.initializer import (
    create_mock_genesis,
)
from eth2.beacon.tools.builder.proposer import (
    create_mock_block,
)
from eth2.beacon.state_machines.forks.serenity import (
    SerenityStateMachine,
)
from eth2.beacon.state_machines.forks.serenity.configs import SERENITY_CONFIG


from trinity.plugins.eth2.beacon.testing_config import Config as p


privkeys = tuple(int.from_bytes(
    hash_eth2(str(i).encode('utf-8'))[:4], 'big')
    for i in range(p.NUM_VALIDATORS)
)
keymap = {}  # pub -> priv
for k in privkeys:
    keymap[bls.privtopub(k)] = k

pubkeys = list(keymap)


config = SERENITY_CONFIG

# Something bad. :'(
config = config._replace(
    SLOTS_PER_EPOCH=p.SLOTS_PER_EPOCH,
    GENESIS_SLOT=2**32,
    GENESIS_EPOCH=2**32 // p.SLOTS_PER_EPOCH,
    TARGET_COMMITTEE_SIZE=2,
    SHARD_COUNT=2,
    MIN_ATTESTATION_INCLUSION_DELAY=2,
)


def generate_genesis_state(config, pubkeys, num_validators):
    state, block = create_mock_genesis(
        num_validators=num_validators,
        config=config,
        keymap=keymap,
        genesis_block_class=SerenityBeaconBlock,
    )
    return state, block


def get_ten_blocks_context():
    base_db = AtomicDB()
    sm_class = SerenityStateMachine.configure(
        __name__='SerenityStateMachineForTesting',
        config=config,
    )
    num_validators = p.NUM_VALIDATORS

    genesis_slot = config.GENESIS_SLOT
    chaindb = BeaconChainDB(base_db)

    # 1. Create genesis state
    print('Creating genesis state')
    genesis_state, genesis_block = create_mock_genesis(
        num_validators=num_validators,
        config=config,
        keymap=keymap,
        genesis_block_class=SerenityBeaconBlock,
    )
    for i in range(num_validators):
        assert genesis_state.validator_registry[i].is_active(genesis_slot)

    chaindb.persist_block(genesis_block, SerenityBeaconBlock)
    chaindb.persist_state(genesis_state)

    state = genesis_state
    block = genesis_block

    chain_length = 10
    blocks = (block,)

    for current_slot in range(genesis_slot + 1, genesis_slot + chain_length):
        print(f'current_slot: {current_slot}')
        attestations = ()

        block = create_mock_block(
            state=state,
            config=config,
            state_machine=sm_class(
                chaindb,
                blocks[-1],
            ),
            block_class=SerenityBeaconBlock,
            parent_block=block,
            keymap=keymap,
            slot=current_slot,
            attestations=attestations,
        )

        # Get state machine instance
        sm = sm_class(
            chaindb,
            blocks[-1],
        )
        state, _ = sm.import_block(block)

        chaindb.persist_state(state)
        chaindb.persist_block(block, SerenityBeaconBlock)

        blocks += (block,)

    return config, genesis_state, genesis_block, blocks
