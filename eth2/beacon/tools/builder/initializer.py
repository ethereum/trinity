from typing import (
    Dict,
    Sequence,
    Tuple,
    Type,
)

from eth2.beacon.constants import (
    GWEI_PER_ETH,
)
from eth2.beacon.on_startup import (
    get_genesis_block,
    get_initial_beacon_state,
)

from eth2.beacon.state_machines.configs import BeaconConfig

from eth2.beacon.types.blocks import (
    BaseBeaconBlock,
)
from eth2.beacon.types.deposits import Deposit
from eth2.beacon.types.deposit_data import DepositData
from eth2.beacon.types.deposit_input import DepositInput
from eth2.beacon.types.eth1_data import Eth1Data
from eth2.beacon.types.forks import Fork
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import (
    BLSPubkey,
    Timestamp,
)

from eth2.beacon.tools.builder.state_machine.validator import (
    sign_proof_of_possession,
)


def mock_initial_validator_deposits(num_validators: int,
                                    config: BeaconConfig,
                                    pubkeys: Sequence[BLSPubkey],
                                    keymap: Dict[BLSPubkey, int]) -> Tuple[Deposit, ...]:
    # Mock data
    withdrawal_credentials = b'\x22' * 32
    randao_commitment = b'\x33' * 32
    custody_commitment = b'\x44' * 32
    deposit_timestamp = 0
    fork = Fork(
        previous_version=config.GENESIS_FORK_VERSION,
        current_version=config.GENESIS_FORK_VERSION,
        slot=config.GENESIS_SLOT,
    )

    initial_validator_deposits = tuple(
        Deposit(
            branch=(
                b'\x11' * 32
                for j in range(10)
            ),
            index=i,
            deposit_data=DepositData(
                deposit_input=DepositInput(
                    pubkey=pubkeys[i],
                    withdrawal_credentials=withdrawal_credentials,
                    randao_commitment=randao_commitment,
                    custody_commitment=custody_commitment,
                    proof_of_possession=sign_proof_of_possession(
                        deposit_input=DepositInput(
                            pubkey=pubkeys[i],
                            withdrawal_credentials=withdrawal_credentials,
                            randao_commitment=randao_commitment,
                            custody_commitment=custody_commitment,
                        ),
                        privkey=keymap[pubkeys[i]],
                        fork=fork,
                        slot=config.GENESIS_SLOT,
                    ),
                ),
                amount=config.MAX_DEPOSIT * GWEI_PER_ETH,
                timestamp=deposit_timestamp,
            ),
        )
        for i in range(num_validators)
    )

    return initial_validator_deposits


def mock_genesis(num_validators: int,
                 config: BeaconConfig,
                 keymap: Dict[BLSPubkey, int],
                 genesis_block_class: Type[BaseBeaconBlock],
                 genesis_time: Timestamp=0) -> Tuple[BeaconState, BaseBeaconBlock]:
    latest_eth1_data = Eth1Data.create_empty_data()

    assert num_validators <= len(keymap)

    pubkeys = list(keymap)[:num_validators]

    initial_validator_deposits = mock_initial_validator_deposits(
        num_validators=num_validators,
        config=config,
        pubkeys=pubkeys,
        keymap=keymap,
    )
    state = get_initial_beacon_state(
        initial_validator_deposits=initial_validator_deposits,
        genesis_time=genesis_time,
        latest_eth1_data=latest_eth1_data,
        genesis_slot=config.GENESIS_SLOT,
        genesis_fork_version=config.GENESIS_FORK_VERSION,
        genesis_start_shard=config.GENESIS_START_SHARD,
        shard_count=config.SHARD_COUNT,
        latest_block_roots_length=config.LATEST_BLOCK_ROOTS_LENGTH,
        epoch_length=config.EPOCH_LENGTH,
        target_committee_size=config.TARGET_COMMITTEE_SIZE,
        max_deposit=config.MAX_DEPOSIT,
        latest_penalized_exit_length=config.LATEST_PENALIZED_EXIT_LENGTH,
        latest_randao_mixes_length=config.LATEST_RANDAO_MIXES_LENGTH,
        entry_exit_delay=config.ENTRY_EXIT_DELAY,
    )

    block = get_genesis_block(
        startup_state_root=state.root,
        genesis_slot=config.GENESIS_SLOT,
        block_class=genesis_block_class,
    )
    assert len(state.validator_registry) == num_validators

    return state, block
