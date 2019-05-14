from typing import (
    NamedTuple,
)

from eth.typing import (
    Address,
)
from eth2.beacon.typing import (
    Epoch,
    Gwei,
    Second,
    Shard,
    Slot,
)


class Eth2Config(NamedTuple):
    # Misc
    SHARD_COUNT: int
    TARGET_COMMITTEE_SIZE: int
    MAX_BALANCE_CHURN_QUOTIENT: int
    MAX_INDICES_PER_SLASHABLE_VOTE: int
    MAX_EXIT_DEQUEUES_PER_EPOCH: int
    SHUFFLE_ROUND_COUNT: int
    # Deposit contract
    DEPOSIT_CONTRACT_ADDRESS: Address
    DEPOSIT_CONTRACT_TREE_DEPTH: int
    # Gwei values,
    MIN_DEPOSIT_AMOUNT: Gwei
    MAX_DEPOSIT_AMOUNT: Gwei
    FORK_CHOICE_BALANCE_INCREMENT: Gwei
    EJECTION_BALANCE: Gwei
    # Initial values
    GENESIS_FORK_VERSION: int
    GENESIS_SLOT: Slot
    GENESIS_EPOCH: Epoch
    GENESIS_START_SHARD: Shard
    # `FAR_FUTURE_EPOCH`, `EMPTY_SIGNATURE` `ZERO_HASH (ZERO_HASH32)`
    # are defined in constants.py
    BLS_WITHDRAWAL_PREFIX_BYTE: bytes
    # Time parameters
    SECONDS_PER_SLOT: Second
    MIN_ATTESTATION_INCLUSION_DELAY: int
    SLOTS_PER_EPOCH: int
    MIN_SEED_LOOKAHEAD: int
    ACTIVATION_EXIT_DELAY: int
    EPOCHS_PER_ETH1_VOTING_PERIOD: int
    MIN_VALIDATOR_WITHDRAWABILITY_DELAY: int
    PERSISTENT_COMMITTEE_PERIOD: int
    # State list lengths
    SLOTS_PER_HISTORICAL_ROOT: int
    LATEST_ACTIVE_INDEX_ROOTS_LENGTH: int
    LATEST_RANDAO_MIXES_LENGTH: int
    LATEST_SLASHED_EXIT_LENGTH: int
    # Reward and penalty quotients
    BASE_REWARD_QUOTIENT: int
    WHISTLEBLOWER_REWARD_QUOTIENT: int
    ATTESTATION_INCLUSION_REWARD_QUOTIENT: int
    INACTIVITY_PENALTY_QUOTIENT: int
    MIN_PENALTY_QUOTIENT: int
    # Max operations per block
    MAX_PROPOSER_SLASHINGS: int
    MAX_ATTESTER_SLASHINGS: int
    MAX_ATTESTATIONS: int
    MAX_DEPOSITS: int
    MAX_VOLUNTARY_EXITS: int
    MAX_TRANSFERS: int


class CommitteeConfig(NamedTuple):
    GENESIS_SLOT: Slot
    GENESIS_EPOCH: Epoch
    SHARD_COUNT: Shard
    SLOTS_PER_EPOCH: int
    TARGET_COMMITTEE_SIZE: int
    SHUFFLE_ROUND_COUNT: int

    MIN_SEED_LOOKAHEAD: int
    ACTIVATION_EXIT_DELAY: int
    LATEST_ACTIVE_INDEX_ROOTS_LENGTH: int
    LATEST_RANDAO_MIXES_LENGTH: int

    @classmethod
    def from_eth2_config(cls, config: Eth2Config) -> 'CommitteeConfig':
        return cls(
            # Basic
            GENESIS_SLOT=config.GENESIS_SLOT,
            GENESIS_EPOCH=config.GENESIS_EPOCH,
            SHARD_COUNT=config.SHARD_COUNT,
            SLOTS_PER_EPOCH=config.SLOTS_PER_EPOCH,
            TARGET_COMMITTEE_SIZE=config.TARGET_COMMITTEE_SIZE,
            SHUFFLE_ROUND_COUNT=config.SHUFFLE_ROUND_COUNT,
            # For seed
            MIN_SEED_LOOKAHEAD=config.MIN_SEED_LOOKAHEAD,
            ACTIVATION_EXIT_DELAY=config.ACTIVATION_EXIT_DELAY,
            LATEST_ACTIVE_INDEX_ROOTS_LENGTH=config.LATEST_ACTIVE_INDEX_ROOTS_LENGTH,
            LATEST_RANDAO_MIXES_LENGTH=config.LATEST_RANDAO_MIXES_LENGTH,
        )
