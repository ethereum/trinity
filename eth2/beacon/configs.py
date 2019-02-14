from typing import (
    NamedTuple,
)

from eth.typing import (
    Address,
)
from eth2.beacon.typing import (
    EpochNumber,
    Gwei,
    Second,
    ShardNumber,
    SlotNumber,
)


BeaconConfig = NamedTuple(
    'BeaconConfig',
    (
        # Misc
        ('SHARD_COUNT', int),
        ('TARGET_COMMITTEE_SIZE', int),
        ('EJECTION_BALANCE', Gwei),
        ('MAX_BALANCE_CHURN_QUOTIENT', int),
        ('BEACON_CHAIN_SHARD_NUMBER', ShardNumber),
        ('MAX_INDICES_PER_SLASHABLE_VOTE', int),
        ('LATEST_BLOCK_ROOTS_LENGTH', int),
        ('LATEST_INDEX_ROOTS_LENGTH', int),
        ('LATEST_RANDAO_MIXES_LENGTH', int),
        ('LATEST_PENALIZED_EXIT_LENGTH', int),
        # EMPTY_SIGNATURE is defined in constants.py
        # Deposit contract
        ('DEPOSIT_CONTRACT_ADDRESS', Address),
        ('DEPOSIT_CONTRACT_TREE_DEPTH', int),
        ('MIN_DEPOSIT_AMOUNT', Gwei),
        ('MAX_DEPOSIT_AMOUNT', Gwei),
        # ZERO_HASH (ZERO_HASH32) is defined in constants.py
        # Initial values
        ('GENESIS_FORK_VERSION', int),
        ('GENESIS_SLOT', SlotNumber),
        ('GENESIS_EPOCH', EpochNumber),
        ('GENESIS_START_SHARD', ShardNumber),
        ('BLS_WITHDRAWAL_PREFIX_BYTE', bytes),
        # Time parameters
        ('SLOT_DURATION', Second),
        ('MIN_ATTESTATION_INCLUSION_DELAY', int),
        ('EPOCH_LENGTH', int),
        ('SEED_LOOKAHEAD', int),
        ('ENTRY_EXIT_DELAY', int),
        ('ETH1_DATA_VOTING_PERIOD', int),
        ('MIN_VALIDATOR_WITHDRAWAL_TIME', int),
        # Reward and penalty quotients
        ('BASE_REWARD_QUOTIENT', int),
        ('WHISTLEBLOWER_REWARD_QUOTIENT', int),
        ('INCLUDER_REWARD_QUOTIENT', int),
        ('INACTIVITY_PENALTY_QUOTIENT', int),
        # Max operations per block
        ('MAX_PROPOSER_SLASHINGS', int),
        ('MAX_ATTESTER_SLASHINGS', int),
        ('MAX_ATTESTATIONS', int),
        ('MAX_DEPOSITS', int),
        ('MAX_EXITS', int),
    )
)


class CommitteeConfig:
    def __init__(self, config: BeaconConfig):
        # Basic
        self.genesis_epoch = config.GENESIS_EPOCH
        self.shard_count = config.SHARD_COUNT
        self.epoch_length = config.EPOCH_LENGTH
        self.target_committee_size = config.TARGET_COMMITTEE_SIZE

        # For seed
        self.seed_lookahead = config.SEED_LOOKAHEAD
        self.entry_exit_delay = config.ENTRY_EXIT_DELAY
        self.latest_index_roots_length = config.LATEST_INDEX_ROOTS_LENGTH
        self.latest_randao_mixes_length = config.LATEST_INDEX_ROOTS_LENGTH
