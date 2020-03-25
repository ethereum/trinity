from dataclasses import Field, dataclass, fields
from typing import Collection, Dict, Iterable, Tuple, Union, cast

from eth_utils import decode_hex, encode_hex, to_dict

from eth2.beacon.typing import Epoch, Gwei, Second, Slot

ConfigTypes = Union[Gwei, Slot, Epoch, Second, bytes, int]
EncodedConfigTypes = Union[str, int]


@to_dict
def _decoder(
    # NOTE: mypy incorrectly thinks `Field` is a generic type
    data: Dict[str, EncodedConfigTypes],
    fields: Collection[Field],  # type: ignore
) -> Iterable[Tuple[str, ConfigTypes]]:
    # NOTE: this code is unwieldly but it satisfies `mypy`
    for field in fields:
        if field.type is Gwei:
            yield field.name, Gwei(cast(int, data[field.name]))
        elif field.type is Slot:
            yield field.name, Slot(cast(int, data[field.name]))
        elif field.type is Epoch:
            yield field.name, Epoch(cast(int, data[field.name]))
        elif field.type is Second:
            yield field.name, Second(cast(int, data[field.name]))
        elif field.type is bytes:
            yield field.name, decode_hex(cast(str, data[field.name]))
        else:
            yield field.name, int(data[field.name])


@dataclass
class Eth2Config:
    # Misc
    MAX_COMMITTEES_PER_SLOT: int
    TARGET_COMMITTEE_SIZE: int
    MAX_VALIDATORS_PER_COMMITTEE: int
    MIN_PER_EPOCH_CHURN_LIMIT: int
    CHURN_LIMIT_QUOTIENT: int
    SHUFFLE_ROUND_COUNT: int
    # Genesis
    MIN_GENESIS_ACTIVE_VALIDATOR_COUNT: int
    MIN_GENESIS_TIME: int
    # Gwei values,
    MIN_DEPOSIT_AMOUNT: Gwei
    MAX_EFFECTIVE_BALANCE: Gwei
    EJECTION_BALANCE: Gwei
    EFFECTIVE_BALANCE_INCREMENT: Gwei
    # Initial values
    GENESIS_SLOT: Slot
    GENESIS_EPOCH: Epoch
    BLS_WITHDRAWAL_PREFIX: int
    # Time parameters
    SECONDS_PER_SLOT: Second
    MIN_ATTESTATION_INCLUSION_DELAY: int
    SLOTS_PER_EPOCH: int
    MIN_SEED_LOOKAHEAD: int
    MAX_SEED_LOOKAHEAD: int
    SLOTS_PER_ETH1_VOTING_PERIOD: int
    SLOTS_PER_HISTORICAL_ROOT: int
    MIN_VALIDATOR_WITHDRAWABILITY_DELAY: int
    PERSISTENT_COMMITTEE_PERIOD: int
    MIN_EPOCHS_TO_INACTIVITY_PENALTY: int
    # State list lengths
    EPOCHS_PER_HISTORICAL_VECTOR: int
    EPOCHS_PER_SLASHINGS_VECTOR: int
    HISTORICAL_ROOTS_LIMIT: int
    VALIDATOR_REGISTRY_LIMIT: int
    # Rewards and penalties
    BASE_REWARD_FACTOR: int
    WHISTLEBLOWER_REWARD_QUOTIENT: int
    PROPOSER_REWARD_QUOTIENT: int
    INACTIVITY_PENALTY_QUOTIENT: int
    MIN_SLASHING_PENALTY_QUOTIENT: int
    # Max operations per block
    MAX_PROPOSER_SLASHINGS: int
    MAX_ATTESTER_SLASHINGS: int
    MAX_ATTESTATIONS: int
    MAX_DEPOSITS: int
    MAX_VOLUNTARY_EXITS: int
    # Fork choice
    SAFE_SLOTS_TO_UPDATE_JUSTIFIED: int
    # Deposit contract
    DEPOSIT_CONTRACT_ADDRESS: bytes

    @to_dict
    def to_formatted_dict(self) -> Iterable[Tuple[str, EncodedConfigTypes]]:
        for field in fields(self):
            if field.type is bytes:
                encoded_value = encode_hex(getattr(self, field.name))
            else:
                encoded_value = getattr(self, field.name)
            yield field.name, encoded_value

    @classmethod
    def from_formatted_dict(cls, data: Dict[str, EncodedConfigTypes]) -> "Eth2Config":
        # NOTE: mypy does not recognize the kwarg unpacking here...
        return cls(**_decoder(data, fields(cls)))  # type: ignore


class CommitteeConfig:
    def __init__(self, config: Eth2Config):
        # Basic
        self.GENESIS_SLOT = config.GENESIS_SLOT
        self.GENESIS_EPOCH = config.GENESIS_EPOCH
        self.MAX_COMMITTEES_PER_SLOT = config.MAX_COMMITTEES_PER_SLOT
        self.SLOTS_PER_EPOCH = config.SLOTS_PER_EPOCH
        self.TARGET_COMMITTEE_SIZE = config.TARGET_COMMITTEE_SIZE
        self.SHUFFLE_ROUND_COUNT = config.SHUFFLE_ROUND_COUNT

        # For seed
        self.MIN_SEED_LOOKAHEAD = config.MIN_SEED_LOOKAHEAD
        self.MAX_SEED_LOOKAHEAD = config.MAX_SEED_LOOKAHEAD
        self.EPOCHS_PER_HISTORICAL_VECTOR = config.EPOCHS_PER_HISTORICAL_VECTOR
        self.EPOCHS_PER_HISTORICAL_VECTOR = config.EPOCHS_PER_HISTORICAL_VECTOR

        self.MAX_EFFECTIVE_BALANCE = config.MAX_EFFECTIVE_BALANCE
        self.EFFECTIVE_BALANCE_INCREMENT = config.EFFECTIVE_BALANCE_INCREMENT


class Eth2GenesisConfig:
    """
    Genesis parameters that might lives in
    a state or a state machine config
    but is assumed unlikely to change between forks.
    Pass this to the chains, chain_db, or other objects that need them.
    """

    def __init__(self, config: Eth2Config) -> None:
        self.GENESIS_SLOT = config.GENESIS_SLOT
        self.GENESIS_EPOCH = config.GENESIS_EPOCH
        self.SECONDS_PER_SLOT = config.SECONDS_PER_SLOT
        self.SLOTS_PER_EPOCH = config.SLOTS_PER_EPOCH
