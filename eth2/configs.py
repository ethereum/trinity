from dataclasses import Field, dataclass, fields
from typing import Collection, Dict, Iterable, Tuple, Union, cast

from eth_utils import decode_hex, encode_hex, to_dict

from eth2.beacon.typing import Epoch, Gwei, Second, Slot, Version

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
        elif field.type is Version:
            yield field.name, Version(decode_hex(cast(str, data[field.name])))
        else:
            yield field.name, int(data[field.name])


@dataclass(eq=True, frozen=True)
class Eth2Config:
    # Misc
    MAX_COMMITTEES_PER_SLOT: int
    TARGET_COMMITTEE_SIZE: int
    MAX_VALIDATORS_PER_COMMITTEE: int
    MIN_PER_EPOCH_CHURN_LIMIT: int
    CHURN_LIMIT_QUOTIENT: int
    SHUFFLE_ROUND_COUNT: int
    HYSTERESIS_QUOTIENT: int
    HYSTERESIS_DOWNWARD_MULTIPLIER: int
    HYSTERESIS_UPWARD_MULTIPLIER: int
    # Genesis
    MIN_GENESIS_ACTIVE_VALIDATOR_COUNT: int
    MIN_GENESIS_TIME: int
    # Gwei values,
    MIN_DEPOSIT_AMOUNT: Gwei
    MAX_EFFECTIVE_BALANCE: Gwei
    EJECTION_BALANCE: Gwei
    EFFECTIVE_BALANCE_INCREMENT: Gwei
    # Initial values
    GENESIS_FORK_VERSION: Version
    BLS_WITHDRAWAL_PREFIX: bytes
    # Time parameters
    GENESIS_DELAY: int
    SECONDS_PER_SLOT: Second
    MIN_ATTESTATION_INCLUSION_DELAY: int
    SLOTS_PER_EPOCH: int
    MIN_SEED_LOOKAHEAD: int
    MAX_SEED_LOOKAHEAD: int
    MIN_EPOCHS_TO_INACTIVITY_PENALTY: int
    EPOCHS_PER_ETH1_VOTING_PERIOD: int
    SLOTS_PER_HISTORICAL_ROOT: int
    MIN_VALIDATOR_WITHDRAWABILITY_DELAY: int
    SHARD_COMMITTEE_PERIOD: int
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
    DEPOSIT_CHAIN_ID: int
    DEPOSIT_NETWORK_ID: int
    DEPOSIT_CONTRACT_ADDRESS: bytes

    @to_dict
    def to_formatted_dict(self) -> Iterable[Tuple[str, EncodedConfigTypes]]:
        for field in fields(self):
            if field.type in (bytes, Version):
                encoded_value = encode_hex(getattr(self, field.name))
            else:
                encoded_value = getattr(self, field.name)
            yield field.name, encoded_value

    @classmethod
    def from_formatted_dict(cls, data: Dict[str, EncodedConfigTypes]) -> "Eth2Config":
        # NOTE: mypy does not recognize the kwarg unpacking here...
        return cls(**_decoder(data, fields(cls)))  # type: ignore
