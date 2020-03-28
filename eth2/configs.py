from dataclasses import Field, dataclass, fields
from typing import Any, Collection, Dict, Iterable, Optional, Tuple, Union, cast

from eth.constants import ZERO_HASH32
from eth_utils import decode_hex, encode_hex, to_dict
from ssz.tools.dump import to_formatted_dict
from typing_extensions import Literal

from eth2.beacon.genesis import initialize_beacon_state_from_eth1
from eth2.beacon.state_machines.forks.serenity.configs import SERENITY_CONFIG
from eth2.beacon.state_machines.forks.skeleton_lake.config import (
    MINIMAL_SERENITY_CONFIG,
)
from eth2.beacon.tools.builder.initializer import (
    create_genesis_deposits_from,
    create_key_pairs_for,
    mk_genesis_key_map,
    mk_withdrawal_credentials_from,
)
from eth2.beacon.tools.misc.ssz_vector import override_lengths
from eth2.beacon.typing import Epoch, Gwei, Second, Slot, Timestamp

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


def generate_genesis_config(
    config_profile: Literal["minimal", "mainnet"],
    genesis_time: Optional[Timestamp] = None,
) -> Dict[str, Any]:
    eth2_config = _get_eth2_config(config_profile)
    override_lengths(eth2_config)
    validator_count = eth2_config.MIN_GENESIS_ACTIVE_VALIDATOR_COUNT

    validator_key_pairs = create_key_pairs_for(validator_count)
    deposits = create_genesis_deposits_from(
        validator_key_pairs,
        withdrawal_credentials_provider=mk_withdrawal_credentials_from(
            eth2_config.BLS_WITHDRAWAL_PREFIX.to_bytes(1, byteorder="little")
        ),
        amount_provider=lambda _public_key: eth2_config.MAX_EFFECTIVE_BALANCE,
    )
    eth1_block_hash = ZERO_HASH32
    eth1_timestamp = eth2_config.MIN_GENESIS_TIME
    initial_state = initialize_beacon_state_from_eth1(
        eth1_block_hash=eth1_block_hash,
        eth1_timestamp=Timestamp(eth1_timestamp),
        deposits=deposits,
        config=eth2_config,
    )

    if genesis_time:
        initial_state.set("genesis_time", genesis_time)

    return {
        "eth2_config": eth2_config.to_formatted_dict(),
        "genesis_validator_key_pairs": mk_genesis_key_map(
            validator_key_pairs, initial_state
        ),
        "genesis_state": to_formatted_dict(initial_state),
    }


def _get_eth2_config(profile: str) -> Eth2Config:
    return {"minimal": MINIMAL_SERENITY_CONFIG, "mainnet": SERENITY_CONFIG}[profile]
