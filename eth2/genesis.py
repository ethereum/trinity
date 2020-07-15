from pathlib import Path
from typing import Any, Dict, Iterable

from eth.constants import ZERO_HASH32
from eth_utils import encode_hex
from ssz.tools.dump import to_formatted_dict
from typing_extensions import Literal

from eth2.beacon.genesis import initialize_beacon_state_from_eth1
from eth2.beacon.state_machines.forks.altona.configs import ALTONA_CONFIG
from eth2.beacon.state_machines.forks.serenity.configs import SERENITY_CONFIG
from eth2.beacon.state_machines.forks.skeleton_lake.configs import (
    MINIMAL_SERENITY_CONFIG,
)
from eth2.beacon.tools.builder.initializer import (
    create_genesis_deposits_from,
    create_key_pairs_for,
    mk_genesis_key_map,
    mk_withdrawal_credentials_from,
)
from eth2.beacon.tools.misc.ssz_vector import override_lengths
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Timestamp
from eth2.configs import Eth2Config


def genesis_config_with_validators(
    config_profile: Literal["minimal", "mainnet"], genesis_time: Timestamp = None
) -> Dict[str, Any]:
    eth2_config = _get_eth2_config(config_profile)
    override_lengths(eth2_config)

    validator_count = eth2_config.MIN_GENESIS_ACTIVE_VALIDATOR_COUNT
    validator_key_pairs = create_key_pairs_for(validator_count)
    deposits = create_genesis_deposits_from(
        validator_key_pairs,
        withdrawal_credentials_provider=mk_withdrawal_credentials_from(
            eth2_config.BLS_WITHDRAWAL_PREFIX
        ),
        amount_provider=lambda _public_key: eth2_config.MAX_EFFECTIVE_BALANCE,
    )
    eth1_block_hash = ZERO_HASH32
    eth1_timestamp = eth2_config.MIN_GENESIS_TIME
    genesis_state = initialize_beacon_state_from_eth1(
        eth1_block_hash=eth1_block_hash,
        eth1_timestamp=Timestamp(eth1_timestamp),
        deposits=deposits,
        config=eth2_config,
    )

    if genesis_time:
        genesis_state = genesis_state.set("genesis_time", genesis_time)

    key_pairs = mk_genesis_key_map(validator_key_pairs, genesis_state)

    return _create_genesis_config(config_profile, eth2_config, genesis_state, key_pairs)


def genesis_config_from_state_file(
    config_profile: Literal["minimal", "mainnet"],
    genesis_state_path: Path,
    genesis_time: Timestamp = None,
) -> Dict[str, Any]:
    eth2_config = _get_eth2_config(config_profile)
    override_lengths(eth2_config)

    with open(genesis_state_path, "rb") as genesis_state_file:
        genesis_state = BeaconState.deserialize(genesis_state_file.read())

    if genesis_time:
        genesis_state = genesis_state.set("genesis_time", genesis_time)

    return _create_genesis_config(config_profile, eth2_config, genesis_state, ())


def genesis_config_with_default_state(
    config_profile: Literal["minimal", "mainnet", "altona"],
    genesis_time: Timestamp = None,
) -> Dict[str, Any]:
    eth2_config = _get_eth2_config(config_profile)
    override_lengths(eth2_config)

    genesis_state = BeaconState.create(config=eth2_config)

    if genesis_time:
        genesis_state = genesis_state.set("genesis_time", genesis_time)

    return _create_genesis_config(config_profile, eth2_config, genesis_state, ())


def format_genesis_config(config: Dict[str, Any]) -> str:
    profile = config["profile"]
    root = config["genesis_state_root"]
    genesis_time = config["genesis_state"]["genesis_time"]

    return f"[profile: {profile}, " f"root: {root}, " f"genesis_time: {genesis_time}]"


def _create_genesis_config(
    config_profile: Literal["minimal", "mainnet", "altona"],
    eth2_config: Eth2Config,
    genesis_state: BeaconState,
    key_pairs: Iterable[Dict[str, str]],
) -> Dict[str, Any]:
    return {
        "profile": config_profile,
        "eth2_config": eth2_config.to_formatted_dict(),
        "genesis_state_root": encode_hex(genesis_state.hash_tree_root),
        "genesis_validator_key_pairs": key_pairs,
        "genesis_state": to_formatted_dict(genesis_state),
    }


def _get_eth2_config(profile: str) -> Eth2Config:
    return {
        "minimal": MINIMAL_SERENITY_CONFIG,
        "mainnet": SERENITY_CONFIG,
        "altona": ALTONA_CONFIG,
    }[profile]
