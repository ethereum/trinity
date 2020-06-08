from pathlib import Path
from typing import Any, Dict

from eth_utils.toolz import keyfilter
from ruamel.yaml import YAML

from eth2.configs import Eth2Config


def generate_config_by_dict(dict_config: Dict[str, Any]) -> Eth2Config:
    filtered_keys = (
        "DOMAIN_",
        "ETH1_FOLLOW_DISTANCE",
        "TARGET_AGGREGATORS_PER_COMMITTEE",
        "RANDOM_SUBNETS_PER_VALIDATOR",
        "EPOCHS_PER_RANDOM_SUBNET_SUBSCRIPTION",
        # Phase 1
        "MAX_EPOCHS_PER_CROSSLINK",
        "EARLY_DERIVED_SECRET_PENALTY_MAX_FUTURE_EPOCHS",
        "EPOCHS_PER_CUSTODY_PERIOD",
        "CUSTODY_PERIOD_TO_RANDAO_PADDING",
        "SHARD_SLOTS_PER_BEACON_SLOT",
        "EPOCHS_PER_SHARD_PERIOD",
        "PHASE_1_FORK_EPOCH",
        "PHASE_1_FORK_SLOT",
        "PHASE_1_FORK_VERSION",
        "SECONDS_PER_ETH1_BLOCK",
        "INITIAL_ACTIVE_SHARDS",
        "MAX_SHARDS",
        "ONLINE_SHARDS",
        "ONLINE_PERIOD",
        "LIGHT_CLIENT_COMMITTEE_SIZE",
        "LIGHT_CLIENT_COMMITTEE_PERIOD",
        "SHARD_COMMITTEE_PERIOD",
        "SHARD_BLOCK_CHUNK_SIZE",
        "MAX_SHARD_BLOCK_CHUNKS",
        "TARGET_SHARD_BLOCK_SIZE",
        "SHARD_BLOCK_OFFSETS",
        "MAX_SHARD_BLOCKS_PER_ATTESTATION",
        "MAX_GASPRICE",
        "MIN_GASPRICE",
        "GASPRICE_ADJUSTMENT_COEFFICIENT",
        "RANDAO_PENALTY_EPOCH",
        "MAX_REVEAL_LATENESS_DECREMENT",
        "MAX_CUSTODY_KEY_REVEALS",
        "MAX_EARLY_DERIVED_SECRET_REVEALS",
        "MAX_CUSTODY_SLASHINGS",
        "EARLY_DERIVED_SECRET_REVEAL_SLOT_REWARD_MULTIPLE",
        "MINOR_REWARD_QUOTIENT",
    )

    return Eth2Config(
        **keyfilter(
            lambda name: all(key not in name for key in filtered_keys), dict_config
        ),
    )


def load_yaml_at(p: Path) -> Dict[str, Any]:
    y = YAML(typ="unsafe")
    return y.load(p)


config_cache: Dict[Path, Eth2Config] = {}


def load_config_at_path(p: Path) -> Eth2Config:
    if p in config_cache:
        return config_cache[p]

    config_data = load_yaml_at(p)
    config = generate_config_by_dict(config_data)
    config_cache[p] = config
    return config
