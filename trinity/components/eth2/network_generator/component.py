from argparse import ArgumentParser, Namespace, _SubParsersAction
import json
import logging
import pathlib
import time
from typing import Any, Dict, Optional

from eth.constants import ZERO_HASH32
from eth_utils import humanize_hash
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
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Timestamp
from eth2.configs import Eth2Config
from trinity.config import BeaconChainConfig, TrinityConfig
from trinity.extensibility import Application

CREATE_NETWORK_HELP = """
Produces a genesis state and a set of private keys for validators in the genesis state
"""
CONFIG_PROFILE_HELP = """
Use this profile of configuration to determine minimal parameters in the system.
"""


def _get_eth2_config(profile: str) -> Eth2Config:
    return {"minimal": MINIMAL_SERENITY_CONFIG, "mainnet": SERENITY_CONFIG}[profile]


def _get_network_config_path_from() -> pathlib.Path:
    return BeaconChainConfig.get_genesis_config_file_path()


def _adjust_genesis_time(
    initial_state: BeaconState,
    genesis_time: Optional[int],
    genesis_delay: Optional[int],
) -> BeaconState:
    if genesis_time:
        return initial_state.set("genesis_time", genesis_time)
    elif genesis_delay:
        return initial_state.set("genesis_time", int(time.time()) + genesis_delay)
    else:
        return initial_state


class NetworkGeneratorComponent(Application):
    """
    This component accepts some initial configuration and produces a genesis state and a set
    of private keys for validators in the genesis state.
    """

    logger = logging.getLogger("trinity.components.eth2.network_generator")

    @classmethod
    def configure_parser(
        cls, arg_parser: ArgumentParser, subparser: _SubParsersAction
    ) -> None:
        network_generator_parser = subparser.add_parser(
            "create-network", help=CREATE_NETWORK_HELP
        )
        network_generator_parser.add_argument(
            "--config-profile",
            help=CONFIG_PROFILE_HELP,
            choices=("minimal", "mainnet"),
            default="minimal",
        )
        network_generator_parser.add_argument(
            "--output",
            required=False,
            type=pathlib.Path,
            help="where to save the output configuration",
            default=_get_network_config_path_from(),
        )
        genesis_time_group = network_generator_parser.add_mutually_exclusive_group(
            required=False
        )
        genesis_time_group.add_argument(
            "--genesis-time", type=int, help="Unix timestamp to use as the genesis time"
        )
        genesis_time_group.add_argument(
            "--genesis-delay",
            type=int,
            help="Delay in seconds to use as the genesis time from time of execution",
        )

        network_generator_parser.set_defaults(func=cls._generate_network_as_json)

    @classmethod
    def _generate_network_as_json(
        cls, args: Namespace, trinity_config: TrinityConfig
    ) -> None:
        eth2_config = _get_eth2_config(args.config_profile)
        override_lengths(eth2_config)
        validator_count = eth2_config.MIN_GENESIS_ACTIVE_VALIDATOR_COUNT

        output_file_path = args.output

        cls.logger.info(
            "generating a configuration file at '%s'"
            " for %d validators and the genesis state containing them...",
            output_file_path,
            validator_count,
        )

        genesis_config = _generate_genesis_config(
            args.config_profile, args.genesis_time, args.genesis_delay
        )

        cls.logger.info(
            "configuration generated; genesis state has root %s with genesis time %d",
            humanize_hash(genesis_config["genesis_state"].hash_tree_root),
            genesis_config["genesis_state"].genesis_time,
        )
        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file_path, "w") as output_file:
            output_file.write(json.dumps(genesis_config))


def _generate_genesis_config(
    config_profile: Literal["minimal", "mainnet"],
    genesis_time: Timestamp = None,
    genesis_delay: int = None,
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
    genesis_state = _adjust_genesis_time(initial_state, genesis_time, genesis_delay)
    return {
        "eth2_config": eth2_config.to_formatted_dict(),
        "genesis_validator_key_pairs": mk_genesis_key_map(
            validator_key_pairs, genesis_state
        ),
        "genesis_state": to_formatted_dict(genesis_state),
    }
