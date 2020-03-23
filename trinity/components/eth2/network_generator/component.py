from argparse import ArgumentParser, Namespace, _SubParsersAction
import json
import logging
import pathlib

from eth.constants import ZERO_HASH32
from eth_utils import humanize_hash
from ssz.tools.dump import to_formatted_dict

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
            "--config-profile", help=CONFIG_PROFILE_HELP, choices=("minimal", "mainnet")
        )
        network_generator_parser.add_argument(
            "--output",
            required=False,
            type=pathlib.Path,
            help="where to save the output configuration",
        )
        network_generator_parser.set_defaults(func=cls._generate_network_as_json)

    @classmethod
    def _generate_network_as_json(
        cls, args: Namespace, trinity_config: TrinityConfig
    ) -> None:
        config = _get_eth2_config(args.config_profile)
        validator_count = config.MIN_GENESIS_ACTIVE_VALIDATOR_COUNT
        output_file_path = (
            args.output if args.output else _get_network_config_path_from()
        )

        cls.logger.info(
            "generating a configuration file at '%s'"
            " for %d validators and the genesis state containing them...",
            output_file_path,
            validator_count,
        )
        validator_key_pairs = create_key_pairs_for(validator_count)
        deposits = create_genesis_deposits_from(
            validator_key_pairs,
            withdrawal_credentials_provider=mk_withdrawal_credentials_from(
                config.BLS_WITHDRAWAL_PREFIX.to_bytes(1, byteorder="little")
            ),
            amount_provider=lambda _public_key: config.MAX_EFFECTIVE_BALANCE,
        )
        eth1_block_hash = ZERO_HASH32
        eth1_timestamp = config.MIN_GENESIS_TIME
        genesis_state = initialize_beacon_state_from_eth1(
            eth1_block_hash=eth1_block_hash,
            eth1_timestamp=Timestamp(eth1_timestamp),
            deposits=deposits,
            config=config,
        )
        output = {
            "eth2_config": config.to_formatted_dict(),
            "genesis_validator_key_pairs": mk_genesis_key_map(
                validator_key_pairs, genesis_state
            ),
            "genesis_state": to_formatted_dict(genesis_state),
        }
        cls.logger.info(
            "configuration generated; genesis state has root %s",
            humanize_hash(genesis_state.hash_tree_root),
        )
        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file_path, "w") as output_file:
            output_file.write(json.dumps(output))
