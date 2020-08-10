from argparse import ArgumentParser, Namespace, _SubParsersAction
import json
import logging
import pathlib
import time

from eth2.genesis import (
    format_genesis_config,
    genesis_config_from_state_file,
    genesis_config_with_default_state,
    genesis_config_with_validators,
)
from trinity.config import BeaconChainConfig, TrinityConfig
from trinity.extensibility import Application

CREATE_NETWORK_HELP = """
Produces a genesis state and a set of private keys for validators in the genesis state
"""
CONFIG_PROFILE_HELP = """
Use this profile of configuration to determine minimal parameters in the system.
"""


def _get_network_config_path_for(profile: str) -> pathlib.Path:
    return BeaconChainConfig.get_genesis_config_file_path(profile)


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
            choices=("minimal", "mainnet", "medalla"),
            default="minimal",
        )
        network_generator_parser.add_argument(
            "--output", type=pathlib.Path, help="Path to save the output configuration"
        )
        genesis_state_group = network_generator_parser.add_mutually_exclusive_group()
        genesis_state_group.add_argument(
            "--generate-validator-keys",
            action="store_true",
            help="Initialize the chain state from self-generated validator keys",
        )
        genesis_state_group.add_argument(
            "--genesis-state",
            type=pathlib.Path,
            help="Path to an SSZ file that contains the initial chain state",
        )
        genesis_time_group = network_generator_parser.add_mutually_exclusive_group()
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
        cls.logger.info(f"generating genesis {args.config_profile} config")

        if args.output:
            output_file_path = args.output
        else:
            output_file_path = _get_network_config_path_for(args.config_profile)

        if args.genesis_time:
            genesis_time = args.genesis_time
            cls.logger.info(f"genesis time specified by user: {genesis_time}")
        elif args.genesis_delay:
            genesis_time = int(time.time()) + args.genesis_delay
            cls.logger.info(f"genesis delay specified by user: {args.genesis_delay}")
        else:
            genesis_time = None

        if args.genesis_state:
            genesis_config = genesis_config_from_state_file(
                args.config_profile, args.genesis_state, genesis_time=genesis_time
            )
        elif args.generate_validator_keys:
            genesis_config = genesis_config_with_validators(
                args.config_profile, genesis_time=genesis_time
            )
        else:
            genesis_config = genesis_config_with_default_state(
                args.config_profile, genesis_time=genesis_time
            )

        cls.logger.info(
            f"configuration generated: {format_genesis_config(genesis_config)}"
        )
        cls.logger.info(f"writing config to {output_file_path}")
        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file_path, "w") as output_file:
            json.dump(genesis_config, output_file)
