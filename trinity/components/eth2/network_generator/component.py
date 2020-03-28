from argparse import ArgumentParser, Namespace, _SubParsersAction
import json
import logging
import pathlib
import time

from eth_utils import humanize_hash

from eth2.genesis import generate_genesis_config
from trinity.config import BeaconChainConfig, TrinityConfig
from trinity.extensibility import Application

CREATE_NETWORK_HELP = """
Produces a genesis state and a set of private keys for validators in the genesis state
"""
CONFIG_PROFILE_HELP = """
Use this profile of configuration to determine minimal parameters in the system.
"""


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
        if args.genesis_time:
            genesis_time = args.genesis_time
        elif args.genesis_delay:
            genesis_time = int(time.time()) + args.genesis_delay
        else:
            genesis_time = None

        genesis_config = generate_genesis_config(args.config_profile, genesis_time)

        output_file_path = args.output
        cls.logger.info(
            "configuration generated; genesis state has root %s with genesis time %d; "
            "writing to '%s'",
            humanize_hash(genesis_config["genesis_state"].hash_tree_root),
            genesis_config["genesis_state"].genesis_time,
            output_file_path,
        )
        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file_path, "w") as output_file:
            output_file.write(json.dumps(genesis_config))
