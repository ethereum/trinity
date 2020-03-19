from argparse import ArgumentParser, Namespace, _SubParsersAction
import json
import logging
import pathlib
import time
from typing import Any, Callable, Dict, Tuple

from eth.constants import ZERO_HASH32
from eth_typing import BLSPubkey
from eth_utils import encode_hex
from ssz.tools.dump import to_formatted_dict

from eth2.beacon.genesis import initialize_beacon_state_from_eth1
from eth2.beacon.state_machines.forks.serenity.configs import SERENITY_CONFIG
from eth2.beacon.state_machines.forks.skeleton_lake.config import (
    MINIMAL_SERENITY_CONFIG,
)
from eth2.beacon.tools.builder.initializer import (
    create_genesis_deposits_from,
    create_key_pairs_for,
    mk_withdrawal_credentials_from,
)
from eth2.beacon.types.states import BeaconState
from eth2.configs import Eth2Config, serialize
from trinity.config import TrinityConfig
from trinity.extensibility import Application

CONFIG_PROFILE_HELP = """
Use this profile of configuration to determine minimal parameters in the system.
"""


def _get_eth2_config(profile: str) -> Eth2Config:
    return {"minimal": MINIMAL_SERENITY_CONFIG, "mainnet": SERENITY_CONFIG}[profile]


def _identity(x: Any) -> Any:
    return x


def _mk_genesis_key_map(
    key_pairs: Dict[BLSPubkey, int],
    genesis_state: BeaconState,
    public_key_codec: Callable[[BLSPubkey], str] = _identity,
    private_key_codec: Callable[[int], str] = _identity,
) -> Tuple[Dict[str, Any]]:
    key_map = ()
    for _, validator in enumerate(genesis_state.validators):
        public_key = validator.pubkey
        private_key = key_pairs[public_key]
        key_map += (
            {
                "public_key": public_key_codec(public_key),
                "private_key": private_key_codec(private_key),
            },
        )
    return key_map


def _encode_private_key_as_hex(private_key: int) -> str:
    return encode_hex(private_key.to_bytes(32, byteorder="little"))


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
            "create-network",
            help="Produces a genesis state and a set of private keys for validators in the genesis state",
        )
        network_generator_parser.add_argument(
            "--config-profile", help=CONFIG_PROFILE_HELP, choices=("minimal", "mainnet")
        )
        network_generator_parser.add_argument(
            "--output", type=pathlib.Path, help="where to save the output configuration"
        )
        network_generator_parser.set_defaults(func=cls._generate_network_as_json)

    @classmethod
    def _generate_network_as_json(
        cls, args: Namespace, trinity_config: TrinityConfig
    ) -> None:
        config = _get_eth2_config(args.config_profile)
        validator_count = config.MIN_GENESIS_ACTIVE_VALIDATOR_COUNT
        cls.logger.info(
            "generating a configuration file at '%s' for %d validators and the genesis state containing them...",
            args.output,
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
        eth1_timestamp = int(time.time())
        genesis_state = initialize_beacon_state_from_eth1(
            eth1_block_hash=eth1_block_hash,
            eth1_timestamp=eth1_timestamp,
            deposits=deposits,
            config=config,
        )
        output = {
            "eth2_config": serialize(config),
            "genesis_validator_key_pairs": _mk_genesis_key_map(
                validator_key_pairs,
                genesis_state,
                public_key_codec=encode_hex,
                private_key_codec=_encode_private_key_as_hex,
            ),
            "genesis_state": to_formatted_dict(genesis_state),
        }
        with open(args.output, "w") as output_file:
            output_file.write(json.dumps(output))
