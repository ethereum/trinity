import json
import logging
from pathlib import Path
from typing import Any, Dict

from ssz.tools.parse import from_formatted_dict
import trio

from eth2.beacon.tools.builder.initializer import load_genesis_key_map
from eth2.beacon.tools.misc.ssz_vector import override_lengths
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Slot
from eth2.configs import Eth2Config
from eth2.validator_client.cli_parser import parse_cli_args
from eth2.validator_client.config import Config
from eth2.validator_client.tools.directory import create_dir_if_missing
from trinity._utils.logging import LOG_FORMATTER
from trinity.bootstrap import load_trinity_config_from_parser_args
from trinity.config import ValidatorClientAppConfig
from trinity.constants import APP_IDENTIFIER_VALIDATOR_CLIENT


def _setup_logging() -> logging.Logger:
    logger = logging.getLogger("eth2.validator_client")
    logger.setLevel(logging.DEBUG)

    # TODO: ergonomic logging config...
    logging.getLogger("eth2.validator_client.client").setLevel(logging.INFO)
    logging.getLogger("eth2.validator_client.duty_scheduler").setLevel(logging.INFO)
    logging.getLogger("eth2.validator_client.signatory").setLevel(logging.INFO)
    logging.getLogger("eth2.validator_client.signatory_db").setLevel(logging.INFO)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(LOG_FORMATTER)
    logger.addHandler(stream_handler)

    return logger


def _load_genesis_config_at(genesis_config_path: Path) -> Dict[str, Any]:
    with open(genesis_config_path) as genesis_config_file:
        return json.load(genesis_config_file)


def main_validator() -> None:
    logger = _setup_logging()

    parser = parse_cli_args()
    arguments = parser.parse_args()
    trinity_config = load_trinity_config_from_parser_args(
        parser, arguments, APP_IDENTIFIER_VALIDATOR_CLIENT, (ValidatorClientAppConfig,)
    )

    # NOTE: we do not want the rest of the functionality in
    # ``trinity.bootstrap.ensure_data_dir_is_initialized
    create_dir_if_missing(trinity_config.data_dir)
    validator_client_app_config = trinity_config.get_app_config(
        ValidatorClientAppConfig
    )
    root_dir = validator_client_app_config.root_dir
    create_dir_if_missing(root_dir)

    try:
        genesis_config = _load_genesis_config_at(
            validator_client_app_config.genesis_config_path
        )
    except FileNotFoundError as e:
        raise Exception("unable to load genesis config: %s", e)

    eth2_config = Eth2Config.from_formatted_dict(genesis_config["eth2_config"])
    override_lengths(eth2_config)
    key_pairs = load_genesis_key_map(genesis_config["genesis_validator_key_pairs"])
    genesis_state = from_formatted_dict(genesis_config["genesis_state"], BeaconState)

    slots_per_epoch = Slot(eth2_config.SLOTS_PER_EPOCH)
    seconds_per_slot = eth2_config.SECONDS_PER_SLOT
    genesis_time = genesis_state.genesis_time

    config = Config(
        key_pairs=key_pairs,
        root_data_dir=root_dir,
        slots_per_epoch=slots_per_epoch,
        seconds_per_slot=seconds_per_slot,
        genesis_time=genesis_time,
    )
    # NOTE: we deviate from the multiprocess-driven Component-based
    # application machinery here until said machinery is more stable
    # with respect to boot, shutdown and general concurrent control.
    trio.run(arguments.func, logger, config, arguments)
