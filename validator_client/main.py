import argparse
import logging
import signal
from typing import Optional

from async_service.trio import background_trio_service
from eth_utils import humanize_hash
import trio

from trinity._utils.logging import LOG_FORMATTER
from validator_client.cli_parser import parse_cli_args
from validator_client.client import Client
from validator_client.config import Config, config
from validator_client.context import Context
from validator_client.key_store import KeyStore


def _setup_logging() -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(LOG_FORMATTER)
    logger.addHandler(stream_handler)

    return logger


async def _wait_for_interrupts(logger: logging.Logger) -> None:
    with trio.open_signal_receiver(signal.SIGINT) as stream:
        async for _ in stream:
            logger.info("received interrupt; shutting down...")
            return


async def _main(
    logger: logging.Logger, config: Config, arguments: argparse.Namespace
) -> None:

    # TODO sanity check the validator key pairs we find are valid
    # perhaps by fetching their indices in the state and adding them to the following
    # logging statement.
    context = Context.from_config(config)
    validator_public_keys = context.validator_public_keys
    logger.info(
        "found %d validator key pair(s) at %s for public key(s) %s",
        len(validator_public_keys),
        context.key_store_location,
        tuple(map(humanize_hash, validator_public_keys)),
    )
    client = Client(context)

    async with background_trio_service(client):
        await _wait_for_interrupts(logger)


async def _import_key(
    logger: logging.Logger, config: Config, arguments: argparse.Namespace
) -> None:
    logger.info("importing private key...")
    try:
        KeyStore.import_private_key(config, arguments.private_key)
    except Exception as e:
        logger.warn(e)


def _fn_for_command(command: Optional[str]):
    return {None: _main, "import-key": _import_key}[command]


def _set_demo_mode_in_config(config: Config, demo_mode: bool) -> None:
    config["demo_mode"] = demo_mode


def main() -> None:
    # TODO:
    # load config from CLI parser
    # load config from file
    # ... including logging config which can then go to `_setup_logging`
    logger = _setup_logging()
    arguments = parse_cli_args()
    fn = _fn_for_command(arguments.subparser_name)
    _set_demo_mode_in_config(config, arguments.demo_mode)
    trio.run(fn, logger, config, arguments)
