import argparse
import logging
import signal

import argcomplete
from async_service.trio import background_trio_service
import trio

from eth2.validator_client.beacon_node import MockBeaconNode as BeaconNode
from eth2.validator_client.client import Client
from eth2.validator_client.clock import Clock
from eth2.validator_client.config import Config
from eth2.validator_client.key_store import KeyStore
from eth2.validator_client.tools.password_providers import terminal_password_provider
from trinity.cli_parser import parser, subparser

IMPORT_PARSER_HELP_MSG = (
    "import a validator private key to the keystore discovered from the configuration"
)
IMPORT_PARSER_KEY_ARGUMENT_HELP_MSG = "private key, encoded as big-endian hex"


async def _wait_for_interrupts() -> None:
    with trio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as stream:
        async for _ in stream:
            return


async def _main(
    logger: logging.Logger, config: Config, arguments: argparse.Namespace
) -> None:
    key_store = KeyStore.from_config(config)
    clock = Clock.from_config(config)
    beacon_node = BeaconNode.from_config(config)

    # with key_store.persistence():
    async with beacon_node:
        client = Client(key_store, clock, beacon_node)
        async with background_trio_service(client):
            await _wait_for_interrupts()
            logger.info("received interrupt; shutting down...")


async def _import_key(
    logger: logging.Logger, config: Config, arguments: argparse.Namespace
) -> None:
    logger.info("importing private key...")
    try:
        key_store = KeyStore(
            key_store_dir=config.key_store_dir,
            password_provider=terminal_password_provider,
        )
        with key_store.persistence(should_load_existing_key_pairs=False):
            key_store.import_private_key(arguments.private_key)
    except Exception:
        logger.exception("error importing key")


def parse_cli_args() -> argparse.ArgumentParser:
    parser.set_defaults(func=_main)

    import_key_parser = subparser.add_parser("import-key", help=IMPORT_PARSER_HELP_MSG)
    import_key_parser.add_argument(
        "private_key", type=str, help=IMPORT_PARSER_KEY_ARGUMENT_HELP_MSG
    )
    import_key_parser.set_defaults(func=_import_key)

    argcomplete.autocomplete(parser)
    return parser
