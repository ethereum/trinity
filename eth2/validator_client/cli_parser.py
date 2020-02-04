import argparse
import getpass
import logging
import signal

import argcomplete
from async_service.trio import background_trio_service
import trio

from eth2.validator_client.client import Client
from eth2.validator_client.config import Config
from eth2.validator_client.key_store import KeyStore

CLI_PARSER_DESCRIPTION = "Trinity Eth2.0 Validator Client"
DEMO_MODE_HELP_MSG = (
    "set configuration suitable for demonstration purposes (like ignoring a password)."
    " Do NOT use in production."
)
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
    client = Client.from_config(config)
    async with background_trio_service(client):
        await _wait_for_interrupts()
        logger.info("received interrupt; shutting down...")


async def _import_key(
    logger: logging.Logger, config: Config, arguments: argparse.Namespace
) -> None:
    logger.info("importing private key...")
    try:
        key_store = KeyStore.from_config(config)
        logger.warn(
            "please enter a password to protect the key on-disk (can be empty):"
        )
        password = getpass.getpass().encode()
        key_store.import_private_key(arguments.private_key, password)
    except Exception:
        logger.exception("error importing key")


def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=CLI_PARSER_DESCRIPTION)
    parser.add_argument("--demo-mode", action="store_true", help=DEMO_MODE_HELP_MSG)
    parser.set_defaults(func=_main)
    subparsers = parser.add_subparsers(title="subcommands", dest="subparser_name")
    import_key_parser = subparsers.add_parser("import-key", help=IMPORT_PARSER_HELP_MSG)
    import_key_parser.add_argument(
        "private_key", type=str, help=IMPORT_PARSER_KEY_ARGUMENT_HELP_MSG
    )
    import_key_parser.set_defaults(func=_import_key)

    argcomplete.autocomplete(parser)
    return parser.parse_args()
