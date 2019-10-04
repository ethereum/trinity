import argparse

import argcomplete

CLI_PARSER_DESCRIPTION = "Trinity Eth2.0 Validator Client"
DEMO_MODE_HELP_MSG = "set configuration suitable for demonstration purposes (like ignoring a password). Do NOT use in production."
IMPORT_PARSER_HELP_MSG = (
    "import a validator private key to the keystore discovered from the configuration"
)
IMPORT_PARSER_KEY_ARGUMENT_HELP_MSG = "private key, encoded as big-endian hex"


def parse_cli_args():
    parser = argparse.ArgumentParser(description=CLI_PARSER_DESCRIPTION)
    parser.add_argument("--demo-mode", action="store_true", help=DEMO_MODE_HELP_MSG)
    subparsers = parser.add_subparsers(title="subcommands", dest="subparser_name")
    import_key_parser = subparsers.add_parser("import-key", help=IMPORT_PARSER_HELP_MSG)
    import_key_parser.add_argument(
        "private_key", type=str, help=IMPORT_PARSER_KEY_ARGUMENT_HELP_MSG
    )

    argcomplete.autocomplete(parser)
    return parser.parse_args()
