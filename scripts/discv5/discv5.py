import argparse
import logging
import logging.config
from typing import (
    Any,
    Dict,
)
import yaml


def configure_logging(path: str) -> None:
    if path is not None:
        with open(path) as file:
            plain_logging_config = yaml.safe_load(file)
    else:
        plain_logging_config = {}

    logging_config = {
        **{
            "version": 1,
            "disable_existing_loggers": True,
        },
        **plain_logging_config,
    }
    logging.config.dictConfig(logging_config)


def load_config(path: str) -> Dict[str, Any]:
    with open(path) as file:
        config = yaml.safe_load(file)

    # we should have a function to convert private keys to public keys to node ids in the
    # IdentityScheme, but we don't at the moment
    local_private_key = decode_hex(config["local_private_key"])
    local_node_id = keccak(PrivateKey(local_private_key).public_key.to_bytes())
    config["local_node_id"] = encode_hex(local_node_id)

    return config


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trinity DiscV5")
    parser.add_argument(
        "-c", "--config",
        help="path to a YAML config file",
        required=True,
    )
    parser.add_argument(
        "-l", "--logging",
        help="path to a YAML file containing the logging config",
        required=False,
    )
    args = parser.parse_args()

    configure_logging(args.logging)

    # Import only after logging has been configured so that loggers that are created at import
    # time are configured as well.
    from eth_utils import (
        decode_hex,
        encode_hex,
        keccak,
    )
    from eth_keys.datatypes import PrivateKey
    import trio
    from p2p.discv5.plugin import DiscV5PluginConfig, run_discv5

    yaml_config = load_config(args.config)
    config = DiscV5PluginConfig(
        host=yaml_config['host'],
        port=yaml_config['port'],
        local_private_key=yaml_config['local_private_key'],
        local_node_id=yaml_config['local_node_id'],
        enrs=yaml_config['enrs'],
        routing_table=yaml_config['routing_table'],
    )

    trio.run(run_discv5, config)
