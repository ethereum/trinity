import logging
import os
from pathlib import Path
import time
from typing import Callable, Collection

import trio

from eth2._utils.bls import bls
from eth2.beacon.typing import Slot
from eth2.validator_client.abc import KeyStoreAPI
from eth2.validator_client.cli_parser import parse_cli_args
from eth2.validator_client.config import Config
from eth2.validator_client.key_store import InMemoryKeyStore
from eth2.validator_client.typing import KeyPair
from trinity._utils.logging import LOG_FORMATTER


def _setup_logging() -> logging.Logger:
    logger = logging.getLogger("eth2.validator_client")
    logger.setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(LOG_FORMATTER)
    logger.addHandler(stream_handler)

    return logger


def _random_private_key(index: int) -> int:
    """
    Using the algorithm from:
    https://github.com/ethereum/eth2.0-pm/blob/master/interop/mocked_start/keygen.py
    """
    from py_ecc.optimized_bls12_381 import curve_order
    from hashlib import sha256

    return (
        int.from_bytes(
            sha256(index.to_bytes(length=32, byteorder="little")).digest(),
            byteorder="little",
        )
        % curve_order
    )


def _mk_random_key_pair(index: int) -> KeyPair:
    private_key = _random_private_key(index)
    public_key = bls.privtopub(private_key)
    return (public_key, private_key)


def _mk_key_store_from_key_pairs(
    key_pairs: Collection[KeyPair]
) -> Callable[[Config], KeyStoreAPI]:
    def _mk_key_store(config: Config) -> KeyStoreAPI:
        return InMemoryKeyStore.from_config(config, key_pairs)

    return _mk_key_store


def main_validator() -> None:
    # TODO:
    # Merge into trinity platform
    # 1. CLI parsing
    # 2. Loading config from file and/or cmd line
    # 3. Logging
    logger = _setup_logging()
    arguments = parse_cli_args()
    root_data_dir = (
        Path(os.environ["HOME"]) /
        ".local" /
        "share" /
        "trinity" /
        "eth2" /
        "validator_client"
    )
    slots_per_epoch = Slot(4)
    seconds_per_slot = 2
    genesis_time = int(time.time()) + slots_per_epoch * seconds_per_slot + 3
    num_validators = 16
    key_pairs = tuple(_mk_random_key_pair(index) for index in range(num_validators))

    config = Config(
        key_store_constructor=_mk_key_store_from_key_pairs(key_pairs),
        root_data_dir=root_data_dir,
        slots_per_epoch=slots_per_epoch,
        seconds_per_slot=seconds_per_slot,
        genesis_time=genesis_time,
        demo_mode=arguments.demo_mode,
    )
    trio.run(arguments.func, logger, config, arguments)
