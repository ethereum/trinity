from abc import ABC, abstractmethod
import getpass
import json
import logging
from pathlib import Path
import random
from typing import Collection, Tuple

import eth_keyfile
from eth_typing import BLSPubkey
from eth_utils import decode_hex, encode_hex, humanize_hash, int_to_big_endian

from eth2._utils.bls import bls
from validator_client.config import Config

logger = logging.getLogger("validator_client.key_store")
logger.setLevel(logging.DEBUG)

EMPTY_PASSWORD = b""

BLSPrivateKey = int
KeyPair = Tuple[BLSPubkey, BLSPrivateKey]


def _compute_key_pair_from_private_key_bytes(private_key_bytes: bytes) -> KeyPair:
    private_key = int.from_bytes(private_key_bytes, byteorder="big")
    return (bls.privtopub(private_key), private_key)


class BaseKeyStore(ABC):
    @abstractmethod
    def __init__(self, config: Config) -> "BaseKeyStore":
        ...

    @classmethod
    def from_config(cls, config: Config) -> "BaseKeyStore":
        return cls(config)

    @property
    @abstractmethod
    def location(self) -> str:
        ...

    @property
    @abstractmethod
    def public_keys(self) -> Collection[BLSPubkey]:
        ...

    @classmethod
    @abstractmethod
    def import_private_key(cls, config: Config) -> None:
        """
        Persist a private key in the key store for use between runs of the client.
        """
        ...

    @abstractmethod
    def private_key_for(self, public_key: BLSPubkey) -> int:
        ...


def _create_dir_if_missing(path: Path) -> None:
    if not path.exists():
        logger.warning(
            "The key store location provided (%s) is missing. Creating...", path
        )
        path.mkdir(parents=True)


class KeyStore(BaseKeyStore):
    """
    A ``KeyStore`` instance is a read-only repository for the private and public keys
    corresponding to the validators under the control of a client of this class.
    """

    def __init__(self, config) -> None:
        self._location = config["key_store_dir"]
        self._demo_mode = config["demo_mode"]
        self._key_pairs = {}

        self._ensure_dirs()

        self._load_validator_key_pairs()

    def _ensure_dirs(self) -> None:
        _create_dir_if_missing(self._location)

    def _load_validator_key_pairs(self) -> None:
        """
        Load all key pairs found in the key store directory.
        """
        for key_file in self._location.iterdir():
            public_key, private_key = self._load_key_file(key_file)
            self._key_pairs[public_key] = private_key

        logger.info("loaded %s validator(s) from the key store", len(self._key_pairs))

    def _load_key_file(self, key_file: Path) -> KeyPair:
        with open(key_file) as f:
            keyfile_json = json.load(f)
            public_key = decode_hex(keyfile_json["public_key"])
            if not self._demo_mode:
                logger.warn(
                    "please enter password for protected keyfile with public key %s:",
                    humanize_hash(public_key),
                )
                password = getpass.getpass().encode()
            else:
                password = EMPTY_PASSWORD
            private_key = eth_keyfile.decode_keyfile_json(keyfile_json, password)
            return _compute_key_pair_from_private_key_bytes(private_key)

    @property
    def location(self) -> str:
        return self._location

    @property
    def public_keys(self):
        return tuple(self._key_pairs.keys())

    @classmethod
    def import_private_key(cls, config: Config, private_key: str) -> None:
        """
        Parse the ``private_key`` provided as a ``str`` and then stores the
        private key and public key as a key pair in the key store on disk.
        """
        key_store_dir = config["key_store_dir"]
        _create_dir_if_missing(key_store_dir)
        private_key_bytes = decode_hex(private_key)
        public_key, _ = _compute_key_pair_from_private_key_bytes(private_key_bytes)
        logger.warn(
            "please enter a password to protect the key on-disk (can be empty):"
        )
        password = getpass.getpass().encode()
        key_file_json = eth_keyfile.create_keyfile_json(private_key_bytes, password)
        key_file_json["public_key"] = encode_hex(public_key)
        with open(key_store_dir / (key_file_json["id"] + ".json"), "w") as f:
            json.dump(key_file_json, f)

    def private_key_for(self, public_key: BLSPubkey) -> int:
        return self._key_pairs[public_key]


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


class MockKeyStore(BaseKeyStore):
    def __init__(self, config) -> None:
        self._config = config

        num_validators = 16
        self._key_pairs = dict(
            _mk_random_key_pair(index) for index in range(num_validators)
        )

    @property
    def location(self) -> str:
        return "<in-memory>"

    @property
    def public_keys(self) -> Collection[BLSPubkey]:
        return tuple(self._key_pairs.keys())

    @classmethod
    def import_private_key(cls, config: Config) -> None:
        pass

    def private_key_for(self, public_key: BLSPubkey) -> int:
        return self._key_pairs[public_key]
