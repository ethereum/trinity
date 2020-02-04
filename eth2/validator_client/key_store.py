import getpass
import json
import logging
from pathlib import Path
from types import TracebackType
from typing import Any, Collection, Dict, Optional, Type

import eth_keyfile
from eth_typing import BLSPubkey
from eth_utils import decode_hex, encode_hex

from eth2._utils.bls import bls
from eth2._utils.humanize import humanize_bytes
from eth2.validator_client.abc import KeyStoreAPI
from eth2.validator_client.config import Config
from eth2.validator_client.typing import BLSPrivateKey, KeyPair

EMPTY_PASSWORD = b""


def _compute_key_pair_from_private_key_bytes(private_key_bytes: bytes) -> KeyPair:
    private_key = int.from_bytes(private_key_bytes, byteorder="big")
    return (bls.privtopub(private_key), private_key)


def _create_dir_if_missing(path: Path) -> bool:
    if not path.exists():
        path.mkdir(parents=True)
        return True
    return False


class KeyStore(KeyStoreAPI):
    """
    A ``KeyStore`` instance is a read-only repository for the private and public keys
    corresponding to the validators under the control of a client of this class.
    """

    logger = logging.getLogger("eth2.validator_client.key_store")

    def __init__(self, key_store_dir: Path, demo_mode: bool) -> None:
        self._location = key_store_dir
        self._demo_mode = demo_mode
        self._key_pairs: Dict[BLSPubkey, BLSPrivateKey] = {}
        # NOTE: ``_key_files`` is a temporary cache
        # so that there may be key pairs in ``_key_pairs``
        # that do not have key files in ``_key_files``.
        self._key_files: Dict[str, Dict[str, Any]] = {}

        self._ensure_dirs()

    @classmethod
    def from_config(cls, config: Config) -> "KeyStore":
        return cls(config.key_store_dir, config.demo_mode)

    def __enter__(self) -> KeyStoreAPI:
        self._load_validator_key_pairs()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        for key_file_id, key_file_json in self._key_files.items():
            with open(self._location / (key_file_id + ".json"), "w") as key_file_handle:
                json.dump(key_file_json, key_file_handle)

    def _ensure_dirs(self) -> None:
        did_create = _create_dir_if_missing(self._location)
        if did_create:
            self.logger.warning(
                "the key store location provided (%s) was created because it was missing",
                self._location,
            )

    def _load_validator_key_pairs(self) -> None:
        """
        Load all key pairs found in the key store directory.
        """
        for key_file in self._location.iterdir():
            public_key, private_key = self._load_key_file(key_file)
            self._key_pairs[public_key] = private_key

        self.logger.info(
            "loaded %d validator(s) from the key store", len(self._key_pairs)
        )

    def _load_key_file(self, key_file: Path) -> KeyPair:
        with open(key_file) as key_file_handle:
            keyfile_json = json.load(key_file_handle)
            public_key = decode_hex(keyfile_json["public_key"])
            if not self._demo_mode:
                self.logger.warn(
                    "please enter password for protected keyfile with public key %s:",
                    humanize_bytes(public_key),
                )
                password = getpass.getpass().encode()
            else:
                password = EMPTY_PASSWORD
            private_key = eth_keyfile.decode_keyfile_json(keyfile_json, password)
            return _compute_key_pair_from_private_key_bytes(private_key)

    @property
    def public_keys(self) -> Collection[BLSPubkey]:
        return tuple(self._key_pairs.keys())

    def import_private_key(self, encoded_private_key: str, password: bytes) -> None:
        """
        Parse the ``private_key`` provided as a hex-encoded ``str`` and then stores the
        private key and public key as a key pair in the key store on disk.
        """
        private_key_bytes = decode_hex(encoded_private_key)
        public_key, private_key = _compute_key_pair_from_private_key_bytes(
            private_key_bytes
        )
        key_file_json = eth_keyfile.create_keyfile_json(private_key_bytes, password)
        key_file_json["public_key"] = encode_hex(public_key)

        self._key_pairs[public_key] = private_key
        self._key_files[key_file_json["id"]] = key_file_json

    def private_key_for(self, public_key: BLSPubkey) -> BLSPrivateKey:
        return self._key_pairs[public_key]


class InMemoryKeyStore(KeyStoreAPI):
    def __init__(self, key_pairs: Collection[KeyPair] = ()) -> None:
        self._key_pairs = dict(key_pairs)

    @classmethod
    def from_config(
        cls, config: Config, key_pairs: Collection[KeyPair] = ()
    ) -> "InMemoryKeyStore":
        return cls(key_pairs)

    def __enter__(self) -> KeyStoreAPI:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        pass

    @property
    def public_keys(self) -> Collection[BLSPubkey]:
        return tuple(self._key_pairs.keys())

    def import_private_key(self, encoded_private_key: str, password: bytes) -> None:
        private_key_bytes = decode_hex(encoded_private_key)
        public_key, private_key = _compute_key_pair_from_private_key_bytes(
            private_key_bytes
        )
        self._key_pairs[public_key] = private_key

    def private_key_for(self, public_key: BLSPubkey) -> BLSPrivateKey:
        return self._key_pairs[public_key]
