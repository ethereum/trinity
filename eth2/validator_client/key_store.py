import json
import logging
from pathlib import Path
from types import TracebackType
from typing import (
    Any,
    Callable,
    Collection,
    ContextManager,
    Dict,
    Optional,
    Tuple,
    Type,
)

import eth_keyfile
from eth_typing import BLSPubkey
from eth_utils import decode_hex, encode_hex

from eth2._utils.bls import bls
from eth2._utils.humanize import humanize_bytes
from eth2.validator_client.abc import KeyStoreAPI
from eth2.validator_client.config import Config
from eth2.validator_client.tools.directory import create_dir_if_missing
from eth2.validator_client.tools.password_providers import insecure_password_provider
from eth2.validator_client.typing import BLSPrivateKey


def _serialize_private_key(private_key: BLSPrivateKey) -> bytes:
    return private_key.to_bytes(32, "little")


def _deserialize_private_key(private_key: bytes) -> BLSPrivateKey:
    return int.from_bytes(private_key, "little")


def _compute_key_pair_from_private_key_bytes(
    private_key_bytes: bytes
) -> Tuple[BLSPubkey, BLSPrivateKey]:
    private_key = _deserialize_private_key(private_key_bytes)
    return (bls.SkToPk(private_key), private_key)


class KeyStore(KeyStoreAPI):
    """
    A ``KeyStore`` instance is a repository for the private and public keys
    corresponding to the validators under the control of a client of this class.
    """

    logger = logging.getLogger("eth2.validator_client.key_store")

    def __init__(
        self,
        key_pairs: Optional[Dict[BLSPubkey, BLSPrivateKey]] = None,
        key_store_dir: Optional[Path] = None,
        password_provider: Callable[[BLSPubkey], bytes] = insecure_password_provider,
    ) -> None:
        self._key_pairs = key_pairs if key_pairs else {}
        self._key_store_dir = key_store_dir
        self._password_provider = password_provider

        self._key_files: Dict[str, Dict[str, Any]] = {}
        # Mapping of public key to key file ID
        self._key_file_index: Dict[BLSPubkey, str] = {}
        self._should_load_existing_key_pairs = False

    @classmethod
    def from_config(cls, config: Config) -> "KeyStore":
        return cls(config.key_pairs, config.key_store_dir, insecure_password_provider)

    def _ensure_dirs(self) -> None:
        did_create = create_dir_if_missing(self._key_store_dir)
        if did_create:
            self.logger.info(
                "the key store location provided (%s) was created because it was missing",
                self._key_store_dir,
            )

    def persistence(
        self, should_load_existing_key_pairs: bool = True
    ) -> ContextManager[KeyStoreAPI]:
        """
        {De,}serialize key pairs from/to disk.

        Example:
        key_store = KeyStore.from_config(config)
        with key_store.persistence():
            # keys are loaded from disk
            # keys are saved to disk upon exit
            ...
        # key_store used outside the context block
        # are available in memory, but not persisted to disk.

        NOTE: ``should_load_existing_key_pairs`` indicates if any key pairs already on-disk
        should be loaded into memory or not. This option is useful for single-key interaction, e.g.
        by a user at some UI.
        """
        self._should_load_existing_key_pairs = should_load_existing_key_pairs
        return self

    def __enter__(self) -> KeyStoreAPI:
        self._ensure_dirs()
        if self._should_load_existing_key_pairs:
            self._load_key_pairs()
            self.logger.info(
                "found %d validator key pair(s) for public key(s) %s",
                len(self.public_keys),
                tuple(map(humanize_bytes, self.public_keys)),
            )
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self._store_key_pairs()
        self._should_load_existing_key_pairs = False

    def _load_key_pairs(self) -> None:
        """
        Load all key pairs found in the key store directory.
        """
        for key_file in self._key_store_dir.iterdir():
            public_key, private_key, key_file_id = self._load_key_file(key_file)
            if public_key in self._key_pairs:
                if private_key != self._key_pairs[public_key]:
                    self.logger.warn(
                        "found a mismatch between key on disk"
                        " and key provided during construction"
                        " for public key %s",
                        encode_hex(public_key),
                    )
                continue
            self._key_pairs[public_key] = private_key
            self._key_file_index[public_key] = key_file_id

    def _load_key_file(self, key_file: Path) -> Tuple[BLSPubkey, BLSPrivateKey, str]:
        with open(key_file) as key_file_handle:
            keyfile_json = json.load(key_file_handle)
            public_key = BLSPubkey(decode_hex(keyfile_json["public_key"]))
            password = self._password_provider(public_key)
            try:
                private_key = eth_keyfile.decode_keyfile_json(keyfile_json, password)
            except ValueError:
                self.logger.error(
                    "password was incorrect for public key %s", encode_hex(public_key)
                )
                raise
            return (public_key, private_key, keyfile_json["id"])

    def _is_persisted(self, public_key: BLSPubkey) -> bool:
        return public_key in self._key_file_index

    def _store_key_pairs(self) -> None:
        for public_key, private_key in self._key_pairs.items():
            if self._is_persisted(public_key):
                continue
            password = self._password_provider(public_key)
            private_key_bytes = _serialize_private_key(private_key)
            key_file_json = eth_keyfile.create_keyfile_json(private_key_bytes, password)
            key_file_json["public_key"] = encode_hex(public_key)
            self._store_key_file(key_file_json)

    def _store_key_file(self, key_file_json: Dict[str, Any]) -> None:
        key_file_id = key_file_json["id"]
        with open(
            self._key_store_dir / (key_file_id + ".json"), "w"
        ) as key_file_handle:
            json.dump(key_file_json, key_file_handle)

    @property
    def public_keys(self) -> Collection[BLSPubkey]:
        return tuple(self._key_pairs.keys())

    def import_private_key(self, encoded_private_key: str) -> None:
        """
        Parse the ``private_key`` provided as a hex-encoded ``str`` and then stores the
        private key and public key as a key pair in the key store on disk.
        """
        private_key_bytes = decode_hex(encoded_private_key)
        public_key, private_key = _compute_key_pair_from_private_key_bytes(
            private_key_bytes
        )
        self._key_pairs[public_key] = private_key

    def private_key_for(self, public_key: BLSPubkey) -> BLSPrivateKey:
        return self._key_pairs[public_key]
