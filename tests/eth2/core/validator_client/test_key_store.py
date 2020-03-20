import json

import eth_keyfile
from eth_utils import decode_hex
import pytest

from eth2.validator_client.config import Config
from eth2.validator_client.key_store import InMemoryKeyStore, KeyStore


@pytest.mark.parametrize("key_store_impl", (KeyStore, InMemoryKeyStore))
def test_key_stores_can_import_private_key(
    tmp_path, sample_bls_key_pairs, key_store_impl
):
    config = Config(
        key_store_constructor=lambda config: key_store_impl.from_config(
            config, sample_bls_key_pairs
        ),
        root_data_dir=tmp_path,
    )
    some_password = b"password"
    public_key, private_key = tuple(sample_bls_key_pairs.items())[0]
    encoded_private_key = private_key.to_bytes(length=32, byteorder="big").hex()

    with config.key_store as key_store:
        key_store.import_private_key(encoded_private_key, some_password)
        assert key_store.private_key_for(public_key) == private_key


def test_key_store_can_persist_key_files(tmp_path, sample_bls_key_pairs):
    config = Config(
        key_store_constructor=lambda config: KeyStore.from_config(
            config, sample_bls_key_pairs
        ),
        root_data_dir=tmp_path,
    )
    some_password = b"password"
    public_key, private_key = tuple(sample_bls_key_pairs.items())[0]
    private_key_bytes = private_key.to_bytes(length=32, byteorder="big")
    encoded_private_key = private_key_bytes.hex()

    with config.key_store as key_store:
        assert not tuple(key_store._location.iterdir())
        key_store.import_private_key(encoded_private_key, some_password)

    key_files = tuple(key_store._location.iterdir())
    assert len(key_files) == 1

    key_file = key_files[0]
    with open(key_file) as key_file_handle:
        key_file_json = json.load(key_file_handle)
        assert decode_hex(key_file_json["public_key"]) == public_key
        assert private_key_bytes == eth_keyfile.decode_keyfile_json(
            key_file_json, some_password
        )
