import json

import eth_keyfile
from eth_utils import decode_hex
import pytest

from eth2.validator_client.key_store import KeyStore


def test_key_store_can_import_private_key(tmp_path, sample_bls_key_pairs):
    public_key, private_key = tuple(sample_bls_key_pairs.items())[0]
    encoded_private_key = private_key.to_bytes(32, "little").hex()

    key_store = KeyStore(sample_bls_key_pairs)
    key_store.import_private_key(encoded_private_key)
    assert key_store.private_key_for(public_key) == private_key


@pytest.mark.parametrize(("has_key_pairs"), [(True), (False)])
def test_key_store_can_persist_key_files(tmp_path, sample_bls_key_pairs, has_key_pairs):
    some_password = b"password"
    public_key, private_key = tuple(sample_bls_key_pairs.items())[0]
    private_key_bytes = private_key.to_bytes(32, "little")
    encoded_private_key = private_key_bytes.hex()

    if has_key_pairs:
        key_pairs = sample_bls_key_pairs
    else:
        key_pairs = {}
    key_store = KeyStore(
        key_pairs=key_pairs,
        key_store_dir=tmp_path,
        password_provider=lambda _public_key: some_password,
    )

    with key_store.persistence():
        assert not tuple(key_store._key_store_dir.iterdir())
        key_store.import_private_key(encoded_private_key)

    key_files = tuple(key_store._key_store_dir.iterdir())
    assert len(key_files) == 1

    key_file = key_files[0]
    with open(key_file) as key_file_handle:
        key_file_json = json.load(key_file_handle)
        assert decode_hex(key_file_json["public_key"]) == public_key
        assert private_key_bytes == eth_keyfile.decode_keyfile_json(
            key_file_json, some_password
        )
