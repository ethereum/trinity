from pathlib import Path
import time

import ssz

from eth2.beacon.genesis import initialize_beacon_state_from_eth1
from eth2.beacon.tools.builder.initializer import (
    create_keypair_and_mock_withdraw_credentials,
    create_mock_deposits_and_root,
)
from eth2.beacon.tools.fixtures.config_types import Minimal
from eth2.beacon.tools.fixtures.loading import load_config_at_path, load_yaml_at
from eth2.beacon.tools.misc.ssz_vector import override_lengths

KEY_DIR = Path("eth2/beacon/scripts/quickstart_state")
KEY_SET_FILE = Path("keygen_16_validators.yaml")

RESOURCE_DIR = Path("resources")
GENESIS_FILE = Path("genesis.ssz")


def _load_config(config):
    config_file_name = KEY_DIR / Path(f"config_{config.name}.yaml")
    return load_config_at_path(config_file_name)


def _main():
    config_type = Minimal
    config = _load_config(config_type)
    override_lengths(config)

    key_set = load_yaml_at(KEY_DIR / KEY_SET_FILE)

    pubkeys, privkeys, withdrawal_credentials = create_keypair_and_mock_withdraw_credentials(
        config, key_set
    )
    keymap = {pubkey: privkey for pubkey, privkey in zip(pubkeys, privkeys)}

    deposits, _ = create_mock_deposits_and_root(
        pubkeys, keymap, config, withdrawal_credentials
    )

    eth1_block_hash = b"\x42" * 32
    # NOTE: this timestamp is a placeholder
    eth1_timestamp = 10000
    state = initialize_beacon_state_from_eth1(
        eth1_block_hash=eth1_block_hash,
        eth1_timestamp=eth1_timestamp,
        deposits=deposits,
        config=config,
    )

    genesis_time = int(time.time())
    print(f"creating genesis at time {genesis_time}")
    genesis_state = state.set("genesis_time", genesis_time)
    print(genesis_state.hash_tree_root.hex())

    genesis_file_path = RESOURCE_DIR / GENESIS_FILE
    output_file = open(genesis_file_path, "wb")
    output_file.write(ssz.encode(genesis_state))
    output_file.close()
    print(f"genesis is saved in {genesis_file_path}")


if __name__ == "__main__":
    _main()
