#!/usr/bin/env python
import json
import time

from py_ecc import bls
from eth2.beacon._utils.hash import (
    hash_eth2,
)


def generate_genesis_json() -> str:
    genesis_time = int(time.time()) + 30

    num_validators = 8
    privkeys = tuple(int.from_bytes(
        hash_eth2(str(i).encode('utf-8'))[:4], 'big')
        for i in range(num_validators)
    )
    keymap = {}  # pub -> priv
    for k in privkeys:
        keymap[bls.privtopub(k)] = k

    genesis_json = {
        "genesis_time": genesis_time,
        # "keymap": keymap
        "privkeys": privkeys
    }

    # Ensure double-quotes, and the other json format.
    return json.dumps(genesis_json)


if __name__ == '__main__':
    print(generate_genesis_json())
