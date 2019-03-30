
from py_ecc import bls
from eth2.beacon._utils.hash import (
    hash_eth2,
)


class Config(object):
    NUM_VALIDATORS = 8


privkeys = tuple(int.from_bytes(
    hash_eth2(str(i).encode('utf-8'))[:4], 'big')
    for i in range(Config.NUM_VALIDATORS)
)
index_to_pubkey = {}
keymap = {}  # pub -> priv
for i, k in enumerate(privkeys):
    index_to_pubkey[i] = bls.privtopub(k)
    keymap[bls.privtopub(k)] = k

pubkeys = list(keymap)
