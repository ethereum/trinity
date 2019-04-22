#!/usr/bin/env python
import json

from typing import Sequence
from eth_typing import BLSPubkey


def generate_pubkeys_json(pubkeys: Sequence[BLSPubkey]) -> str:
    alice_pubkeys_json = {
        # "pubkeys": pubkeys[:len(pubkeys)]
        "privkeys": pubkeys[:(len(pubkeys) // 2)]
    }

    bob_pubkeys_json = {
        # "pubkeys": pubkeys[len(pubkeys):]
        "privkeys": pubkeys[len(pubkeys) // 2:]
    }

    return json.dumps(alice_pubkeys_json), json.dumps(bob_pubkeys_json)
