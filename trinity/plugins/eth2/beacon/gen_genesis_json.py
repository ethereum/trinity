#!/usr/bin/env python

import time


# NOTE: Should be `trinity/genesis.json`. Because our .py is under trinity/eth2/beacon/scripts,
#   `genesis.json` is in three layers above.
genesis_time = int(time.time())
genesis_json = {
    "genesis_time": genesis_time
}
print(genesis_json)
