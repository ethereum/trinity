#!/usr/bin/env python
import json
import time


# NOTE: Should be `trinity/genesis.json`. Because our .py is under trinity/eth2/beacon/scripts,
#   `genesis.json` is in three layers above.
genesis_time = int(time.time())
genesis_json = {
    "genesis_time": genesis_time
}

# Ensure double-quotes, and the other json format.
formatted_json = json.dumps(genesis_json)
print(formatted_json)
