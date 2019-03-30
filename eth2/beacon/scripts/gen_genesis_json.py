#!/usr/bin/env python

import json
import sys
import time
import os.path


# NOTE: Should be `trinity/genesis.json`. Because our .py is under trinity/eth2/beacon/scripts,
#   `genesis.json` is in three layers above.
file_genesis_json = f"{os.path.dirname(sys.argv[0])}/../../../genesis.json"
genesis_time = int(time.time())
genesis_json = {
    "genesis_time": genesis_time
}
with open(file_genesis_json, "w") as f:
    json.dump(genesis_json, f)
