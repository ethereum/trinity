#!/usr/bin/env python

import json
import time

genesis_time = int(time.time())
genesis_json = {
    "genesis_time": genesis_time
}
with open("genesis.json", "w") as f:
    json.dump(genesis_json, f)
