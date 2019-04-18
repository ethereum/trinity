#!/usr/bin/env python
import json
import time


def generate_genesis_json() -> str:
    genesis_time = int(time.time()) + 30
    genesis_json = {
        "genesis_time": genesis_time
    }

    # Ensure double-quotes, and the other json format.
    return json.dumps(genesis_json)


if __name__ == '__main__':
    print(generate_genesis_json())
