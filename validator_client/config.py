import os
from pathlib import Path
import time
from typing import Any, Dict

DEFAULT_BEACON_NODE_ENDPOINT = "https://127.0.0.1:30303"
DEFAULT_VALIDATOR_DATA_DIR = (
    Path(os.environ["HOME"]) / ".local" / "share" / "trinity" / "validator_client"
)
DEFAULT_KEY_STORE_DIR = DEFAULT_VALIDATOR_DATA_DIR / "key_store"
DEFAULT_SIGNATORY_DB = DEFAULT_VALIDATOR_DATA_DIR / "db" / "signatory"


config = dict(
    beacon_node_endpoint=DEFAULT_BEACON_NODE_ENDPOINT,
    key_store_dir=DEFAULT_KEY_STORE_DIR,
    signatory_db_handle=DEFAULT_SIGNATORY_DB,
    slots_per_epoch=4,
    seconds_per_slot=2,
    genesis_time=int(time.time()),
    demo_mode=False,
)

# TODO turn into proper class
Config = Dict[str, Any]
