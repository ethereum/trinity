from pathlib import Path
from typing import Dict

from cached_property import cached_property
from eth_typing import BLSPubkey

from eth2.beacon.typing import Slot
from eth2.validator_client.typing import BLSPrivateKey

DEFAULT_BEACON_NODE_ENDPOINT = "https://127.0.0.1:30303"
DEFAULT_VALIDATOR_DATA_DIR = Path("validator_client")
DEFAULT_KEY_STORE_DIR_SUFFIX = Path("key_store")
DEFAULT_SIGNATORY_DB_DIR_SUFFIX = Path("db") / "signatory"


class Config:
    """
    Represents data specific to a particular instance of a ``Client``.
    """

    def __init__(
        self,
        *,
        key_pairs: Dict[BLSPubkey, BLSPrivateKey] = None,
        root_data_dir: Path = None,
        beacon_node_endpoint: str = DEFAULT_BEACON_NODE_ENDPOINT,
        key_store_dir_suffix: Path = DEFAULT_KEY_STORE_DIR_SUFFIX,
        signatory_db_handle: Path = DEFAULT_SIGNATORY_DB_DIR_SUFFIX,
        slots_per_epoch: Slot = None,
        seconds_per_slot: int = None,
        genesis_time: int = None,
        demo_mode: bool = False,
    ) -> None:
        self.key_pairs = key_pairs
        self._root_data_dir = root_data_dir
        self.beacon_node_endpoint = beacon_node_endpoint
        self.key_store_dir_suffix = key_store_dir_suffix
        self.signatory_db_handle = signatory_db_handle
        self.slots_per_epoch = slots_per_epoch
        self.seconds_per_slot = seconds_per_slot
        self.genesis_time = genesis_time
        self.demo_mode = demo_mode

    @cached_property
    def seconds_per_epoch(self) -> int:
        return self.slots_per_epoch * self.seconds_per_slot

    @cached_property
    def key_store_dir(self) -> Path:
        return self._root_data_dir / self.key_store_dir_suffix
