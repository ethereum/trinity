from cached_property import cached_property
from eth_typing import BLSPubkey

from validator_client.key_store import MockKeyStore as KeyStore


class Context:
    """
    Represents data specific to a particular instance of a ``Client``.
    """

    def __init__(self, config):
        self._config = config
        self._key_store = KeyStore(config)

    @classmethod
    def from_config(cls, config):
        return cls(config)

    @cached_property
    def config(self):
        return self._config

    @cached_property
    def genesis_time(self):
        return self._config["genesis_time"]

    @cached_property
    def seconds_per_slot(self):
        return self._config["seconds_per_slot"]

    @cached_property
    def slots_per_epoch(self):
        return self._config["slots_per_epoch"]

    @cached_property
    def beacon_node_endpoint(self):
        return self._config["beacon_node_endpoint"]

    @cached_property
    def signatory_db_handle(self):
        return self._config["signatory_db_handle"]

    @cached_property
    def validator_public_keys(self):
        return self._key_store.public_keys

    @cached_property
    def key_store_location(self):
        return self._key_store.location

    def private_key_for(self, public_key: BLSPubkey) -> int:
        return self._key_store.private_key_for(public_key)
