from pathlib import Path
from typing import Collection

from multiaddr import Multiaddr

from eth2.beacon.chains.testnet import MinimalChain
from trinity.config import BeaconAppConfig, BeaconChainConfig, TrinityConfig


class BeaconNodeConfig:
    def __init__(
        self,
        database_dir: Path,
        bootstrap_nodes: Collection[Multiaddr],
        preferred_nodes: Collection[Multiaddr],
        chain_config: BeaconChainConfig,
        local_node_key,
        validator_api_port,
        eth2_config,
        client_identifier,
    ) -> None:
        self._database_dir = database_dir
        self._bootstrap_nodes = bootstrap_nodes
        self._preferred_nodes = preferred_nodes
        self._chain_config = chain_config
        self._local_node_key = local_node_key
        self._validator_api_port = validator_api_port
        self._eth2_config = eth2_config
        self.client_identifier = client_identifier

        self._chain_class = MinimalChain

    @classmethod
    def from_platform_config(
        cls,
        trinity_config: TrinityConfig,
        beacon_app_config: BeaconAppConfig,
        validator_api_port,
    ) -> "BeaconNodeConfig":
        chain_config = beacon_app_config.get_chain_config()
        return cls(
            beacon_app_config.database_dir,
            beacon_app_config.bootstrap_nodes,
            beacon_app_config.preferred_nodes,
            chain_config,
            trinity_config.nodekey,
            validator_api_port,
            chain_config._eth2_config,
            beacon_app_config.client_identifier,
        )
