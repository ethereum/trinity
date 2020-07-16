from pathlib import Path
from typing import Collection

from eth_keys.datatypes import PrivateKey
from multiaddr import Multiaddr

from eth2.beacon.chains.testnet.altona import BeaconChain
from eth2.configs import Eth2Config
from trinity.config import BeaconAppConfig, BeaconChainConfig, TrinityConfig


class BeaconNodeConfig:
    def __init__(
        self,
        database_dir: Path,
        orchestration_profile: str,
        bootstrap_nodes: Collection[Multiaddr],
        preferred_nodes: Collection[Multiaddr],
        chain_config: BeaconChainConfig,
        local_node_key: PrivateKey,
        validator_api_port: int,
        eth2_config: Eth2Config,
        client_identifier: str,
        p2p_maddr: Multiaddr,
    ) -> None:
        self.database_dir = database_dir
        self.orchestration_profile = orchestration_profile
        self.bootstrap_nodes = bootstrap_nodes
        self.preferred_nodes = preferred_nodes
        self.chain_config = chain_config
        self.local_node_key = local_node_key
        self.validator_api_port = validator_api_port
        self.eth2_config = eth2_config
        self.client_identifier = client_identifier
        self.p2p_maddr = p2p_maddr

        self.chain_class = BeaconChain

    @classmethod
    def from_platform_config(
        cls,
        trinity_config: TrinityConfig,
        beacon_app_config: BeaconAppConfig,
        validator_api_port: int,
        bootstrap_nodes: Collection[Multiaddr],
    ) -> "BeaconNodeConfig":
        chain_config = beacon_app_config.get_chain_config()
        return cls(
            beacon_app_config.database_dir,
            beacon_app_config.orchestration_profile,
            bootstrap_nodes,
            beacon_app_config.preferred_nodes,
            chain_config,
            trinity_config.nodekey,
            validator_api_port,
            chain_config._eth2_config,
            beacon_app_config.client_identifier,
            beacon_app_config.p2p_maddr,
        )
