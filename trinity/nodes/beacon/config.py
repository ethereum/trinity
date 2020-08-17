from pathlib import Path
from typing import Collection, Optional

from eth_keys.datatypes import PrivateKey
from multiaddr import Multiaddr

from eth2.configs import Eth2Config
from trinity.config import BeaconTrioAppConfig, BeaconTrioChainConfig, TrinityConfig


class BeaconNodeConfig:
    def __init__(
        self,
        database_dir: Path,
        bootstrap_nodes: Collection[Multiaddr],
        preferred_nodes: Collection[Multiaddr],
        chain_config: BeaconTrioChainConfig,
        local_node_key: PrivateKey,
        validator_api_port: int,
        eth2_config: Eth2Config,
        client_identifier: str,
        p2p_maddr: Multiaddr,
        genesis_state_ssz: Optional[Path],
    ) -> None:
        self.eth2_config = eth2_config

        self.client_identifier = client_identifier
        self.p2p_maddr = p2p_maddr
        self.bootstrap_nodes = bootstrap_nodes
        self.preferred_nodes = preferred_nodes

        self.database_dir = database_dir
        self.chain_config = chain_config
        self.local_node_key = local_node_key
        self.validator_api_port = validator_api_port

        self.chain_db_class = chain_config.chain_db_class
        self.chain_class = chain_config.chain_class
        self.genesis_state_ssz = genesis_state_ssz

    @classmethod
    def from_platform_config(
        cls, trinity_config: TrinityConfig, beacon_app_config: BeaconTrioAppConfig
    ) -> "BeaconNodeConfig":
        return cls(
            beacon_app_config.database_dir,
            beacon_app_config.bootstrap_nodes,
            beacon_app_config.preferred_nodes,
            beacon_app_config.get_chain_config(),
            trinity_config.nodekey,
            beacon_app_config.validator_api_port,
            beacon_app_config.network_config,
            beacon_app_config.client_identifier,
            beacon_app_config.p2p_maddr,
            beacon_app_config.genesis_state_ssz,
        )
