from argparse import ArgumentParser, _SubParsersAction
import logging
from pathlib import Path
from typing import Iterable

from eth_utils import humanize_hash, to_tuple
from multiaddr import Multiaddr

from trinity._utils.logging import get_logger
from trinity.boot_info import BootInfo
from trinity.config import ETH2_NETWORKS, BeaconTrioAppConfig
from trinity.extensibility import TrioComponent
from trinity.nodes.beacon.config import BeaconNodeConfig
from trinity.nodes.beacon.full import BeaconNode


@to_tuple
def _parse_multiaddrs_from_args(multiaddrs: str) -> Iterable[Multiaddr]:
    for multiaddr in multiaddrs.split(","):
        yield Multiaddr(multiaddr.strip())


class BeaconNodeComponent(TrioComponent):
    name = "Beacon Node"

    logger = get_logger("trinity.components.beacon.BeaconNode[trio]")

    def __init__(self, boot_info: BootInfo) -> None:
        super().__init__(boot_info)

        trinity_config = self._boot_info.trinity_config
        beacon_app_config = trinity_config.get_app_config(BeaconTrioAppConfig)
        config = BeaconNodeConfig.from_platform_config(
            trinity_config, beacon_app_config
        )
        self.logger.info(
            "using peer identity %s",
            humanize_hash(config.local_node_key.public_key.to_bytes()),
        )
        self.logger.info(
            "booting beacon node on network: %s", beacon_app_config.network_name
        )
        node = BeaconNode.from_config(config)
        self._node = node

    @classmethod
    def configure_parser(
        cls, arg_parser: ArgumentParser, subparser: _SubParsersAction
    ) -> None:
        arg_parser.add_argument(
            "--nodekey-seed", help="hex-encoded seed to use for local node key"
        )
        arg_parser.add_argument(
            "--p2p-maddr",
            type=Multiaddr,
            help="p2p host multiaddress",
            default="/ip4/127.0.0.1/tcp/13000",
        )
        arg_parser.add_argument(
            "--bootstrap-maddrs",
            type=_parse_multiaddrs_from_args,
            help="comma-separated list of maddrs for bootstrap nodes",
            default=(),
        )
        arg_parser.add_argument(
            "--preferred-maddrs",
            type=_parse_multiaddrs_from_args,
            help="comma-separated list of maddrs for preferred nodes",
            default=(),
        )
        arg_parser.add_argument(
            "--validator-api-port", type=int, help="API server port", default=5005
        )
        arg_parser.add_argument(
            "--recent-state-ssz",
            type=Path,
            help="path to a recent ssz-encoded state (for use in weak subjectivity)",
        )

        network_description_group = arg_parser.add_mutually_exclusive_group()
        network_description_group.add_argument(
            "--network",
            choices=ETH2_NETWORKS.keys(),
            default="medalla",
            help="Name of a pre-defined network",
        )
        network_description_group.add_argument(
            "--network-config",
            help="Path to a YAML configuration file describing a network",
        )

    @property
    def is_enabled(self) -> bool:
        return self._boot_info.trinity_config.has_app_config(BeaconTrioAppConfig)

    async def run(self) -> None:
        logging.getLogger("libp2p.pubsub").setLevel(logging.INFO)
        logging.getLogger("libp2p.pubsub.gossipsub").setLevel(logging.INFO)
        logging.getLogger("libp2p.transport.tcp").setLevel(logging.INFO)
        logging.getLogger("async_service.Manager").setLevel(logging.INFO)
        logging.getLogger("eth2.api.http.validator").setLevel(logging.INFO)
        logging.getLogger("eth2.beacon.chains.BeaconChain").setLevel(logging.INFO)
        await self._node.run()
