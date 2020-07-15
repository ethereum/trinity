from argparse import ArgumentParser, _SubParsersAction
import logging
from typing import Iterable

from eth_utils import to_tuple
from multiaddr import Multiaddr

from trinity.boot_info import BootInfo
from trinity.config import BeaconAppConfig
from trinity.constants import BEACON_TESTNET_NETWORK_ID
from trinity.extensibility import TrioComponent
from trinity.nodes.beacon.config import BeaconNodeConfig
from trinity.nodes.beacon.full import BeaconNode


@to_tuple
def _parse_multiaddrs_from_args(multiaddrs: str) -> Iterable[Multiaddr]:
    for multiaddr in multiaddrs.split(","):
        yield Multiaddr(multiaddr.strip())


class BeaconNodeComponent(TrioComponent):
    name = "Beacon Node"

    logger = logging.getLogger("trinity.components.beacon.BeaconNode[trio]")

    def __init__(self, boot_info: BootInfo) -> None:
        super().__init__(boot_info)

        trinity_config = self._boot_info.trinity_config
        beacon_app_config = trinity_config.get_app_config(BeaconAppConfig)
        config = BeaconNodeConfig.from_platform_config(
            boot_info.args.config_profile,
            trinity_config,
            beacon_app_config,
            boot_info.args.validator_api_port,
            boot_info.args.bootstrap_nodes,
        )
        node = BeaconNode.from_config(config)
        self._node = node

    @classmethod
    def configure_parser(
        cls, arg_parser: ArgumentParser, subparser: _SubParsersAction
    ) -> None:
        # NOTE: this workaround puts the testnet data into its own `datadir`
        # TODO integrate into the greater trinity config so we do not need workarounds like this
        arg_parser.set_defaults(network_id=BEACON_TESTNET_NETWORK_ID)

        arg_parser.add_argument(
            "--bootstrap-nodes",
            type=_parse_multiaddrs_from_args,
            help="bootstrap nodes",
            default=(),
        )
        arg_parser.add_argument(
            "--preferred-nodes",
            type=_parse_multiaddrs_from_args,
            help="preferred nodes",
            default=(),
        )

        arg_parser.add_argument(
            "--validator-api-port", type=int, help="API server port", default=5005
        )

        arg_parser.add_argument(
            "--p2p-maddr",
            type=Multiaddr,
            help="p2p host multiaddress",
            default="/ip4/127.0.0.1/tcp/13000",
        )

        arg_parser.add_argument(
            "--orchestration-profile",
            help="[temporary developer option] manage several beacon nodes on one machine",
            default="a",
        )

        arg_parser.add_argument(
            "--config-profile",
            help="the profile used to generate the genesis config",
            choices=("minimal", "mainnet", "altona"),
            default="minimal",
        )

    @property
    def is_enabled(self) -> bool:
        return self._boot_info.trinity_config.has_app_config(BeaconAppConfig)

    async def run(self) -> None:
        logging.getLogger("libp2p.pubsub").setLevel(logging.INFO)
        logging.getLogger("libp2p.pubsub.gossipsub").setLevel(logging.INFO)
        logging.getLogger("libp2p.transport.tcp").setLevel(logging.INFO)
        logging.getLogger("async_service.Manager").setLevel(logging.INFO)
        logging.getLogger("eth2.api.http.validator").setLevel(logging.INFO)
        logging.getLogger("eth2.beacon.chains.BeaconChain").setLevel(logging.INFO)
        await self._node.run()
