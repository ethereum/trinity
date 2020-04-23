from argparse import ArgumentParser, _SubParsersAction
import logging

from trinity.boot_info import BootInfo
from trinity.config import BeaconAppConfig
from trinity.constants import BEACON_TESTNET_NETWORK_ID
from trinity.extensibility import TrioComponent
from trinity.nodes.beacon.config import BeaconNodeConfig
from trinity.nodes.beacon.full import BeaconNode


class BeaconNodeComponent(TrioComponent):
    name = "Beacon Node"

    logger = logging.getLogger("trinity.components.beacon.BeaconNode[trio]")

    def __init__(self, boot_info: BootInfo) -> None:
        super().__init__(boot_info)

        trinity_config = self._boot_info.trinity_config
        beacon_app_config = trinity_config.get_app_config(BeaconAppConfig)
        config = BeaconNodeConfig.from_platform_config(
            trinity_config, beacon_app_config, boot_info.args.validator_api_port
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
            "--validator-api-port", type=int, help="API server port", default=5005
        )

    @property
    def is_enabled(self) -> bool:
        return self._boot_info.trinity_config.has_app_config(BeaconAppConfig)

    async def run(self) -> None:
        await self._node.run()
