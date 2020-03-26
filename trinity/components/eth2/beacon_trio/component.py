from argparse import ArgumentParser, _SubParsersAction
import logging

from lahja import EndpointAPI
import trio

from trinity.boot_info import BootInfo
from trinity.config import BeaconAppConfig

TrioComponent = object


class BeaconNodeComponent(TrioComponent):
    name = "Beacon Node"

    logger = logging.getLogger("trinity.components.beacon.BeaconNode[trio]")

    @classmethod
    def configure_parser(
        cls, arg_parser: ArgumentParser, subparser: _SubParsersAction
    ) -> None:
        arg_parser.add_argument(
            "--validator-api-port", type=int, help="API server port", default=5005
        )

    @property
    def is_enabled(self) -> bool:
        return self._boot_info.trinity_config.has_app_config(BeaconAppConfig)

    @classmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        # boot-info
        # ->beacon-app-config
        # ->beacon-node-config
        node = BeaconNode.from_config(beacon_node_config)
        async with trio.open_nursery() as nursery:
            nursery.start_soon(node.run_with, event_bus, nursery)
