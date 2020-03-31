from argparse import ArgumentParser, _SubParsersAction
import logging

from trinity.config import BeaconAppConfig
from trinity.extensibility.trio import TrioComponent


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

    async def run(self) -> None:
        self.logger.warn("running bcc!")
