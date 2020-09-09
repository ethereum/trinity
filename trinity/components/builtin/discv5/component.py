from argparse import (
    ArgumentParser,
    _SubParsersAction,
)

from ddht.boot_info import BootInfo

from async_service import run_trio_service

from lahja import EndpointAPI

from trinity.extensibility import (
    TrioIsolatedComponent,
)

from trinity.component.builtin.discv5.service import DiscoveryV5Service


class DiscoveryV5Component(TrioIsolatedComponent):
    """
    Continously discover other Ethereum nodes.
    """
    name = "discv5"
    endpoint_name = "discv5"

    @property
    def is_enabled(self) -> bool:
        return bool(self._boot_info.args.enable_discv5)

    @classmethod
    def configure_parser(cls,
                         arg_parser: ArgumentParser,
                         subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--enable-discv5",
            action="store_true",
            help="Enable v5 peer discovery",
        )
        arg_parser.add_argument(
            "--discv5-port",
            type=int,
            help="The port number that should be used for discovery v5",
        )

    async def do_run(self, event_bus: EndpointAPI) -> None:
        boot_info = BootInfo(
        )
        service = DiscoveryV5Service(
            event_bus=event_bus,
            trinity_config=self._boot_info.trinity_config,
            boot_info=boot_info,
        )
        await run_trio_service(service)
