from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
import asyncio

from lahja import EndpointAPI

from trinity.extensibility import (
    BaseComponent,
    ComponentService,
)
from trinity._utils.shutdown import (
    exit_with_services,
)
from .nat import (
    UPnPService
)


class UpnpComponent(BaseComponent):
    """
    Continously try to map external to internal ip address/port using the
    Universal Plug 'n' Play (upnp) standard.
    """
    name = "Upnp"

    @property
    def is_enabled(self) -> bool:
        return not self.boot_info.args.disable_upnp

    @classmethod
    def configure_parser(cls,
                         arg_parser: ArgumentParser,
                         subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--disable-upnp",
            action="store_true",
            help="Disable upnp mapping",
        )

    async def run(self) -> None:
        port = self.boot_info.trinity_config.port
        upnp_service = UPnPService(port)

        async with background_service(upnp_service) as manager:
            await manager.wait_stopped()

