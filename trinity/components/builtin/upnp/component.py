from argparse import (
    ArgumentParser,
    _SubParsersAction,
)

from async_service import run_asyncio_service
from lahja import EndpointAPI

from trinity.boot_info import BootInfo
from trinity.extensibility import (
    AsyncioIsolatedComponent,
)
from .nat import (
    UPnPService
)


class UpnpComponent(AsyncioIsolatedComponent):
    """
    Continously try to map external to internal ip address/port using the
    Universal Plug 'n' Play (upnp) standard.
    """
    name = "Upnp"

    @property
    def is_enabled(self) -> bool:
        return not bool(self._boot_info.args.disable_upnp)

    @classmethod
    def configure_parser(cls,
                         arg_parser: ArgumentParser,
                         subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--disable-upnp",
            action="store_true",
            help="Disable upnp mapping",
        )

    @classmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        port = boot_info.trinity_config.port
        upnp_service = UPnPService(port)

        await run_asyncio_service(upnp_service)
