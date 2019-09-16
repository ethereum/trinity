from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
import asyncio

from lahja import EndpointAPI

from trinity.extensibility import (
    AsyncioIsolatedComponent,
)
from trinity._utils.shutdown import (
    exit_with_services,
)
from .nat import (
    UPnPService
)


class UpnpComponent(AsyncioIsolatedComponent):
    """
    Continously try to map external to internal ip address/port using the
    Universal Plug 'n' Play (upnp) standard.
    """

    @property
    def name(self) -> str:
        return "Upnp"

    def on_ready(self, manager_eventbus: EndpointAPI) -> None:
        if self.boot_info.args.disable_upnp:
            self.logger.debug("UPnP component disabled")
        else:
            self.start()

    @classmethod
    def configure_parser(cls,
                         arg_parser: ArgumentParser,
                         subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--disable-upnp",
            action="store_true",
            help="Disable upnp mapping",
        )

    def do_start(self) -> None:
        port = self.boot_info.trinity_config.port
        self.upnp_service = UPnPService(port)
        asyncio.ensure_future(exit_with_services(
            self.upnp_service,
            self._event_bus_service,
        ))
        asyncio.ensure_future(self.upnp_service.run())
