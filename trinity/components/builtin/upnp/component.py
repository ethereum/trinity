from argparse import (
    ArgumentParser,
    _SubParsersAction,
)

from async_service import background_trio_service
from lahja import EndpointAPI

from trinity.extensibility import (
    TrioIsolatedComponent,
)
from trinity.components.builtin.upnp.nat import UPnPService
from trinity.constants import UPNP_EVENTBUS_ENDPOINT


class UpnpComponent(TrioIsolatedComponent):
    """
    Continously try to map external to internal ip address/port using the
    Universal Plug 'n' Play (upnp) standard.
    """
    name = "Upnp"
    endpoint_name = UPNP_EVENTBUS_ENDPOINT

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

    async def do_run(self, event_bus: EndpointAPI) -> None:
        port = self._boot_info.trinity_config.port
        upnp_service = UPnPService(port, event_bus)

        async with background_trio_service(upnp_service) as manager:
            await manager.wait_finished()


if __name__ == "__main__":
    from trinity.extensibility.component import run_trio_eth1_component
    run_trio_eth1_component(UpnpComponent)
