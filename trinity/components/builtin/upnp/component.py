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
from trinity.components.builtin.upnp.nat import UPnPService
from trinity.constants import UPNP_EVENTBUS_ENDPOINT


class UpnpComponent(AsyncioIsolatedComponent):
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

    @classmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        port = boot_info.trinity_config.port
        upnp_service = UPnPService(port, event_bus)

        await run_asyncio_service(upnp_service)


if __name__ == "__main__":
    import asyncio
    from trinity.extensibility.component import run_standalone_eth1_component
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_standalone_eth1_component(UpnpComponent))
