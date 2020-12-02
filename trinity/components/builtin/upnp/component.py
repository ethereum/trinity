from argparse import (
    ArgumentParser,
    Namespace,
    _SubParsersAction,
)
import logging

from async_service import background_trio_service
from lahja import EndpointAPI
from upnp_port_forward import (
    fetch_add_portmapping_services,
    NoPortMapServiceFound,
)

from trinity.config import TrinityConfig
from trinity.extensibility import (
    TrioIsolatedComponent,
)
from trinity.components.builtin.upnp.nat import UPnPService
from trinity.constants import UPNP_EVENTBUS_ENDPOINT
from trinity.extensibility.component import Application


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

        arg_parser.add_argument(
            "--upnp-service-name",
            action="store",
            help="upnp service name for port mapping",
        )

    async def do_run(self, event_bus: EndpointAPI) -> None:
        port = self._boot_info.trinity_config.port
        upnp_service_name = self._boot_info.args.upnp_service_name
        upnp_service = UPnPService(port, event_bus, upnp_service_name)

        async with background_trio_service(upnp_service) as manager:
            await manager.wait_finished()


class UpnpServiceNamesComponent(Application):
    '''
    Fetch available upnp services that can map ports and dump them on the terminal.
    '''
    logger = logging.getLogger('trinity.components.UpnpServiceNames')

    @classmethod
    def configure_parser(cls,
                         arg_parser: ArgumentParser,
                         subparser: _SubParsersAction) -> None:
        upnp_service_names_parser = subparser.add_parser(
            'export-upnp-info',
            help='Fetch available upnp services that can map ports and dump them on the terminal',
        )

        upnp_service_names_parser.set_defaults(func=cls.run_fetch_upnp_service_names)

    @classmethod
    def run_fetch_upnp_service_names(cls, args: Namespace, trinity_config: TrinityConfig) -> None:
        try:
            upnp_service_names_by_device = fetch_add_portmapping_services()
            service_names_output = "\n"
            for service in upnp_service_names_by_device:
                service_names_output = service_names_output.join([
                    f"\n{'-' * 40}",
                    f"Device Name: {service.device_friendly_name}",
                    f"Device Location: {service.device_location}",
                    "Available UPnP services found on this device:",
                    "\n".join(f"    {service_name}" for service_name in service.service_names)
                ])
            service_names_output = "\n".join([
                service_names_output,
                "You can now try to launch Trinity with: --upnp-service-name <service name>",
                "To help Trinity supports more routers, you can submit new service names here:",
                "https://github.com/ethereum/trinity/issues - Thanks !!"
            ])
            cls.logger.info(service_names_output)
        except NoPortMapServiceFound:
            cls.logger.error("No port mapping services found")
        return


if __name__ == "__main__":
    from trinity.extensibility.component import run_trio_eth1_component
    run_trio_eth1_component(UpnpComponent)
