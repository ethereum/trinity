from argparse import (
    ArgumentParser,
    _SubParsersAction,
)

from asyncio_run_in_process import run_in_process

from p2p.service import AsyncioManager

from trinity.extensibility import (
    BaseApplicationComponent,
)

from .nat import (
    UPnPService
)


class UpnpComponent(BaseApplicationComponent):
    """
    Continously try to map external to internal ip address/port using the
    Universal Plug 'n' Play (upnp) standard.
    """
    name = "Upnp"

    @property
    def is_enabled(self) -> bool:
        return not self._boot_info.args.disable_upnp

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
        service = UPnPService(self._boot_info, self.name)

        await run_in_process(AsyncioManager.run_service, service)
