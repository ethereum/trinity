from argparse import (
    ArgumentParser,
    _SubParsersAction,
)

from asyncio_run_in_process import asyncio_run_in_process

from p2p.asycnio_service import Manager

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
        upnp_service = UPnPService(self.boot_info, self.name)

        async with asyncio_run_in_process(Manager.run_service, upnp_service) as proc:
            await proc.wait()
