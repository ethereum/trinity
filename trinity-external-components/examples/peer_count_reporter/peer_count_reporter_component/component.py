# This is an example component. It is exposed in the docs and intentionally
# uses more concise style for imports

from argparse import ArgumentParser, _SubParsersAction
import asyncio

from async_service import Service, background_asyncio_service

from lahja import EndpointAPI

from trinity.boot_info import BootInfo
from trinity.extensibility import AsyncioIsolatedComponent
from trinity.protocol.common.events import PeerCountRequest
from trinity._utils.logging import get_logger


class PeerCountReporter(Service):

    def __init__(self, event_bus: EndpointAPI) -> None:
        self.event_bus = event_bus
        self.logger = get_logger('PeerCountReporter')

    async def run(self) -> None:
        self.manager.run_daemon_task(self._periodically_report_stats())
        await self.manager.wait_finished()

    async def _periodically_report_stats(self) -> None:
        while self.manager.is_running:
            try:
                response = await asyncio.wait_for(
                    self.event_bus.request(PeerCountRequest()),
                    timeout=1.0
                )
                self.logger.info("CONNECTED PEERS: %s", response.peer_count)
            except asyncio.TimeoutError:
                self.logger.warning("TIMEOUT: Waiting on PeerPool to boot")
            await asyncio.sleep(5)


class PeerCountReporterComponent(AsyncioIsolatedComponent):
    name = "Peer Count Reporter"

    @classmethod
    def configure_parser(cls,
                         arg_parser: ArgumentParser,
                         subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--report-peer-count",
            action="store_true",
            help="Report peer count to console",
        )

    @property
    def is_enabled(self) -> bool:
        return bool(self._boot_info.args.report_peer_count)

    @classmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        service = PeerCountReporter(event_bus)
        async with background_asyncio_service(service) as manager:
            await manager.wait_finished()
