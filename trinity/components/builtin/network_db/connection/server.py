from lahja import EndpointAPI

from eth_utils import humanize_seconds, get_extended_debug_logger

from p2p.service import Service
from p2p.tracking.connection import BaseConnectionTracker

from .events import (
    BlacklistEvent,
    ShouldConnectToPeerRequest,
    ShouldConnectToPeerResponse,
)


class ConnectionTrackerServer(Service):
    logger = get_extended_debug_logger('trinity.components.network_db.ConnectionTrackerServer')
    """
    Server to handle the event bus communication for BlacklistEvent and
    ShouldConnectToPeerRequest/Response events
    """

    def __init__(self,
                 event_bus: EndpointAPI,
                 tracker: BaseConnectionTracker) -> None:
        self.tracker = tracker
        self.event_bus = event_bus

    async def handle_should_connect_to_requests(self) -> None:
        async for req in self.event_bus.stream(ShouldConnectToPeerRequest):
            self.logger.debug2('Received should connect to request: %s', req.remote)
            should_connect = await self.tracker.should_connect_to(req.remote)
            await self.event_bus.broadcast(
                ShouldConnectToPeerResponse(should_connect),
                req.broadcast_config()
            )

    async def handle_blacklist_command(self) -> None:
        async for command in self.event_bus.stream(BlacklistEvent):
            self.logger.debug2(
                'Received blacklist commmand: remote: %s | timeout: %s | reason: %s',
                command.remote,
                humanize_seconds(command.timeout_seconds),
                command.reason,
            )
            self.tracker.record_blacklist(
                command.remote,
                command.timeout_seconds,
                command.reason
            )

    async def run(self) -> None:
        self.logger.debug("Running ConnectionTrackerServer")

        self.manager.run_daemon_task(self.handle_should_connect_to_requests)
        self.manager.run_daemon_task(self.handle_blacklist_command)

        await self.manager.wait_stopped()
