from lahja import EndpointAPI

from async_service import Service
from eth_utils import get_extended_debug_logger, humanize_seconds

from p2p.tracking.connection import BaseConnectionTracker

from .events import (
    BlacklistEvent,
    GetBlacklistedPeersRequest,
    GetBlacklistedPeersResponse,
)


class ConnectionTrackerServer(Service):
    """
    Server to handle the event bus communication for BlacklistEvent and
    GetBlacklistedPeersRequest/Response events
    """
    logger = get_extended_debug_logger('trinity.components.network_db.ConnectionTrackerServer')

    def __init__(self,
                 event_bus: EndpointAPI,
                 tracker: BaseConnectionTracker) -> None:
        self.tracker = tracker
        self.event_bus = event_bus

    async def handle_get_blacklisted_requests(self) -> None:
        async for req in self.event_bus.stream(GetBlacklistedPeersRequest):
            self.logger.debug2('Received get_blacklisted request')
            blacklisted = await self.tracker.get_blacklisted()
            await self.event_bus.broadcast(
                GetBlacklistedPeersResponse(blacklisted),
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

        self.manager.run_daemon_task(
            self.handle_blacklist_command,
            name='ConnectionTrackerServer.handle_blacklist_command',
        )
        self.manager.run_daemon_task(
            self.handle_get_blacklisted_requests,
            name='ConnectionTrackerServer.handle_get_blacklisted_requests,',
        )

        await self.manager.wait_finished()
