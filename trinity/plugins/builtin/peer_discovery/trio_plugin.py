from argparse import (
    ArgumentParser,
    _SubParsersAction,
)

from lahja import (
    EndpointAPI,
    BroadcastConfig,
)
from lahja.trio.endpoint import (
    TrioEndpoint,
)

from p2p.constants import (
    DISCOVERY_EVENTBUS_ENDPOINT,
)
from p2p.trio_discovery.service import (
    NoopDiscoveryRequestHandler,
)
from p2p.trio_service import run_service

from trinity.constants import (
    MAIN_EVENTBUS_ENDPOINT,
)
from trinity.endpoint import (
    TrinityEventBusEndpoint,
)
from trinity.events import (
    AvailableEndpointsUpdated,
    EventBusConnected,
    ShutdownRequest,
)




class TrinityTrioEventBusEndpoint(TrioEndpoint):
    """
    Trio flavored
    Lahja Endpoint with some Trinity specific logic.
    """

    async def request_shutdown(self, reason: str) -> None:
        """
        Perfom a graceful shutdown of Trinity. Can be called from any process.
        """
        await self.broadcast(
            ShutdownRequest(reason),
            BroadcastConfig(filter_endpoint=MAIN_EVENTBUS_ENDPOINT)
        )

    async def auto_connect_new_announced_endpoints(self) -> None:
        """
        Connect this endpoint to all new endpoints that are announced
        """
        async for event in self.stream(AvailableEndpointsUpdated):
            for config in event.available_endpoints:
                if config.name == self.name:
                    continue
                elif self.is_connected_to(config.name):
                    continue
                else:
                    self.logger.info(
                        "EventBus Endpoint %s connecting to other Endpoint %s",
                        self.name,
                        config.name
                    )
                    await self.connect_to_endpoint(config)

    async def announce_endpoint(self, config) -> None:
        """
        Announce this endpoint to the :class:`~trinity.endpoint.TrinityMainEventBusEndpoint` so
        that it will be further propagated to all other endpoints, allowing them to connect to us.
        """
        await self.broadcast(
            EventBusConnected(config),
            BroadcastConfig(filter_endpoint=MAIN_EVENTBUS_ENDPOINT)
        )


class PeerDiscoveryPlugin(TrioIsolatedPlugin):
    """
    Trio implementation of discovery plugin

    Continously discover other Ethereum nodes.
    """

    @property
    def name(self) -> str:
        return "Discovery[Trio]"

    @property
    def normalized_name(self) -> str:
        return DISCOVERY_EVENTBUS_ENDPOINT

    def on_ready(self, manager_eventbus: TrinityEventBusEndpoint) -> None:
        self.logger.info('STARTING PeerDiscoveryPlugin in on_ready()')
        self.start()

    @classmethod
    def configure_parser(cls,
                         arg_parser: ArgumentParser,
                         subparser: _SubParsersAction) -> None:
        arg_parser.add_argument(
            "--disable-discovery",
            action="store_true",
            help="Disable peer discovery",
        )

    async def do_start(self) -> None:
        self.logger.info('PLUGIN do_start() entered')
        service = NoopDiscoveryRequestHandler(self.event_bus)
        try:
            self.logger.info('RUNNING NoopDiscoveryRequestHandler service')
            async with run_service(service) as manager:
                self.logger.info('NoopDiscoveryRequestHandler service STARTED')
                await manager.wait_cancelled()
        except Exception:
            self.logger.exception('ERROR RUNNING SERVICE')
