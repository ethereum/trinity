from typing import (
    Callable,
    Tuple,
)
from lahja import (
    BaseEvent,
    BroadcastConfig,
    Endpoint,
    filter_none,
    ListenerConfig,
)

from trinity.constants import (
    MAIN_EVENTBUS_ENDPOINT,
)
from trinity.events import (
    AvailableEndpointsUpdated,
    EventBusConnected,
    ShutdownRequest,
)


class TrinityEventBusEndpoint(Endpoint):
    """
    Lahja Endpoint with some Trinity specific logic.
    """

    def request_shutdown(self, reason: str) -> None:
        """
        Perfom a graceful shutdown of Trinity. Can be called from any process.
        """
        self.broadcast(
            ShutdownRequest(reason),
            BroadcastConfig(filter_endpoint=MAIN_EVENTBUS_ENDPOINT)
        )

    def add_other_listener_endpoints(self,
                                     ev: AvailableEndpointsUpdated) -> None:

        for connection_config in ev.available_endpoints:
            if connection_config.name == self.name:
                continue
            elif self.is_connected_to(connection_config.name):
                continue
            else:
                self.logger.info(
                    "EventBus Endpoint %s connecting to other Endpoint %s",
                    self.name,
                    connection_config.name
                )
                self.add_listener_endpoints_nowait(connection_config)

    def auto_add_announced_listener_endpoints(self) -> None:
        """
        Connect this endpoint to all new endpoints that are announced
        """
        self.subscribe(AvailableEndpointsUpdated, self.add_other_listener_endpoints)

    def announce_endpoint(self, filter_predicate: Callable[[BaseEvent], bool]=filter_none) -> None:
        """
        Announce this endpoint to the :class:`~trinity.endpoint.TrinityMainEventBusEndpoint` so
        that it will be further propagated to all other endpoints, allowing them to connect to us.
        """
        self.broadcast(
            EventBusConnected(ListenerConfig(
                name=self.name,
                path=self.ipc_path,
                filter_predicate=filter_predicate
            )),
            BroadcastConfig(filter_endpoint=MAIN_EVENTBUS_ENDPOINT)
        )


class TrinityMainEventBusEndpoint(TrinityEventBusEndpoint):
    """
    Endpoint that operates like a bootnode in the sense that every other endpoint is aware of this
    endpoint, connects to it by default and uses it to advertise itself to other endpoints.
    """

    available_endpoints: Tuple[ListenerConfig, ...]

    def track_and_propagate_available_endpoints(self) -> None:
        """
        Track new announced endpoints and propagate them across all other existing endpoints.
        """
        self.available_endpoints = tuple()

        def handle_new_endpoints(ev: EventBusConnected) -> None:
            # In a perfect world, we should only reach this code once for every endpoint.
            # However, we check `is_connected_to` here as a safe guard because theoretically
            # it could happen that a (buggy, malicious) plugin raises the `EventBusConnected`
            # event multiple times which would then raise an exception if we are already connected
            # to that endpoint.
            if not self.is_connected_to(ev.listener_config.name):
                self.logger.error(
                    "EventBus of main process connecting to EventBus %s", ev.listener_config.name
                )
                self.add_listener_endpoints_blocking(ev.listener_config)

            self.available_endpoints = self.available_endpoints + (ev.listener_config,)
            self.logger.debug("New EventBus Endpoint connected %s", ev.listener_config.name)
            # Broadcast available endpoints to all connected endpoints, giving them
            # a chance to cross connect
            self.broadcast(AvailableEndpointsUpdated(self.available_endpoints))
            self.logger.debug("Connected EventBus Endpoints %s", self.available_endpoints)

        self.subscribe(EventBusConnected, handle_new_endpoints)
