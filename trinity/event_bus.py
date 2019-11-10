from argparse import ArgumentParser
import asyncio
from typing import (
    Collection,
    Sequence,
    Type,
    Tuple,
)

from lahja import AsyncioEndpoint, ConnectionConfig, BroadcastConfig, EndpointAPI

from eth_utils import get_extended_debug_logger

from p2p.service import Service

from trinity._utils.ipc import (
    remove_dangling_ipc_files,
)
from trinity.config import TrinityConfig
from trinity.constants import (
    MAIN_EVENTBUS_ENDPOINT,
)
from trinity.events import (
    ShutdownRequest,
    AvailableEndpointsUpdated,
    EventBusConnected,
)
from trinity.extensibility import (
    ApplicationComponentAPI,
    TrinityBootInfo,
    run_component,
)


class ComponentManager(Service):
    _endpoint: EndpointAPI
    logger = get_extended_debug_logger('trinity.event_bus.ComponentManager')

    def __init__(self,
                 trinity_boot_info: TrinityBootInfo,
                 component_types: Sequence[Type[ApplicationComponentAPI]]) -> None:
        self._boot_info = trinity_boot_info
        self._component_types = component_types
        self._trigger_component_stop = asyncio.Event()

    async def _run_component(self, component: ApplicationComponentAPI) -> None:
        async with run_component(component):
            await self._trigger_component_stop.wait()

    async def run(self) -> None:
        self._connection_config = ConnectionConfig.from_name(
            MAIN_EVENTBUS_ENDPOINT,
            self._boot_info.trinity_config.ipc_dir
        )
        async with AsyncioEndpoint.serve(self._connection_config) as endpoint:
            self._endpoint = endpoint

            # start the background process that tracks and propagates available
            # endpoints to the other connected endpoints
            self.manager.run_daemon_task(self._track_and_propagate_available_endpoints)

            # start the component manager
            components = tuple(
                component_type(self._boot_info, endpoint)
                for component_type
                in self._component_types
            )
            active_components = tuple(
                component
                for component
                in components
                if component.is_enabled
            )

            for component in active_components:
                self.manager.run_task(component.run, name=f"Component[{component.name}]")

            try:
                await self._trigger_component_stop.wait()
            finally:
                self._trigger_component_stop.set()

    async def _handle_shutdown_request(self,
                                       components: Collection[ApplicationComponentAPI],
                                       ) -> None:
        req = await self._endpoint.wait_for(ShutdownRequest)
        self._trigger_component_stop.set()

        self.manager.cancel()

        hint = f"({req.reason})" if req.reason else f""
        self.logger.info('Shutting down Trinity %s', hint)

        remove_dangling_ipc_files(self.logger, self._boot_info.trinity_config.ipc_dir)

        ArgumentParser().exit(message=f"Trinity shutdown complete {hint}\n")

    _available_endpoints: Tuple[ConnectionConfig, ...] = ()

    async def _track_and_propagate_available_endpoints(self) -> None:
        """
        Track new announced endpoints and propagate them across all other existing endpoints.
        """
        async for ev in self._endpoint.stream(EventBusConnected):
            self._available_endpoints = self._available_endpoints + (ev.connection_config,)
            self.logger.debug("New EventBus Endpoint connected %s", ev.connection_config.name)
            # Broadcast available endpoints to all connected endpoints, giving them
            # a chance to cross connect
            await self._endpoint.broadcast(AvailableEndpointsUpdated(self._available_endpoints))
            self.logger.debug("Connected EventBus Endpoints %s", self._available_endpoints)


class AsyncioEventBusService(Service):
    endpoint: AsyncioEndpoint

    def __init__(self,
                 trinity_config: TrinityConfig,
                 endpoint_name: str) -> None:
        self._trinity_config = trinity_config
        self._endpoint_available = asyncio.Event()
        self._connection_config = ConnectionConfig.from_name(
            endpoint_name,
            self._trinity_config.ipc_dir
        )

    async def wait_event_bus_available(self) -> None:
        await self._endpoint_available.wait()

    def get_event_bus(self) -> AsyncioEndpoint:
        return self._endpoint

    async def run(self) -> None:
        async with AsyncioEndpoint.serve(self._connection_config) as endpoint:
            self._endpoint = endpoint
            # signal that the endpoint is now available
            self._endpoint_available.set()

            # run background task that automatically connects to newly announced endpoints
            self.manager.run_daemon_task(self._auto_connect_new_announced_endpoints)

            # connect to the *main* endpoint which communicates information
            # about other endpoints that come online.
            main_endpoint_config = ConnectionConfig.from_name(
                MAIN_EVENTBUS_ENDPOINT,
                self._trinity_config.ipc_dir,
            )
            await endpoint.connect_to_endpoints(main_endpoint_config)

            # announce ourself to the event bus
            await endpoint.wait_until_endpoint_subscribed_to(
                MAIN_EVENTBUS_ENDPOINT,
                EventBusConnected,
            )
            await endpoint.broadcast(
                EventBusConnected(self._connection_config),
                BroadcastConfig(filter_endpoint=MAIN_EVENTBUS_ENDPOINT)
            )

            # run until the endpoint exits
            await self.manager.wait_stopped()

    async def _auto_connect_new_announced_endpoints(self) -> None:
        """
        Connect this endpoint to all new endpoints that are announced
        """
        async for ev in self._endpoint.stream(AvailableEndpointsUpdated):
            # We only connect to Endpoints that appear after our own Endpoint in the set.
            # This ensures that we don't try to connect to an Endpoint while that remote
            # Endpoint also wants to connect to us.
            endpoints_to_connect_to = tuple(
                connection_config
                for index, val in enumerate(ev.available_endpoints)
                if val.name == self._endpoint.name
                for connection_config in ev.available_endpoints[index:]
                if not self._endpoint.is_connected_to(connection_config.name)
            )
            self._endpoint.logger.debug(
                "EventBus Endpoint %s connecting to other Endpoints %s",
                self._endpoint.name,
                ','.join((config.name for config in endpoints_to_connect_to)),
            )
            await self._endpoint.connect_to_endpoints(*endpoints_to_connect_to)
