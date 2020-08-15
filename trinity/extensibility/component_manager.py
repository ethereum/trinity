import asyncio
import logging
from typing import (
    Any,
    List,
    Sequence,
    Type,
    Tuple,
)

from async_service import Service

from lahja import AsyncioEndpoint, ConnectionConfig, EndpointAPI

from trinity.boot_info import BootInfo
from trinity.constants import (
    MAIN_EVENTBUS_ENDPOINT,
)
from trinity.events import (
    AvailableEndpointsUpdated,
    EventBusConnected,
)
from trinity.extensibility import (
    BaseIsolatedComponent,
)


class ComponentManager(Service):
    """
    Run the given components until any of them terminates.

    If our process is killed, we cancel and wait for all components to terminate.
    """

    logger = logging.getLogger('trinity.extensibility.component_manager.ComponentManager')

    _endpoint: EndpointAPI
    reason = None

    def __init__(self,
                 boot_info: BootInfo,
                 component_types: Sequence[Type[BaseIsolatedComponent]]) -> None:
        self._boot_info = boot_info
        self._component_types = component_types

        self._endpoint_available = asyncio.Event()
        self._trigger_component_exit = asyncio.Event()

    async def get_event_bus(self) -> EndpointAPI:
        await self._endpoint_available.wait()
        return self._endpoint

    async def run(self) -> None:
        connection_config = ConnectionConfig.from_name(
            MAIN_EVENTBUS_ENDPOINT,
            self._boot_info.trinity_config.ipc_dir
        )

        async with AsyncioEndpoint.serve(connection_config) as endpoint:
            self._endpoint = endpoint

            # start the background process that tracks and propagates available
            # endpoints to the other connected endpoints
            self.manager.run_daemon_task(self._track_and_propagate_available_endpoints)

            await endpoint.wait_until_any_endpoint_subscribed_to(EventBusConnected)

            # signal the endpoint is up and running and available
            self._endpoint_available.set()

            # instantiate all of the components
            all_components = tuple(
                component_cls(self._boot_info)
                for component_cls
                in self._component_types
            )
            # filter out any components that should not be enabled.
            enabled_components = tuple(
                component
                for component in all_components
                if component.is_enabled
            )

            # a little bit of extra try/finally structure here to produce good
            # logging messages about the component lifecycle.
            from p2p.asyncio_utils import create_task, wait_first
            try:
                self.logger.info(
                    "Starting components: %s",
                    '/'.join(component.name for component in enabled_components),
                )
                tasks: List[asyncio.Task[Any]] = []
                for component in enabled_components:
                    tasks.append(
                        create_task(
                            component.run_in_process(),
                            f'IsolatedComponent/{component.name}/run_in_process'
                        )
                    )
                tasks.append(asyncio.create_task(self._trigger_component_exit.wait()))
                self.logger.info("Components started")
                try:
                    # The timeout is long as our component tasks can do a lot of stuff during
                    # their cleanup.
                    await wait_first(tasks, max_wait_after_cancellation=10)
                finally:
                    self.logger.info("Stopping components")
            finally:
                self.logger.info("Components stopped.")
                self.manager.cancel()

    def shutdown(self, reason: str) -> None:
        self.logger.info("Shutting down, reason: %s", reason)
        self.reason = reason
        self._trigger_component_exit.set()

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
