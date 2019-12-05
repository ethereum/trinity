import asyncio
from typing import Any, AsyncGenerator, Callable, Sequence, Type, Tuple

import trio

from lahja import (
    AsyncioEndpoint,
    ConnectionConfig,
    BroadcastConfig,
    EndpointAPI,
    TrioEndpoint,
)
from lahja.base import BaseEndpoint

from cancel_token import CancelToken

from async_service import Service

from p2p.service import BaseService

from trinity.boot_info import BootInfo
from trinity.config import TrinityConfig
from trinity.constants import MAIN_EVENTBUS_ENDPOINT
from trinity.events import ShutdownRequest, AvailableEndpointsUpdated, EventBusConnected
from trinity.extensibility import BaseComponent, ComponentManager


class ComponentManagerService(BaseService):
    _endpoint: EndpointAPI

    def __init__(
        self,
        boot_info: BootInfo,
        components: Sequence[Type[BaseComponent]],
        kill_trinity_fn: Callable[[str], Any],
        cancel_token: CancelToken = None,
        loop: asyncio.AbstractEventLoop = None,
    ) -> None:
        self._boot_info = boot_info
        self._components = components
        self._kill_trinity_fn = kill_trinity_fn
        super().__init__(cancel_token, loop)

    async def _run(self) -> None:
        self._connection_config = ConnectionConfig.from_name(
            MAIN_EVENTBUS_ENDPOINT, self._boot_info.trinity_config.ipc_dir
        )
        async with AsyncioEndpoint.serve(self._connection_config) as endpoint:
            self._endpoint = endpoint

            # start the background process that tracks and propagates available
            # endpoints to the other connected endpoints
            self.run_daemon_task(self._track_and_propagate_available_endpoints())
            self.run_daemon_task(self._handle_shutdown_request())

            # start the component manager
            self.component_manager = ComponentManager(endpoint, self._components)
            self.component_manager.prepare(self._boot_info)
            await self.cancellation()

    async def _handle_shutdown_request(self) -> None:
        req = await self.wait(self._endpoint.wait_for(ShutdownRequest))
        self._kill_trinity_fn(req.reason)
        self.cancel_nowait()

    async def _cleanup(self) -> None:
        self.component_manager.shutdown_blocking()

    _available_endpoints: Tuple[ConnectionConfig, ...] = ()

    async def _track_and_propagate_available_endpoints(self) -> None:
        """
        Track new announced endpoints and propagate them across all other existing endpoints.
        """
        async for ev in self.wait_iter(self._endpoint.stream(EventBusConnected)):
            self._available_endpoints = self._available_endpoints + (
                ev.connection_config,
            )
            self.logger.debug(
                "New EventBus Endpoint connected %s", ev.connection_config.name
            )
            # Broadcast available endpoints to all connected endpoints, giving them
            # a chance to cross connect
            await self._endpoint.broadcast(
                AvailableEndpointsUpdated(self._available_endpoints)
            )
            self.logger.debug(
                "Connected EventBus Endpoints %s", self._available_endpoints
            )


class AsyncioEventBusService(BaseService):
    endpoint: AsyncioEndpoint

    def __init__(
        self,
        trinity_config: TrinityConfig,
        endpoint_name: str,
        cancel_token: CancelToken = None,
        loop: asyncio.AbstractEventLoop = None,
    ) -> None:
        self._trinity_config = trinity_config
        self._endpoint_available = asyncio.Event()
        self._connection_config = ConnectionConfig.from_name(
            endpoint_name, self._trinity_config.ipc_dir
        )
        super().__init__(cancel_token, loop)

    async def wait_event_bus_available(self) -> None:
        await self._endpoint_available.wait()

    def get_event_bus(self) -> AsyncioEndpoint:
        return self._endpoint

    async def _run(self) -> None:
        async with AsyncioEndpoint.serve(self._connection_config) as endpoint:
            self._endpoint = endpoint
            # signal that the endpoint is now available
            self._endpoint_available.set()

            # run background task that automatically connects to newly announced endpoints
            self.run_daemon_task(
                _auto_connect_new_announced_endpoints(
                    self._endpoint, self._new_available_endpoints()
                )
            )

            # connect to the *main* endpoint which communicates information
            # about other endpoints that come online.
            main_endpoint_config = ConnectionConfig.from_name(
                MAIN_EVENTBUS_ENDPOINT, self._trinity_config.ipc_dir
            )
            await endpoint.connect_to_endpoints(main_endpoint_config)

            # announce ourself to the event bus
            await endpoint.wait_until_endpoint_subscribed_to(
                MAIN_EVENTBUS_ENDPOINT, EventBusConnected
            )
            await endpoint.broadcast(
                EventBusConnected(self._connection_config),
                BroadcastConfig(filter_endpoint=MAIN_EVENTBUS_ENDPOINT),
            )

            # run until the endpoint exits
            await self.cancellation()

    async def _new_available_endpoints(
        self
    ) -> AsyncGenerator[AvailableEndpointsUpdated, None]:
        async for ev in self.wait_iter(
            self._endpoint.stream(AvailableEndpointsUpdated)
        ):
            yield ev


class TrioEventBusService(Service):
    endpoint: TrioEndpoint

    def __init__(self, trinity_config: TrinityConfig, endpoint_name: str) -> None:
        self._trinity_config = trinity_config
        self._endpoint_available = trio.Event()
        self._connection_config = ConnectionConfig.from_name(
            endpoint_name, self._trinity_config.ipc_dir
        )

    async def wait_event_bus_available(self) -> None:
        await self._endpoint_available.wait()

    def get_event_bus(self) -> TrioEndpoint:
        return self._endpoint

    async def run(self) -> None:
        async with TrioEndpoint.serve(self._connection_config) as endpoint:
            self._endpoint = endpoint
            # signal that the endpoint is now available
            self._endpoint_available.set()

            # run background task that automatically connects to newly announced endpoints
            self.manager.run_daemon_task(
                _auto_connect_new_announced_endpoints,
                self._endpoint,
                self._new_available_endpoints(),
            )

            # connect to the *main* endpoint which communicates information
            # about other endpoints that come online.
            main_endpoint_config = ConnectionConfig.from_name(
                MAIN_EVENTBUS_ENDPOINT, self._trinity_config.ipc_dir
            )
            await endpoint.connect_to_endpoints(main_endpoint_config)

            # announce ourself to the event bus
            await endpoint.wait_until_endpoint_subscribed_to(
                MAIN_EVENTBUS_ENDPOINT, EventBusConnected
            )
            await endpoint.broadcast(
                EventBusConnected(self._connection_config),
                BroadcastConfig(filter_endpoint=MAIN_EVENTBUS_ENDPOINT),
            )

            # run until the endpoint exits
            await self.manager.wait_finished()

    async def _new_available_endpoints(
        self
    ) -> AsyncGenerator[AvailableEndpointsUpdated, None]:
        async for ev in self._endpoint.stream(AvailableEndpointsUpdated):
            yield ev


async def _auto_connect_new_announced_endpoints(
    endpoint: BaseEndpoint,
    new_available_endpoints_stream: AsyncGenerator[AvailableEndpointsUpdated, None],
) -> None:
    """
    Connect the given endpoint to all new endpoints on the given stream
    """
    async for ev in new_available_endpoints_stream:
        # We only connect to Endpoints that appear after our own Endpoint in the set.
        # This ensures that we don't try to connect to an Endpoint while that remote
        # Endpoint also wants to connect to us.
        endpoints_to_connect_to = tuple(
            connection_config
            for index, val in enumerate(ev.available_endpoints)
            if val.name == endpoint.name
            for connection_config in ev.available_endpoints[index:]
            if not endpoint.is_connected_to(connection_config.name)
        )
        if not endpoints_to_connect_to:
            continue

        endpoint_names = ",".join((config.name for config in endpoints_to_connect_to))
        endpoint.logger.debug(
            "EventBus Endpoint %s connecting to other Endpoints: %s",
            endpoint.name,
            endpoint_names,
        )
        try:
            await endpoint.connect_to_endpoints(*endpoints_to_connect_to)
        except Exception as e:
            endpoint.logger.warning(
                "Failed to connect %s to one of %s: %s",
                endpoint.name,
                endpoint_names,
                e,
            )
            raise
