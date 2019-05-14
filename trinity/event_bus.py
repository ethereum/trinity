import asyncio
from typing import (
    Any,
    Callable,
    Type,
    Tuple,
)

import trio

from lahja import AsyncioEndpoint, ConnectionConfig, BroadcastConfig, TrioEndpoint

from p2p.trio_service import (
    Service as TrioService,
    ManagerAPI as TrioManagerAPI,
)
from p2p.service import BaseService

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
    BasePlugin,
    MainAndIsolatedProcessScope,
    PluginManager,
    TrinityBootInfo,
)


class PluginManagerService(BaseService):
    def __init__(self,
                 trinity_boot_info: TrinityBootInfo,
                 plugins: Tuple[Type[BasePlugin], ...],
                 kill_trinity_fn: Callable[[str], Any]) -> None:
        self._boot_info = trinity_boot_info
        self._plugins = plugins
        self._kill_trinity_fn

    async def _run(self) -> None:
        self._connection_config = ConnectionConfig.from_name(
            MAIN_EVENTBUS_ENDPOINT,
            self._boot_info._trinity_config.ipc_dir
        )
        async with AsyncioEndpoint.serve(self._connection_config) as endpoint:
            self._endpoint = endpoint

            # connect to ourselves
            await endpoint.connect_to_endpoint(self._connection_config)

            # start the background process that tracks and propagates available
            # endpoints to the other connected endpoints
            self.run_daemon_task(self._track_and_propagate_available_endpoints())

            # start the plugin manager
            plugin_manager = PluginManager(
                MainAndIsolatedProcessScope(endpoint),
                self._plugins
            )
            plugin_manager.prepare(self._boot_info)
            await self.cancellation()

    async def _handle_shutdown_request(self) -> None:
        req = await self._endpoint.wait_for(ShutdownRequest)
        self._kill_trinity_fn(req.reason)
        self.cancel_nowait()

    _available_endpoints: Tuple[ConnectionConfig, ...] = ()

    async def _track_and_propagate_available_endpoints(self) -> None:
        """
        Track new announced endpoints and propagate them across all other existing endpoints.
        """
        async for ev in self.wait_iter(self.endpoint.stream(EventBusConnected)):
            if not self._endpoint.is_connected_to(ev.connection_config.name):
                self.logger.info(
                    "EventBus of main process connecting to EventBus %s", ev.connection_config.name
                )
                await self._endpoint.connect_to_endpoint(ev.connection_config)

            self._available_endpoints = self._available_endpoints + (ev.connection_config,)
            self.logger.debug("New EventBus Endpoint connected %s", ev.connection_config.name)
            # Broadcast available endpoints to all connected endpoints, giving them
            # a chance to cross connect
            await self.broadcast(AvailableEndpointsUpdated(self._available_endpoints))
            self.logger.debug("Connected EventBus Endpoints %s", self._available_endpoints)


class AsyncioEventBusService(BaseService):
    endpoint: AsyncioEndpoint

    def __init__(self, trinity_config: TrinityConfig, endpoint_name: str) -> None:
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

    async def _run(self) -> None:
        async with AsyncioEndpoint.serve(self._connection_config) as endpoint:
            self._endpoint = endpoint
            # signal that the endpoint is now available
            self._endpoint_available.set()

            # run background task that automatically connects to newly announced endpoints
            self.run_daemon_task(self._auto_connect_new_announced_endpoints())

            # connect to the *main* endpoint which communicates enformation
            # about other endpoints that come online.
            await endpoint.connect_to_endpoints(
                ConnectionConfig.from_name(MAIN_EVENTBUS_ENDPOINT, self._trinity_config.ipc_dir),
                # Plugins that run within the networking process broadcast and receive on the
                # the same endpoint
                self._connection_config,
            )

            # announce ourself to the event bus
            await self._endpoint.broadcast(
                EventBusConnected(self._connection_config),
                BroadcastConfig(filter_endpoint=MAIN_EVENTBUS_ENDPOINT)
            )

            # run until the endpoint exits
            await endpoint.wait_stopped()

    async def _auto_connect_new_announced_endpoints(self) -> None:
        """
        Connect this endpoint to all new endpoints that are announced
        """
        async for ev in self.wait_iter(self._endpoint.stream(AvailableEndpointsUpdated)):
            for connection_config in ev.available_endpoints:
                if connection_config.name == self._endpoint.name:
                    continue
                elif self._endpoint.is_connected_to(connection_config.name):
                    continue
                else:
                    self._endpoint.logger.info(
                        "EventBus Endpoint %s connecting to other Endpoint %s",
                        self._endpoint.name,
                        connection_config.name
                    )
                    await self._endpoint.connect_to_endpoints(connection_config)


class TrioEventBusService(TrioService):
    def __init__(self, trinity_config: TrinityConfig, endpoint_name: str) -> None:
        self._trinity_config = trinity_config
        self._endpoint_available = trio.Event()
        self._connection_config = ConnectionConfig.from_name(
            endpoint_name,
            self._trinity_config.ipc_dir
        )

    async def wait_event_bus_available(self) -> None:
        await self._endpoint_available.wait()

    def get_event_bus(self) -> TrioEndpoint:
        return self._endpoint

    async def run(self, manager: TrioManagerAPI) -> None:
        async with TrioEndpoint.serve(self._connection_config) as endpoint:
            self._endpoint = endpoint
            manager.run_daemon_task(self._auto_connect_new_announced_endpoints)
            await manager.wait_cancelled()

    async def _auto_connect_new_announced_endpoints(self) -> None:
        """
        Connect this endpoint to all new endpoints that are announced
        """
        async for ev in self._endpoint.stream(AvailableEndpointsUpdated):
            for connection_config in ev.available_endpoints:
                if connection_config.name == self._endpoint.name:
                    continue
                elif self._endpoint.is_connected_to(connection_config.name):
                    continue
                else:
                    self._endpoint.logger.info(
                        "EventBus Endpoint %s connecting to other Endpoint %s",
                        self._endpoint.name,
                        connection_config.name
                    )
                    await self._endpoint.connect_to_endpoints(connection_config)
