import asyncio
import logging
from typing import Type, Union

import trio

from lahja import (
    AsyncioEndpoint,
    ConnectionConfig,
    BroadcastConfig,
    EndpointAPI,
    TrioEndpoint,
)

from async_service import Service

from trinity.config import TrinityConfig
from trinity.constants import MAIN_EVENTBUS_ENDPOINT
from trinity.events import AvailableEndpointsUpdated, EventBusConnected


class BaseEventBusService(Service):
    logger = logging.getLogger('trinity.extensibility.event_bus.EventBusService')

    _endpoint: EndpointAPI
    EndpointType: Union[Type[TrioEndpoint], Type[AsyncioEndpoint]]

    EventType: Union[Type[trio.Event], Type[asyncio.Event]]
    _endpoint_available: Union[trio.Event, asyncio.Event]

    def __init__(
        self,
        trinity_config: TrinityConfig,
        endpoint_name: str,
    ) -> None:
        self._trinity_config = trinity_config
        self._endpoint_available = self.EventType()
        self._connection_config = ConnectionConfig.from_name(
            endpoint_name, self._trinity_config.ipc_dir
        )

    async def get_event_bus(self) -> EndpointAPI:
        await self._endpoint_available.wait()
        return self._endpoint

    async def run(self) -> None:
        async with self.EndpointType.serve(self._connection_config) as endpoint:
            self._endpoint = endpoint

            # run background task that automatically connects to newly announced endpoints
            self.manager.run_daemon_task(self._auto_connect_new_announced_endpoints, endpoint)

            # connect to the *main* endpoint which communicates information
            # about other endpoints that come online.
            main_endpoint_config = ConnectionConfig.from_name(
                MAIN_EVENTBUS_ENDPOINT, self._trinity_config.ipc_dir
            )
            await endpoint.connect_to_endpoints(main_endpoint_config)

            # announce ourself to the event bus
            await endpoint.wait_until_any_endpoint_subscribed_to(
                EventBusConnected,
            )
            await endpoint.broadcast(
                EventBusConnected(self._connection_config),
                BroadcastConfig(filter_endpoint=main_endpoint_config.name)
            )

            # signal that the endpoint is now available
            self._endpoint_available.set()

            # run until the endpoint exits
            await self.manager.wait_finished()

    async def _auto_connect_new_announced_endpoints(
        self,
        endpoint: EndpointAPI,
    ) -> None:
        """
        Connect the given endpoint to all new endpoints on the given stream
        """
        async for ev in endpoint.stream(AvailableEndpointsUpdated):
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
            self.logger.debug(
                "EventBus Endpoint %s connecting to other Endpoints: %s",
                endpoint.name,
                endpoint_names,
            )
            try:
                await endpoint.connect_to_endpoints(*endpoints_to_connect_to)
            except Exception as e:
                self.logger.warning(
                    "Failed to connect %s to one of %s: %s",
                    endpoint.name,
                    endpoint_names,
                    e,
                )
                raise


class AsyncioEventBusService(BaseEventBusService):
    EventType = asyncio.Event
    EndpointType = AsyncioEndpoint


class TrioEventBusService(BaseEventBusService):
    EventType = trio.Event
    EndpointType = TrioEndpoint
