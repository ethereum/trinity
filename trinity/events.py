from typing import (
    Tuple,
)

from lahja import (
    BaseEvent,
    ListenerConfig,
)


class ShutdownRequest(BaseEvent):

    def __init__(self, reason: str="") -> None:
        self.reason = reason


class EventBusConnected(BaseEvent):
    """
    Broadcasted when a new :class:`~lahja.endpoint.Endpoint` connects to the ``main``
    :class:`~lahja.endpoint.Endpoint`. The :class:`~lahja.endpoint.Endpoint` that connects to the
    the ``main`` :class:`~lahja.endpoint.Endpoint` should send
    :class:`~trinity.events.EventBusConnected` to ``main`` which will then cause ``main`` to send
    a :class:`~trinity.events.AvailableEndpointsUpdated` event to every connected
    :class:`~lahja.endpoint.Endpoint`, making them aware of other endpoints they can connect to.
    """

    def __init__(self, listener_config: ListenerConfig) -> None:
        self.listener_config = listener_config


class AvailableEndpointsUpdated(BaseEvent):
    """
    Broadcasted by the ``main`` :class:`~lahja.endpoint.Endpoint` after it has received a
    :class:`~trinity.events.EventBusConnected` event. The ``available_endpoints`` property
    lists all available endpoints that are known at the time when the event is raised.
    """

    def __init__(self, available_endpoints: Tuple[ListenerConfig, ...]) -> None:
        self.available_endpoints = available_endpoints
