from typing import Any, Type, TYPE_CHECKING

from lahja import BaseEvent


if TYPE_CHECKING:
    from trinity.extensibility import BasePlugin  # noqa: F401


class PluginStartedEvent(BaseEvent):
    """
    Broadcasted when a plugin was started
    """

    def __init__(self, plugin_type: Type["BasePlugin"]) -> None:
        self.plugin_type = plugin_type


class ResourceAvailableEvent(BaseEvent):
    """
    Broadcasted when a resource becomes available
    """

    def __init__(self, resource: Any, resource_type: Type[Any]) -> None:
        self.resource = resource
        self.resource_type = resource_type
