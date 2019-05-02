from trinity.exceptions import BaseTrinityError


class EventBusNotReady(BaseTrinityError):
    """
    Raised when a plugin tried to access the event bus before the plugin
    had received its :meth:`~trinity.extensibility.plugin.BasePlugin.on_ready` call.
    """

    pass


class InvalidPluginStatus(BaseTrinityError):
    """
    Raised when it was attempted to perform an action while the current
    :class:`~trinity.extensibility.plugin.PluginStatus` does not allow to perform such action.
    """

    pass


class UnsuitableShutdownError(BaseTrinityError):
    """
    Raised when :meth:`~trinity.extensibility.plugin_manager.PluginManager.shutdown` was called on
    a :class:`~trinity.extensibility.plugin_manager.PluginManager` instance that operates in the
    :class:`~trinity.extensibility.plugin_manager.MainAndIsolatedProcessScope` or when
    :meth:`~trinity.extensibility.plugin.PluginManager.shutdown_blocking` was called on a
    :class:`~trinity.extensibility.plugin_manager.PluginManager` instance that operates in the
    :class:`~trinity.extensibility.plugin_manager.SharedProcessScope`.
    """

    pass
