class ServiceException(Exception):
    """
    Base class for Service exceptions
    """
    pass


class LifecycleError(ServiceException):
    """
    Raised when an action would violate the service lifecycle rules.
    """
    pass


class DaemonTaskExit(ServiceException):
    """
    Raised when an action would violate the service lifecycle rules.
    """
    pass


class ServiceCancelled(ServiceException):
    """
    Raised by external service APIs when a service is cancelled.
    """
    pass
