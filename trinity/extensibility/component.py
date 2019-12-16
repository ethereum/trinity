from abc import (
    ABC,
    abstractmethod
)
from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
import asyncio
import logging
from typing import AsyncIterator

from async_generator import asynccontextmanager

from trinity._utils.os import friendly_filename_or_url
from trinity.boot_info import BootInfo


logger = logging.getLogger('trinity.extensibility.component')


class BaseComponentAPI(ABC):
    @classmethod
    @abstractmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        """
        Give the component a chance to amend the Trinity CLI argument parser.
        """
        ...

    @classmethod
    @abstractmethod
    def validate_cli(cls, boot_info: BootInfo) -> None:
        """
        Give the component a chance to do runtime validation of the command line arguments.
        """
        ...


class Application(BaseComponentAPI):
    @classmethod
    def validate_cli(cls, boot_info: BootInfo) -> None:
        pass


class ComponentAPI(BaseComponentAPI):
    name: str

    @abstractmethod
    def __init__(self, boot_info: BootInfo) -> None:
        ...

    @property
    @abstractmethod
    def is_enabled(self) -> bool:
        ...

    @abstractmethod
    async def run(self) -> None:
        ...


class BaseComponent(ComponentAPI):
    def __init__(self, boot_info: BootInfo) -> None:
        if not hasattr(self, 'name'):
            raise AttributeError(f"No name attribute defined for {self.__class__}")
        self._boot_info = boot_info

    def __str__(self) -> str:
        return f"<Component[{self.name}]>"

    def __repr__(self) -> str:
        return f"{type(self).__name__}(boot_info={self._boot_info})"

    @classmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        pass

    @classmethod
    def validate_cli(cls, boot_info: BootInfo) -> None:
        pass


class BaseIsolatedComponent(BaseComponent):
    """
    A :class:`~trinity.extensibility.component.BaseIsolatedComponent` runs in an isolated process
    and hence provides security and flexibility by not making assumptions about its internal
    operations.

    Such components are free to use non-blocking asyncio as well as synchronous calls. When an
    isolated component is stopped it does first receive a SIGINT followed by a SIGTERM soon after.
    It is up to the component to handle these signals accordingly.
    """
    endpoint_name: str = None

    @classmethod
    def _get_endpoint_name(cls) -> str:
        if cls.endpoint_name is None:
            return friendly_filename_or_url(cls.name)
        else:
            return cls.endpoint_name


@asynccontextmanager
async def run_component(component: ComponentAPI) -> AsyncIterator[None]:
    task = asyncio.ensure_future(component.run())
    logger.debug("Starting component: %s", component.name)
    try:
        yield
    finally:
        logger.debug("Stopping component: %s", component.name)
        if not task.done():
            logger.debug("Cancelling component: %s", component.name)
            task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        logger.debug("Stopped component: %s", component.name)
