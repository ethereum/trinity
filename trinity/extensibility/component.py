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

from trinity._utils.logging import (
    setup_child_process_logging,
)
from trinity.boot_info import BootInfo


logger = logging.getLogger('trinity.extensibility.component')


class BaseComponentAPI(ABC):
    @classmethod
    @abstractmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        """
        Give the component a chance to amend the Trinity CLI argument parser. This hook is called
        before :meth:`~trinity.extensibility.component.BaseComponent.on_ready`
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
        self._boot_info = boot_info

    def __str__(self) -> str:
        return f"<Component[self.name]>"

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
    @classmethod
    def _run_process(cls, boot_info: BootInfo) -> None:
        setup_child_process_logging(boot_info)
        cls.run_process(boot_info)

    @classmethod
    @abstractmethod
    def run_process(cls, boot_info: BootInfo) -> None:
        ...


@asynccontextmanager
async def run_component(component: ComponentAPI) -> AsyncIterator[None]:
    task = asyncio.ensure_future(component.run())
    logger.debug("Starting component: %s", component)
    try:
        yield
    finally:
        logger.debug("Stopping component: %s", component)
        if not task.done():
            logger.debug("Cancelling component task: %s", component)
            task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        logger.debug("Stopped component: %s", component)
