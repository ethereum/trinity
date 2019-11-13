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
from typing import (
    AsyncIterator,
)

from async_generator import asynccontextmanager

from lahja.base import EndpointAPI

from trinity.boot_info import TrinityBootInfo


logger = logging.getLogger('trinity.extensibility.run_component')


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
    def validate_cli(cls, boot_info: TrinityBootInfo) -> None:
        """
        Give the component a chance to perform any validation that should be
        run prior to being run or checking if it should be enabled.
        """
        ...


class BaseCommandComponent(BaseComponentAPI):
    """
    Component base class for implement a new CLI command that is intended to
    take over the main process.
    """
    @classmethod
    def validate_cli(cls, boot_info: TrinityBootInfo) -> None:
        pass


class ApplicationComponentAPI(BaseComponentAPI):
    """
    Component API for components which augment or add functionality to the
    application.
    """
    name: str

    @abstractmethod
    def __init__(self, trinity_boot_info: TrinityBootInfo, endpoint: EndpointAPI) -> None:
        ...

    @property
    @abstractmethod
    def is_enabled(self) -> bool:
        ...

    @abstractmethod
    async def run(self) -> None:
        ...


class BaseApplicationComponent(ApplicationComponentAPI):
    """
    Base class for ApplicationComponentAPI implementations.
    """
    def __init__(self, trinity_boot_info: TrinityBootInfo, endpoint: EndpointAPI) -> None:
        self._boot_info = trinity_boot_info
        self._event_bus = endpoint

    @classmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        pass

    @classmethod
    def validate_cli(cls, boot_info: TrinityBootInfo) -> None:
        pass


@asynccontextmanager
async def run_component(component: ApplicationComponentAPI,
                        loop: asyncio.AbstractEventLoop = None,
                        ) -> AsyncIterator[None]:
    task = asyncio.ensure_future(component.run(), loop=loop)

    logger.debug('Running component: %s', component)
    try:
        yield
    finally:
        logger.debug('Stopping component: %s', component)
        if not task.done():
            task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass
        except KeyboardInterrupt:
            pass
    logger.debug('Finished component shutdown: %s', component)
