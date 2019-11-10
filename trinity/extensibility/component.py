from abc import (
    ABC,
    abstractmethod
)
from argparse import (
    ArgumentParser,
    Namespace,
    _SubParsersAction,
)
import asyncio
from typing import (
    Any,
    AsyncIterator,
    Dict,
    NamedTuple,
)

from async_generator import asynccontextmanager

from lahja.base import EndpointAPI

from trinity.config import (
    TrinityConfig
)


class TrinityBootInfo(NamedTuple):
    args: Namespace
    trinity_config: TrinityConfig
    boot_kwargs: Dict[str, Any] = None


class BaseComponentAPI(ABC):
    name: str

    @classmethod
    @abstractmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        """
        Give the component a chance to amend the Trinity CLI argument parser. This hook is called
        before :meth:`~trinity.extensibility.component.BaseComponent.on_ready`
        """
        ...


class BaseCommandComponent(BaseComponentAPI):
    """
    Component base class for implement a new CLI command that is intended to
    take over the main process.
    """
    pass


class ApplicationComponentAPI(BaseComponentAPI):
    """
    Component API for components which augment or add functionality to the
    application.
    """
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


class BaseApplicationComponent(BaseComponentAPI):
    """
    Base class for ApplicationComponentAPI implementations.
    """
    def __init__(self, trinity_boot_info: TrinityBootInfo, endpoint: EndpointAPI) -> None:
        self._boot_info = trinity_boot_info
        self._event_bus = endpoint

    @classmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        pass


@asynccontextmanager
async def run_component(component: ApplicationComponentAPI,
                        loop: asyncio.AbstractEventLoop = None,
                        ) -> AsyncIterator[None]:
    task = asyncio.ensure_future(component.run(), loop=loop)

    try:
        yield
    finally:
        if not task.done():
            task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass
