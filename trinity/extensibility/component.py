from abc import (
    ABC,
    abstractmethod
)
from argparse import (
    ArgumentParser,
    Namespace,
    _SubParsersAction,
)
from typing import (
    Any,
    Dict,
    NamedTuple,
)

from lahja.base import EndpointAPI

from trinity.config import (
    TrinityConfig
)


class TrinityBootInfo(NamedTuple):
    args: Namespace
    trinity_config: TrinityConfig
    boot_kwargs: Dict[str, Any] = None


class ComponentAPI(ABC):
    name: str
    endpoint: EndpointAPI

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

    @abstractmethod
    async def stop(self) -> None:
        ...

    @classmethod
    @abstractmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        """
        Give the component a chance to amend the Trinity CLI argument parser. This hook is called
        before :meth:`~trinity.extensibility.component.BaseComponent.on_ready`
        """
        ...


class BaseComponent(ComponentAPI):
    def __init__(self, trinity_boot_info: TrinityBootInfo, endpoint: EndpointAPI) -> None:
        self._boot_info = trinity_boot_info
        self._endpoint = endpoint

    @classmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        pass
