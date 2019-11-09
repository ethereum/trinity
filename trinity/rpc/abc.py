from abc import (
    ABC,
    abstractmethod
)
from typing import (
    Any,
    Dict,
    List,
)

from trinity.rpc.typing import Response


class BaseRPCServer(ABC):
    @abstractmethod
    async def execute_post(self, request: Dict[str, Any]) -> str:
        ...

    @abstractmethod
    async def execute_get(self, request: Dict[str, Any]) -> Response:
        ...

    @property
    @abstractmethod
    def supported_methods(self) -> List[str]:
        ...
