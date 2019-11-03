from abc import (
    ABC,
    abstractmethod
)
from typing import (
    Any,
    Dict,
)


class BaseRPCServer(ABC):
    @abstractmethod
    async def execute(self, request: Dict[str, Any]) -> str:
        ...
