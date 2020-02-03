import asyncio
from collections import defaultdict
from collections.abc import Hashable
from typing import AsyncIterator, DefaultDict, Dict, Generic, TypeVar

from async_generator import asynccontextmanager


TResource = TypeVar('TResource', bound=Hashable)


class ResourceLock(Generic[TResource]):
    """
    Manage a set of locks for some set of hashable resources.
    """
    _locks: Dict[TResource, asyncio.Lock]
    _reference_counts: DefaultDict[TResource, int]

    def __init__(self) -> None:
        self._locks = {}
        self._reference_counts = defaultdict(int)

    @asynccontextmanager
    async def lock(self, resource: TResource) -> AsyncIterator[None]:
        if resource not in self._locks:
            self._locks[resource] = asyncio.Lock()

        try:
            self._reference_counts[resource] += 1
            async with self._locks[resource]:
                yield
        finally:
            self._reference_counts[resource] -= 1
            if self._reference_counts[resource] <= 0:
                del self._reference_counts[resource]
                del self._locks[resource]

    def is_locked(self, resource: TResource) -> bool:
        if resource not in self._locks:
            return False
        else:
            return self._locks[resource].locked()
