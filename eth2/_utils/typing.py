from abc import abstractmethod
from typing import Any, TypeVar

from typing_extensions import Protocol

ComparableType = TypeVar("ComparableType", bound="Comparable")


# Can't use total_ordering here: https://github.com/python/mypy/issues/8539
# @functools.total_ordering
class Comparable(Protocol):
    @abstractmethod
    def __lt__(self: ComparableType, other: ComparableType) -> bool:
        ...

    @abstractmethod
    def __eq__(self, other: Any) -> bool:
        ...


class Serializeable(Protocol):
    @abstractmethod
    def serialize(self) -> bytes:
        ...

    @classmethod
    @abstractmethod
    def deserialize(cls, data: bytes) -> "Serializeable":
        ...
