from abc import abstractmethod
import functools
from typing import Any, TypeVar

from typing_extensions import Protocol

ComparableType = TypeVar("ComparableType", bound="Comparable")


@functools.total_ordering
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
