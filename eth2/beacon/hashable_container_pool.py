from typing import Callable, Dict, Generic, Iterable, Iterator, Tuple, TypeVar, Union

from ssz.hashable_container import HashableContainer

from eth2.beacon.typing import Root

T = TypeVar("T", bound=HashableContainer)


class HashableContainerPool(Generic[T]):
    _pool_storage: Dict[Root, T]

    def __init__(self) -> None:
        self._pool_storage = {}

    def __len__(self) -> int:
        return len(self._pool_storage)

    def __iter__(self) -> Iterator[Tuple[Root, T]]:
        return iter(self._pool_storage.items())

    def __contains__(self, container_or_root: Union[T, Root]) -> bool:
        if isinstance(container_or_root, bytes):
            root = container_or_root
        else:
            root = self._get_root(container_or_root)

        return root in self._pool_storage

    def _get_root(self, container: T) -> Root:
        return container.hash_tree_root

    def get(self, hash_tree_root: Root) -> T:
        return self._pool_storage[hash_tree_root]

    def get_all(self) -> Tuple[T, ...]:
        return tuple(self._pool_storage.values())

    def _batch_do(self, f: Callable[[T], None], containers: Iterable[T]) -> None:
        for container in containers:
            f(container)

    def add(self, container: T) -> None:
        self._pool_storage[self._get_root(container)] = container

    def batch_add(self, containers: Iterable[T]) -> None:
        self._batch_do(self.add, containers)

    def remove(self, container: T) -> None:
        root = self._get_root(container)
        if root in self._pool_storage:
            del self._pool_storage[root]

    def batch_remove(self, containers: Iterable[T]) -> None:
        self._batch_do(self.remove, containers)
