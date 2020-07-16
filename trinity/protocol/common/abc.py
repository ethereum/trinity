from abc import ABC, abstractmethod

from eth_typing import BlockNumber, Hash32


class ChainInfoAPI(ABC):
    network_id: int
    genesis_hash: Hash32


class HeadInfoAPI(ABC):

    @property
    @abstractmethod
    def head_td(self) -> int:
        ...

    @property
    @abstractmethod
    def head_hash(self) -> Hash32:
        ...

    @property
    @abstractmethod
    def head_number(self) -> BlockNumber:
        ...
