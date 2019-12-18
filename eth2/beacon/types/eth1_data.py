from typing import Type, TypeVar

from eth.constants import ZERO_HASH32
from eth_typing import Hash32
from eth_utils import humanize_hash
from ssz.hashable_container import HashableContainer
from ssz.sedes import bytes32, uint64

TEth1Data = TypeVar("TEth1Data", bound="Eth1Data")


class Eth1Data(HashableContainer):

    fields = [
        ("deposit_root", bytes32),
        ("deposit_count", uint64),
        ("block_hash", bytes32),
    ]

    @classmethod
    def create(
        cls: Type[TEth1Data],
        deposit_root: Hash32 = ZERO_HASH32,
        deposit_count: int = 0,
        block_hash: Hash32 = ZERO_HASH32,
    ) -> TEth1Data:
        return super().create(
            deposit_root=deposit_root,
            deposit_count=deposit_count,
            block_hash=block_hash,
        )

    def __str__(self) -> str:
        return (
            f"deposit_root={humanize_hash(self.deposit_root)},"
            f" deposit_count={self.deposit_count},"
            f" block_hash={humanize_hash(self.block_hash)}"
        )


default_eth1_data = Eth1Data.create()
