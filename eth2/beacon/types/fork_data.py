from typing import Type, TypeVar

from eth_utils import humanize_hash
from ssz.hashable_container import HashableContainer
from ssz.sedes import bytes4, bytes32

from eth2.beacon.typing import Root, Version

from .defaults import default_root, default_version

TForkData = TypeVar("TForkData", bound="ForkData")


class ForkData(HashableContainer):

    fields = [("current_version", bytes4), ("genesis_validators_root", bytes32)]

    @classmethod
    def create(
        cls: Type[TForkData],
        current_version: Version = default_version,
        genesis_validators_root: Root = default_root,
    ) -> TForkData:
        return super().create(
            current_version=current_version,
            genesis_validators_root=genesis_validators_root,
        )

    def __str__(self) -> str:
        return (
            f"current_version={self.current_version.hex()},"
            f" genesis_validators_root={humanize_hash(self.genesis_validtors_root)}"
        )


default_fork_data = ForkData.create()
