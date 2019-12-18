from typing import Type, TypeVar

from eth_utils import humanize_hash
from ssz.hashable_container import HashableContainer
from ssz.sedes import bytes4, uint64

from eth2.beacon.typing import Epoch, Version

from .defaults import default_epoch, default_version

TFork = TypeVar("TFork", bound="Fork")


class Fork(HashableContainer):

    fields = [
        ("previous_version", bytes4),
        ("current_version", bytes4),
        # Epoch of latest fork
        ("epoch", uint64),
    ]

    @classmethod
    def create(
        cls: Type[TFork],
        previous_version: Version = default_version,
        current_version: Version = default_version,
        epoch: Epoch = default_epoch,
    ) -> TFork:
        return super().create(
            previous_version=previous_version,
            current_version=current_version,
            epoch=epoch,
        )

    def __str__(self) -> str:
        return (
            f"previous_version={humanize_hash(self.previous_version)},"
            f" current_version={humanize_hash(self.current_version)},"
            f" epoch={self.epoch}"
        )


default_fork = Fork.create()
