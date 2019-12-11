from typing import Type, TypeVar

from eth_utils import humanize_hash

from eth2.beacon.constants import ZERO_SIGNING_ROOT
from eth2.beacon.typing import Epoch, SigningRoot
from ssz.hashable_container import HashableContainer
from ssz.sedes import bytes32, uint64

from .defaults import default_epoch

TCheckpoint = TypeVar("TCheckpoint", bound="Checkpoint")


class Checkpoint(HashableContainer):

    fields = [("epoch", uint64), ("root", bytes32)]

    @classmethod
    def create(
        cls: Type[TCheckpoint],
        epoch: Epoch = default_epoch,
        root: SigningRoot = ZERO_SIGNING_ROOT,
    ) -> TCheckpoint:
        return super().create(epoch=epoch, root=root)

    def __str__(self) -> str:
        return f"{self.epoch}, {humanize_hash(self.root)}"


default_checkpoint = Checkpoint.create()
