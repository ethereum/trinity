from typing import Type, TypeVar

from eth_utils import humanize_hash
from ssz.hashable_container import HashableContainer
from ssz.sedes import bytes4, bytes32, uint64

from eth2.beacon.types.checkpoints import Checkpoint
from eth2.beacon.typing import (
    Epoch,
    ForkDigest,
    Root,
    Slot,
    default_epoch,
    default_fork_digest,
    default_root,
    default_slot,
)

TStatus = TypeVar("TStatus", bound="Status")


class Status(HashableContainer):
    fields = [
        ("fork_digest", bytes4),
        ("finalized_root", bytes32),
        ("finalized_epoch", uint64),
        ("head_root", bytes32),
        ("head_slot", uint64),
    ]

    @classmethod
    def create(
        cls: Type[TStatus],
        *,
        fork_digest: ForkDigest = default_fork_digest,
        finalized_root: Root = default_root,
        finalized_epoch: Epoch = default_epoch,
        head_root: Root = default_root,
        head_slot: Slot = default_slot,
    ) -> TStatus:
        return super().create(
            fork_digest=fork_digest,
            finalized_root=finalized_root,
            finalized_epoch=finalized_epoch,
            head_root=head_root,
            head_slot=head_slot,
        )

    @property
    def finalized_checkpoint(self) -> Checkpoint:
        return Checkpoint.create(root=self.finalized_root, epoch=self.finalized_epoch)

    def __str__(self) -> str:
        return (
            f"fork_digest={self.fork_digest.hex()},"
            f" finalized_root={humanize_hash(self.finalized_root)},"
            f" finalized_epoch={self.finalized_epoch},"
            f" head_root={humanize_hash(self.head_root)},"
            f" head_slot={self.head_slot}"
        )
