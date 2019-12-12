from typing import (
    Sequence,
)

from ssz.sedes import (
    List,
    bytes4,
    bytes32,
    uint64,
)
from ssz.hashable_container import HashableContainer

from eth2.beacon.typing import (
    Version,
    default_epoch,
    default_slot,
    default_version,
)
from eth2.beacon.typing import SigningRoot, Slot, Epoch
from eth2.beacon.constants import ZERO_SIGNING_ROOT
from .configs import GoodbyeReasonCode


class Status(HashableContainer):
    fields = [
        ('head_fork_version', bytes4),
        ('finalized_root', bytes32),
        ('finalized_epoch', uint64),
        ('head_root', bytes32),
        ('head_slot', uint64),
    ]

    @classmethod
    def create(
        cls,
        head_fork_version: Version = default_version,
        finalized_root: SigningRoot = ZERO_SIGNING_ROOT,
        finalized_epoch: Epoch = default_epoch,
        head_root: SigningRoot = ZERO_SIGNING_ROOT,
        head_slot: Slot = default_slot,
    ) -> "Status":
        return super().create(
            head_fork_version=head_fork_version,
            finalized_root=finalized_root,
            finalized_epoch=finalized_epoch,
            head_root=head_root,
            head_slot=head_slot,
        )


class Goodbye(HashableContainer):
    fields = [
        ('reason', uint64),
    ]

    @classmethod
    def create(self, reason: int) -> None:
        return super().create(reason=GoodbyeReasonCode(reason))


class BeaconBlocksByRangeRequest(HashableContainer):
    fields = [
        ('head_block_root', bytes32),
        ('start_slot', uint64),
        ('count', uint64),
        ('step', uint64),
    ]

    @classmethod
    def create(
        cls,
        head_block_root: SigningRoot,
        start_slot: Slot,
        count: int,
        step: int,
    ) -> "BeaconBlocksByRangeRequest":
        return super().create(
            head_block_root=head_block_root,
            start_slot=start_slot,
            count=count,
            step=step,
        )


class BeaconBlocksByRootRequest(HashableContainer):
    fields = [
        ('block_roots', List(bytes32, 64)),
    ]

    @classmethod
    def create(cls, block_roots: Sequence[SigningRoot]) -> "BeaconBlocksByRootRequest":
        return super().create(block_roots=block_roots)
