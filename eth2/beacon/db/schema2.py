import ssz

from eth2.beacon.typing import Root, Slot


def finalized_head_root() -> bytes:
    return b"v1:beacon:finalized-head-root"


def slot_to_block_root(slot: Slot) -> bytes:
    return b"v1:beacon:slot-to-block-root:" + ssz.encode(slot, ssz.uint64)


def block_root_to_block(root: Root) -> bytes:
    return b"v1:beacon:block-root-to-block:" + root


def block_root_to_signature(root: Root) -> bytes:
    return b"v1:beacon:block-root-to-signature:" + root


def slot_to_state_root(slot: Slot) -> bytes:
    return b"v1:beacon:slot-to-state-root:" + ssz.encode(slot, ssz.uint64)


def state_root_to_state(root: Root) -> bytes:
    return b"v1:beacon:state-root-to-state:" + root


def weak_subjectivity_state_root() -> bytes:
    return b"v1:beacon:weak-subjectivity-state-root"
