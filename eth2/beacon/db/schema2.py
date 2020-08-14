import ssz

from eth2.beacon.typing import Root, Slot


def finalized_head_root() -> bytes:
    return b"v1:beacon:finalized-head-root"


def canonical_head_root() -> bytes:
    return b"v1:beacon:canonical-head-root"


def slot_to_block_root(slot: Slot) -> bytes:
    return b"v1:beacon:slot-to-block-root:" + ssz.encode(slot, ssz.uint64)


def block_root_to_block(root: Root) -> bytes:
    return b"v1:beacon:block-root-to-block:" + root


def block_root_to_block_header(root: Root) -> bytes:
    return b"v1:beacon:block-root-to-block-header:" + root


def block_root_to_signature(root: Root) -> bytes:
    return b"v1:beacon:block-root-to-signature:" + root


def slot_to_state_root(slot: Slot) -> bytes:
    return b"v1:beacon:slot-to-state-root:" + ssz.encode(slot, ssz.uint64)


def state_root_to_slot(root: Root) -> bytes:
    return b"v1:beacon:state-root-to-slot:" + root


def state_root_to_fork_root(root: Root) -> bytes:
    return b"v1:beacon:state-root-to-fork:" + root


def state_root_to_latest_block_header_root(root: Root) -> bytes:
    return b"v1:beacon:state-root-to-latest-block-header-root:" + root


def state_root_to_parent_state_root(root: Root) -> bytes:
    return b"v1:beacon:state-root-to-parent-state-root:" + root


def state_root_to_historical_roots_root(root: Root) -> bytes:
    return b"v1:beacon:state-root-to-historical-roots-root:" + root


def state_root_to_eth1_data_root(root: Root) -> bytes:
    return b"v1:beacon:state-root-to-eth1-data:" + root


def state_root_to_eth1_data_votes(root: Root) -> bytes:
    return b"v1:beacon:state-root-to-eth1-data-votes:" + root


def state_root_to_eth1_deposit_index(root: Root) -> bytes:
    return b"v1:beacon:state-root-to-eth1-deposit-index:" + root


def validators_root_to_roots_of_validators(root: Root) -> bytes:
    return b"v1:beacon:validators-root-to-roots-of-validators:" + root


def state_root_to_validators_root(root: Root) -> bytes:
    return b"v1:beacon:state-root-to-validators-root:" + root


def balances_root_to_balances(root: Root) -> bytes:
    return b"v1:beacon:balances-root-to-balances:" + root


def state_root_to_balances_root(root: Root) -> bytes:
    return b"v1:beacon:state-root-to-balances-root:" + root


def state_root_to_randao_mix(root: Root) -> bytes:
    return b"v1:beacon:state-root-to-randao-mix:" + root


def slot_to_randao_mix(slot: Slot) -> bytes:
    return b"v1:beacon:slot-to-randao-mix:" + ssz.encode(slot, ssz.uint64)


def state_root_to_slashings_root(root: Root) -> bytes:
    return b"v1:beacon:state-root-to-slashings-root:" + root


def state_root_to_previous_epoch_attestations(root: Root) -> bytes:
    return b"v1:beacon:state-root-to-previous-epoch-attestations:" + root


def state_root_to_current_epoch_attestations(root: Root) -> bytes:
    return b"v1:beacon:state-root-to-current-epoch-attestations:" + root


def state_root_to_justification_bitfield(root: Root) -> bytes:
    return b"v1:beacon:state-root-to-justification-bits:" + root


def state_root_to_previous_justified_checkpoint_root(root: Root) -> bytes:
    return b"v1:beacon:state-root-to-previous-justified-checkpoint:" + root


def state_root_to_current_justified_checkpoint_root(root: Root) -> bytes:
    return b"v1:beacon:state-root-to-current-justified-checkpoint:" + root


def state_root_to_finalized_checkpoint_root(root: Root) -> bytes:
    return b"v1:beacon:state-root-to-finalized-checkpoint:" + root


def genesis_data() -> bytes:
    return b"v1:beacon:genesis-data"
