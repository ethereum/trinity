from dataclasses import dataclass
from typing import (
    Dict,
    Generic,
    Iterable,
    List,
    NewType,
    Optional,
    Sequence,
    TypeVar,
    cast,
)

from eth2.beacon.db.abc import BaseBeaconChainDB
from eth2.beacon.epoch_processing_helpers import get_active_validator_indices
from eth2.beacon.fork_choice.abc import BaseForkChoice, BlockSink
from eth2.beacon.genesis import get_genesis_block
from eth2.beacon.helpers import compute_epoch_at_slot
from eth2.beacon.types.blocks import BaseBeaconBlock, BeaconBlock
from eth2.beacon.types.checkpoints import Checkpoint
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Epoch, Gwei, Root, Slot, ValidatorIndex, default_root
from eth2.configs import Eth2Config

# NOTE: copying `proto_array` implementation from:
# https://github.com/protolambda/eth2-py-hacks/proto_array.py

# Note: The Python implementation of Proto-array is an adaption of the Rust
# implementation by Sigma Prime (Apache 2.0). The Rust implementation is in
# turn an adaption of the original Proto-array work of @protolambda (licensed under MIT).
# However, as part of the Eth2 specification effort, and wider discussions
# with Eth2 implementers, the general idea of this implementation can be regarded as
# licensed under CC0 1.0 Universal, like the Eth2 specification.

ProtoNodeIndex = NewType("ProtoNodeIndex", int)

T = TypeVar("T")


class BlockNode(Generic[T]):
    slot: Slot
    root: Root
    data: T

    def __init__(self, slot: Slot, root: Root, data: T):
        self.slot = slot
        self.root = root
        self.data = data


class ProtoNode(Generic[T]):
    block: BlockNode[T]
    parent: Optional[ProtoNodeIndex]
    justified_epoch: Epoch
    finalized_epoch: Epoch
    weight: int
    best_child: Optional[ProtoNodeIndex]
    best_descendant: Optional[ProtoNodeIndex]

    def __init__(
        self,
        block: BlockNode[T],
        parent: Optional[ProtoNodeIndex],
        justified_epoch: Epoch,
        finalized_epoch: Epoch,
    ):
        self.block = block
        self.parent = parent
        self.justified_epoch = justified_epoch
        self.finalized_epoch = finalized_epoch
        self.weight = 0
        self.best_child = None
        self.best_descendant = None


class ProtoArray(Generic[T]):
    _block_sink: BlockSink
    _index_offset: ProtoNodeIndex
    _finalized_root: Root
    _justified_epoch: Epoch
    _finalized_epoch: Epoch
    nodes: List[ProtoNode[T]]
    indices: Dict[Root, ProtoNodeIndex]

    def __init__(
        self,
        justified_epoch: Epoch,
        finalized_block: BlockNode[T],
        block_sink: BlockSink,
        config: Eth2Config,
    ):
        self._block_sink = block_sink
        self._index_offset = ProtoNodeIndex(0)
        self._justified_epoch = justified_epoch
        finalized_epoch = compute_epoch_at_slot(
            finalized_block.slot, config.SLOTS_PER_EPOCH
        )
        self._finalized_epoch = finalized_epoch
        finalized_node = ProtoNode[T](
            block=finalized_block,
            parent=None,
            justified_epoch=justified_epoch,
            finalized_epoch=finalized_epoch,
        )
        self.nodes = [finalized_node]
        self.indices = {finalized_block.root: ProtoNodeIndex(0)}

    def _get_node(self, index: ProtoNodeIndex) -> ProtoNode[T]:
        if index < self._index_offset:
            raise IndexError(f"Minimum proto-array index is {self._index_offset}")
        i = index - self._index_offset
        if i > len(self.nodes):
            raise IndexError(
                f"Maximum proto-array index is {self._index_offset + len(self.nodes)}"
            )
        return self.nodes[i]

    def canonical_chain(self, anchor_root: Root) -> Iterable[BlockNode[T]]:
        """From head back to anchor root (including the anchor itself)"""
        index: Optional[ProtoNodeIndex] = self.indices[
            self.find_head(anchor_root).root
        ]  # KeyError if unknown root
        while index is not None and index >= self._index_offset:
            node = self._get_node(index)
            yield node.block
            if node.block.root == anchor_root:
                break
            index = node.parent

    def apply_score_changes(
        self, deltas: Iterable[int], justified_epoch: Epoch, finalized_epoch: Epoch
    ) -> None:
        """
        Iterate backwards through the array, touching all nodes and their parents and potentially
        the best-child of each parent.

        The structure of the `self.nodes` array ensures that the child of each node is always
        touched before its parent.

        For each node, the following is done:

        - Update the node's weight with the corresponding delta (can be negative).
        - Back-propagate each node's delta to its parents delta.
        - Compare the current node with the parents best-child, updating it if the current node
        should become the best child.
        - If required, update the parents best-descendant
          with the current node or its best-descendant.
        """
        deltas = list(deltas)  # Copy, during back-prop the contents are mutated.
        assert len(deltas) == len(self.nodes) == len(self.indices)

        if (
            justified_epoch != self._justified_epoch
            or finalized_epoch != self._finalized_epoch
        ):
            self._justified_epoch = justified_epoch
            self._finalized_epoch = finalized_epoch

        # Iterate backwards through all indices in `self.nodes`.
        min_bound = self._index_offset - 1
        max_bound = min_bound + len(self.nodes)
        for node_index, node in zip(
            range(max_bound, min_bound, -1), reversed(self.nodes)
        ):
            node_delta = deltas[node_index - self._index_offset]

            # Apply the delta to the node.
            node.weight = node.weight + node_delta

            # If the node has a parent, try to update its best-child and best-descendant.
            if node.parent is not None and node.parent >= self._index_offset:
                # Back-propagate the nodes delta to its parent.
                parent_index = node.parent - self._index_offset
                deltas[parent_index] += node_delta

                self._maybe_update_best_child_and_descendant(
                    node.parent, ProtoNodeIndex(node_index)
                )

    def on_block(
        self,
        block: BlockNode[T],
        parent_root: Root,
        justified_epoch: Epoch,
        finalized_epoch: Epoch,
    ) -> None:
        """
        Register a block with the fork choice.

        It is only sane to supply a `None` parent for the genesis block.
        """
        # If the block is already known, simply ignore it.
        if block.root in self.indices:
            return

        node_index = ProtoNodeIndex(self._index_offset + len(self.nodes))

        # NOTE: if the parent root is missing, then we take the convention that the parent
        # is the genesis block. This convention handles the alias of the "empty" root as
        # a block root for the genesis block.
        if parent_root in self.indices:
            parent_index = self.indices[parent_root]
        else:
            parent_index = self.indices.get(default_root, None)

        node = ProtoNode[T](block, parent_index, justified_epoch, finalized_epoch)

        self.indices[block.root] = node_index
        self.nodes.append(node)

        if node.parent is not None:
            self._maybe_update_best_child_and_descendant(node.parent, node_index)

    def find_head(self, anchor_root: Root) -> BlockNode[T]:
        """
        Finds the head, starting from the anchor_root
        subtree. (justified_root for regular fork-choice)

        Follows the best-descendant links to find the best-block (i.e., head-block).

        The result of this function is not guaranteed to be accurate if `on_block` has
        been called without a subsequent `apply_score_changes` call. This is because
        `on_block` does not attempt to walk backwards through the tree and update the
        best-child/best-descendant links.
        """
        anchor_index = self.indices.get(anchor_root)  # Key error if not there

        anchor_node = self._get_node(anchor_index)
        best_descendant_index = anchor_node.best_descendant
        if best_descendant_index is None:
            best_descendant_index = anchor_index

        best_node = self._get_node(best_descendant_index)

        # Perform a sanity check that the node is indeed valid to be the head.
        assert self._node_is_viable_for_head(best_node)

        return best_node.block

    def on_prune(self, anchor_root: Root) -> None:
        """
        Update the tree with new finalization information (or alternatively another trusted root)
        """
        anchor_index = self.indices[anchor_root]  # KeyError if unknown root
        if anchor_index == self._index_offset:
            return  # nothing to do

        assert anchor_index > self._index_offset

        best_index = self.indices[self.find_head(anchor_root).root]

        # Remove the `self.indices` key/values for all the to-be-deleted nodes.
        # And send the nodes to the block sink.
        for idx, node in zip(range(self._index_offset, anchor_index), self.nodes):
            canonical = node.best_descendant == best_index
            self._block_sink.on_pruned_block(
                _block_node_to_block(node.block), canonical
            )
            root = self.nodes[idx - self._index_offset].block.root
            del self.indices[root]

        # Drop all the nodes prior to finalization.
        prune_index = anchor_index - self._index_offset
        self.nodes = list(self.nodes[prune_index:])
        # update offset
        self._index_offset = anchor_index

    def _maybe_update_best_child_and_descendant(
        self, parent_index: ProtoNodeIndex, child_index: ProtoNodeIndex
    ) -> None:
        """
        Observe the parent at `parent_index` with respect to the child at `child_index` and
        potentially modify the `parent.best_child` and `parent.best_descendant` values.

        There are four outcomes:

        - The child is already the best child but it's now invalid due
          to a FFG change and should be removed.
        - The child is already the best child and the parent is updated
          with the new best-descendant.
        - The child is not the best child but becomes the best child.
        - The child is not the best child and does not become the best child.
        """
        child = self._get_node(child_index)
        parent = self._get_node(parent_index)

        child_leads_to_viable_head = self._node_leads_to_viable_head(child)

        # The three options that we may set the `parent.best_child` and `parent.best_descendant` to.

        def change_to_none() -> None:
            parent.best_child = None
            parent.best_descendant = None

        def change_to_child() -> None:
            parent.best_child = child_index
            if child.best_descendant is None:
                parent.best_descendant = child_index
            else:
                parent.best_descendant = child.best_descendant

        def no_change() -> None:
            pass

        if parent.best_child is not None:
            if parent.best_child == child_index:
                if not child_leads_to_viable_head:
                    # If the child is already the best-child of the parent
                    # but it's not viable for the head, remove it.
                    change_to_none()
                else:
                    # If the child is the best-child already, set it again to ensure that the
                    # best-descendant of the parent is updated.
                    change_to_child()
            else:
                best_child = self._get_node(parent.best_child)
                best_child_leads_to_viable_head = self._node_leads_to_viable_head(
                    best_child
                )

                if child_leads_to_viable_head and (not best_child_leads_to_viable_head):
                    # The child leads to a viable head, but the current best-child doesn't.
                    change_to_child()
                elif (
                    not child_leads_to_viable_head
                ) and best_child_leads_to_viable_head:
                    # The best child leads to a viable head, but the child doesn't.
                    no_change()
                elif child.weight == best_child.weight:
                    # Tie-breaker of equal weights by root.
                    if child.block.root >= best_child.block.root:
                        change_to_child()
                    else:
                        no_change()
                else:
                    # Choose the winner by weight.
                    if child.weight >= best_child.weight:
                        change_to_child()
                    else:
                        no_change()
        else:
            if child_leads_to_viable_head:
                # There is no current best-child and the child is viable.
                change_to_child()
            else:
                # There is no current best-child but the child is not viable.
                no_change()

    def _node_leads_to_viable_head(self, node: ProtoNode[T]) -> bool:
        """Indicates if the node itself is viable for the head,
           or if it's best descendant is viable for the head."""
        if node.best_descendant is not None:
            best_descendant = self._get_node(node.best_descendant)
            return self._node_is_viable_for_head(best_descendant)
        else:
            return self._node_is_viable_for_head(node)

    def _node_is_viable_for_head(self, node: ProtoNode[T]) -> bool:
        """
        This is the equivalent to the `filter_block_tree` function in the eth2 spec:

        https://github.com/ethereum/eth2.0-specs/blob/v0.10.0/specs/phase0/fork-choice.md#filter_block_tree

        Any node that has a different finalized or
        justified epoch should not be viable for the head.
        """
        return (
            node.justified_epoch == self._justified_epoch or self._justified_epoch == 0
        ) and (
            node.finalized_epoch == self._finalized_epoch or self._finalized_epoch == 0
        )


@dataclass
class VoteTracker:
    current_root: Root
    next_root: Root
    next_epoch: Epoch


class ProtoArrayForkChoice(Generic[T]):
    proto_array: ProtoArray[T]
    votes: List[VoteTracker]
    balances: Sequence[Gwei]

    justified: Checkpoint
    finalized: Checkpoint

    def __init__(
        self,
        finalized_block: BlockNode[T],
        finalized: Checkpoint,
        justified: Checkpoint,
        block_sink: BlockSink,
        config: Eth2Config,
    ):
        finalized_epoch = compute_epoch_at_slot(
            finalized_block.slot, config.SLOTS_PER_EPOCH
        )
        assert finalized_epoch == finalized.epoch
        self.proto_array = ProtoArray(
            justified.epoch, finalized_block, block_sink, config
        )
        self.balances = []
        self.votes = []

    def on_prune(self, anchor_root: Root) -> None:
        self.proto_array.on_prune(anchor_root)

    def get_canonical_chain(self, anchor_root: Root) -> Iterable[BlockNode[T]]:
        self._reconcile_changes()
        for block in self.proto_array.canonical_chain(anchor_root):
            yield block

    def process_attestation(
        self, validator_index: ValidatorIndex, block_root: Root, target_epoch: Epoch
    ) -> None:
        if validator_index >= len(self.votes):
            self.votes.extend(
                [
                    VoteTracker(default_root, default_root, Epoch(0))
                    for _ in range(validator_index - len(self.votes) + 1)
                ]
            )
        vote = self.votes[validator_index]
        if target_epoch > vote.next_epoch:
            vote.next_root = block_root
            vote.next_epoch = target_epoch

    def process_block(
        self,
        block: BlockNode[T],
        parent_root: Root,
        justified_epoch: Epoch,
        finalized_epoch: Epoch,
    ) -> None:
        self.proto_array.on_block(block, parent_root, justified_epoch, finalized_epoch)

    def update_justified(
        self,
        justified: Checkpoint,
        finalized: Checkpoint,
        justified_state_balances: Sequence[Gwei],
    ) -> None:
        old_balances = self.balances
        new_balances = justified_state_balances

        deltas = _compute_deltas(
            self.proto_array.indices,
            self.proto_array._index_offset,
            self.votes,
            old_balances,
            new_balances,
        )

        self.proto_array.apply_score_changes(deltas, justified.epoch, finalized.epoch)

        self.balances = new_balances
        self.justified = justified
        self.finalized = finalized

    def _reconcile_changes(self) -> None:
        """
        NOTE: we call ``apply_score_changes``, see comment under ``ProtoArray.find_head``.
        This should be called before reading the canonical chain.
        """
        old_balances = self.balances
        new_balances = old_balances

        deltas = _compute_deltas(
            self.proto_array.indices,
            self.proto_array._index_offset,
            self.votes,
            old_balances,
            new_balances,
        )

        self.proto_array.apply_score_changes(
            deltas, self.justified.epoch, self.finalized.epoch
        )

    def find_head(self) -> BlockNode[T]:
        self._reconcile_changes()
        # NOTE: can skip some work by starting from justified, rather than finalized head
        return self.proto_array.find_head(self.justified.root)


def _compute_deltas(
    indices: Dict[Root, ProtoNodeIndex],
    index_offset: int,
    votes: List[VoteTracker],
    old_balances: Sequence[Gwei],
    new_balances: Sequence[Gwei],
) -> Sequence[int]:
    """
    Returns a list of `deltas`, where there is one delta for each of the ProtoArray nodes.

    The deltas are calculated between `old_balances` and `new_balances`, and/or a change of vote.
    """
    deltas = [0] * len(indices)

    for val_index, vote in enumerate(votes):
        # There is no need to create a score change
        # if the validator has never voted (may not be active)
        # or both their votes are for the zero hash (alias to the genesis block).
        if vote.current_root == default_root and vote.next_root == default_root:
            continue

        # Validator sets may have different sizes (but attesters are not different,
        # activation only under finality)
        old_balance = old_balances[val_index] if val_index < len(old_balances) else 0
        new_balance = new_balances[val_index] if val_index < len(new_balances) else 0

        if vote.current_root != vote.next_root or old_balance != new_balance:
            # Ignore the current or next vote if it is not known in `indices`.
            # We assume that it is outside of our tree (i.e., pre-finalization)
            # and therefore not interesting.
            if vote.current_root in indices:
                deltas[indices[vote.current_root] - index_offset] -= old_balance

            if vote.next_root in indices:
                deltas[indices[vote.next_root] - index_offset] += new_balance

            vote.current_root = vote.next_root

    return deltas


def _block_node_to_block(node: BlockNode[T]) -> BaseBeaconBlock:
    return cast(BaseBeaconBlock, node.data)


def _block_to_block_node(block: BaseBeaconBlock) -> BlockNode[BaseBeaconBlock]:
    return BlockNode(block.slot, block.hash_tree_root, block)


class LMDGHOSTForkChoice(BaseForkChoice):
    def __init__(
        self,
        finalized_block_node: BlockNode[BaseBeaconBlock],
        finalized_state: BeaconState,
        config: Eth2Config,
        block_sink: BlockSink,
    ) -> None:
        self._config = config
        self._impl = ProtoArrayForkChoice(
            finalized_block_node,
            finalized_state.finalized_checkpoint,
            finalized_state.current_justified_checkpoint,
            block_sink,
            config,
        )
        self.update_justified(finalized_state)

    @classmethod
    def from_genesis(
        cls, genesis_state: BeaconState, config: Eth2Config, block_sink: BlockSink
    ) -> "LMDGHOSTForkChoice":
        # NOTE: patch up genesis state to reflect the genesis block as an initial checkpoint
        # this only has to be patched once at genesis
        genesis_block = get_genesis_block(genesis_state.hash_tree_root, BeaconBlock)
        genesis_block_node = BlockNode(genesis_block.slot, default_root, genesis_block)
        return cls(genesis_block_node, genesis_state, config, block_sink)

    @classmethod
    def from_db(
        cls, chain_db: BaseBeaconChainDB, config: Eth2Config, block_sink: BlockSink
    ) -> "LMDGHOSTForkChoice":
        finalized_head = chain_db.get_finalized_head(BeaconBlock)
        finalized_state = chain_db.get_state_by_root(
            finalized_head.state_root, BeaconState
        )
        finalized_head_node = _block_to_block_node(finalized_head)
        # TODO: need genesis patch up here as well....
        return cls(finalized_head_node, finalized_state, config, block_sink)

    def update_justified(self, state: BeaconState) -> None:
        """
        Call when a new ``state`` is justified.
        """
        self._justified = state.current_justified_checkpoint
        self._finalized = state.finalized_checkpoint

        # NOTE: prune before updating justified as it touches some internal state...
        self._impl.on_prune(self._finalized.root)
        current_epoch = state.current_epoch(self._config.SLOTS_PER_EPOCH)
        balances = tuple(
            state.validators[i].effective_balance
            for i in get_active_validator_indices(state.validators, current_epoch)
        )
        self._impl.update_justified(self._justified, self._finalized, balances)

    def get_canonical_chain(self) -> Iterable[BaseBeaconBlock]:
        for block_node in self._impl.get_canonical_chain(self._finalized.root):
            yield _block_node_to_block(block_node)

    def on_block(self, block: BaseBeaconBlock) -> None:
        """
        NOTE: assumes that only ``block``s are supplied to this method
        if their parent has already been registered.

        Otherwise, the way this module handles the genesis alias may break things.
        Refer to ``ProtoArray.on_block`` for more information.
        """
        self._impl.process_block(
            _block_to_block_node(block),
            block.parent_root,
            self._justified.epoch,
            self._finalized.epoch,
        )

    def on_attestation(
        self, block_root: Root, target_epoch: Epoch, *indices: ValidatorIndex
    ) -> None:
        for validator_index in indices:
            self._impl.process_attestation(validator_index, block_root, target_epoch)

    def find_head(self) -> BaseBeaconBlock:
        node = self._impl.find_head()
        return _block_node_to_block(node)
