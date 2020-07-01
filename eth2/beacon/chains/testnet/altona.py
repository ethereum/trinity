import logging
from typing import Collection, Container, Iterable, Optional, Tuple, Type

from eth.abc import AtomicDatabaseAPI
from eth.exceptions import BlockNotFound
from eth_utils import to_dict, toolz

from eth2._utils.ssz import validate_imported_block_unchanged
from eth2.beacon.chains.abc import BaseBeaconChain
from eth2.beacon.chains.exceptions import ParentNotFoundError, SlashableBlockError
from eth2.beacon.constants import FAR_FUTURE_SLOT, GENESIS_SLOT
from eth2.beacon.db.abc import BaseBeaconChainDB
from eth2.beacon.db.chain2 import BeaconChainDB, StateNotFound
from eth2.beacon.epoch_processing_helpers import get_attesting_indices
from eth2.beacon.fork_choice.abc import BaseForkChoice, BlockSink
from eth2.beacon.state_machines.abc import BaseBeaconStateMachine
from eth2.beacon.state_machines.forks.altona.state_machine import AltonaStateMachine
from eth2.beacon.tools.misc.ssz_vector import override_lengths
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.blocks import (
    BaseBeaconBlock,
    BaseSignedBeaconBlock,
    BeaconBlock,
    SignedBeaconBlock,
)
from eth2.beacon.types.checkpoints import Checkpoint, default_checkpoint
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Root, Slot, ValidatorIndex
from eth2.clock import Tick

StateMachineConfiguration = Tuple[Tuple[Slot, Type[BaseBeaconStateMachine]], ...]


def _sm_configuration_has_increasing_slot(
    sm_configuration: StateMachineConfiguration
) -> bool:
    last_slot = GENESIS_SLOT
    for (slot, _state_machine_class) in sm_configuration:
        if slot < last_slot:
            return False
        else:
            last_slot = slot
    return True


def _validate_sm_configuration(sm_configuration: StateMachineConfiguration) -> None:
    if not sm_configuration:
        raise ValueError(
            "The Chain class cannot be instantiated with an empty `sm_configuration`"
        )
    if not _sm_configuration_has_increasing_slot(sm_configuration):
        raise ValueError(
            "The Chain class requires a state machine configuration"
            " with monotonically increasing slot number"
        )


@to_dict
def _load_state_machines(
    sm_configuration: StateMachineConfiguration
) -> Iterable[Tuple[Container[int], BaseBeaconStateMachine]]:
    sm_configuration += ((FAR_FUTURE_SLOT, None),)
    for (first_fork, second_fork) in toolz.sliding_window(2, sm_configuration):
        valid_range = range(first_fork[0], second_fork[0])
        valid_sm = first_fork[1]()
        yield (valid_range, valid_sm)


class ChainDBBlockSink(BlockSink):
    def __init__(self, chain_db: BaseBeaconChainDB) -> None:
        self._chain_db = chain_db

    def on_pruned_block(self, block: BaseBeaconBlock, canonical: bool) -> None:
        if canonical:
            self._chain_db.mark_canonical_block(block)


class BeaconChain(BaseBeaconChain):
    logger = logging.getLogger("eth2.beacon.chains.BeaconChain")

    _chain_db: BaseBeaconChainDB
    _sm_configuration = ((GENESIS_SLOT, AltonaStateMachine),)
    _fork_choice: BaseForkChoice
    _current_head: BeaconBlock
    _justified_checkpoint: Checkpoint = default_checkpoint
    _finalized_checkpoint: Checkpoint = default_checkpoint

    def __init__(
        self, chain_db: BaseBeaconChainDB, fork_choice: BaseForkChoice
    ) -> None:
        self._chain_db = chain_db

        _validate_sm_configuration(self._sm_configuration)
        self._state_machines_by_range = _load_state_machines(self._sm_configuration)

        self._fork_choice = fork_choice
        self._current_head = fork_choice.find_head()
        head_state = self._chain_db.get_state_by_root(
            self._current_head.state_root, BeaconState
        )

        self._reconcile_justification_and_finality(head_state)

    @classmethod
    def from_genesis(
        cls, base_db: AtomicDatabaseAPI, genesis_state: BeaconState
    ) -> "BeaconChain":
        for starting_slot, state_machine_class in cls._sm_configuration:
            if starting_slot == GENESIS_SLOT:
                signed_block_class = state_machine_class.signed_block_class
                fork_choice_class = state_machine_class.fork_choice_class
                config = state_machine_class.config
                # NOTE: important this happens as soon as it can...
                override_lengths(config)
                break
        else:
            raise Exception("state machine configuration missing genesis era")

        assert genesis_state.slot == GENESIS_SLOT

        chain_db = BeaconChainDB.from_genesis(
            base_db, genesis_state, signed_block_class
        )

        block_sink = ChainDBBlockSink(chain_db)
        fork_choice = fork_choice_class.from_genesis(genesis_state, config, block_sink)
        return cls(chain_db, fork_choice)

    def _get_fork_choice(self, slot: Slot) -> BaseForkChoice:
        # NOTE: ignoring slot polymorphism for now...
        expected_class = self._get_state_machine(slot).fork_choice_class
        if expected_class == self._fork_choice.__class__:
            return self._fork_choice
        else:
            raise NotImplementedError(
                "a fork choice different than the one implemented was requested by slot"
            )

    def _get_state_machine(self, slot: Slot) -> BaseBeaconStateMachine:
        """
        Return the ``StateMachine`` instance for the given slot number.
        """
        # TODO iterate over ``reversed(....items())`` once we support only >=py3.8
        for (slot_range, state_machine) in self._state_machines_by_range.items():
            if slot in slot_range:
                return state_machine
        else:
            raise Exception("state machine configuration was incorrect")

    def get_canonical_head(self) -> BeaconBlock:
        return self._current_head

    def on_tick(self, tick: Tick) -> None:
        if tick.is_first_in_slot():
            fork_choice = self._get_fork_choice(tick.slot)
            head = fork_choice.find_head()
            self._update_head_if_new(head)

    def _get_block_by_slot(
        self, slot: Slot, block_class: Type[SignedBeaconBlock]
    ) -> Optional[SignedBeaconBlock]:
        # check in db first, implying a finalized chain
        block = self._chain_db.get_block_by_slot(slot, block_class.block_class)
        if block:
            signature = self._chain_db.get_block_signature_by_root(block.hash_tree_root)
            return SignedBeaconBlock.create(message=block, signature=signature)
        else:
            # check in the canonical chain according to fork choice
            # NOTE: likely want a more efficient way to determine block by slot...
            for block in self._fork_choice.get_canonical_chain():
                if block.slot == slot:
                    signature = self._chain_db.get_block_signature_by_root(
                        block.hash_tree_root
                    )
                    return SignedBeaconBlock.create(message=block, signature=signature)
            else:
                return None

    def _import_block(
        self, block: BaseSignedBeaconBlock, perform_validation: bool = True
    ) -> BaseSignedBeaconBlock:
        try:
            # NOTE: would need to introduce a "root to slot" look up here for polymorphism
            parent_block = self._chain_db.get_block_by_root(
                block.parent_root, BeaconBlock
            )
        except BlockNotFound:
            raise ParentNotFoundError(
                f"attempt to import block {block} but missing parent block"
            )

        # NOTE: check if block is in the canonical chain
        # First, see if we have a block already at that slot...
        existing_block = self._get_block_by_slot(block.slot, block.__class__)
        if existing_block:
            if existing_block != block:
                # NOTE: we want to keep the block but avoid heavy state transition for now...
                # Rationale: this block may simply be a slashable block. It could also be on
                # a fork. Choose to defer the state materialization until we re-org via fork choice.
                self._chain_db.persist_block(block)

                raise SlashableBlockError(
                    block,
                    f"attempt to import {block} but canonical chain"
                    " already has a block at this slot",
                )
            else:
                # NOTE: block already imported!
                return block
        else:
            head = self.get_canonical_head()
            extension_of_head = block.parent_root == head.hash_tree_root
            if not extension_of_head:
                # NOTE: this arm implies we received a block for a slot _ahead_ of our head
                # on the canonical chain...
                # NOTE: block validity _should_ reject a block before it gets to this layer
                # but we will double-check in the event that invariant is violated or does not hold
                # NOTE: we elect to the block in the event of a
                # re-org later, but do no further processing.
                self._chain_db.persist_block(block)

                raise SlashableBlockError(
                    block,
                    f"attempt to import {block} but canonical chain is not as far ahead",
                )

        state_machine = self._get_state_machine(block.slot)
        state_class = state_machine.state_class
        pre_state = self._chain_db.get_state_by_root(
            parent_block.state_root, state_class
        )

        state, imported_block = state_machine.apply_state_transition(
            pre_state, block, check_proposer_signature=perform_validation
        )

        if perform_validation:
            validate_imported_block_unchanged(imported_block, block)

        # NOTE: if we have a valid block/state, then record in the database.
        self._chain_db.persist_block(block)
        self._chain_db.persist_state(state)

        self._reconcile_justification_and_finality(state)

        return imported_block

    def _reconcile_justification_and_finality(self, state: BeaconState) -> None:
        justified_checkpoint = state.current_justified_checkpoint
        finalized_checkpoint = state.finalized_checkpoint

        if justified_checkpoint.epoch > self._justified_checkpoint.epoch:
            self._justified_checkpoint = justified_checkpoint
            self._fork_choice.update_justified(state)

        if finalized_checkpoint.epoch > self._finalized_checkpoint.epoch:
            self._finalized_checkpoint = finalized_checkpoint
            finalized_head = self._chain_db.get_block_by_root(
                self._finalized_checkpoint.root, BeaconBlock
            )
            self._chain_db.mark_finalized_head(finalized_head)

    def _update_head_if_new(self, block: BeaconBlock) -> None:
        if block != self._current_head:
            self._current_head = block
            self.logger.debug("new head of chain: %s", block)

    def _update_fork_choice_with_block(self, block: BeaconBlock) -> None:
        """
        NOTE: it is assumed that ``_import_block`` has successfully be called
        before this method is run as the fork choice shares state with the underlying
        chain db.

        Adding a new ``block`` likely updates the head so we also call
        ``_update_head_if_new`` after registering the new data with the
        fork choice module.
        """
        fork_choice = self._get_fork_choice(block.slot)
        fork_choice.on_block(block)
        for attestation in block.body.attestations:
            self._update_fork_choice_with_attestation(fork_choice, attestation)
        head = fork_choice.find_head()
        self._update_head_if_new(head)

    def _update_fork_choice_with_attestation(
        self, fork_choice: BaseForkChoice, attestation: Attestation
    ) -> None:
        block_root = attestation.data.beacon_block_root
        target_epoch = attestation.data.target.epoch
        indices = self._get_indices_from_attestation(attestation)
        fork_choice.on_attestation(block_root, target_epoch, *indices)

    def _find_present_ancestor_state(
        self, block_root: Root
    ) -> Tuple[BeaconState, Tuple[SignedBeaconBlock, ...]]:
        """
        Find the first state we have persisted that is an ancestor of ``target_block``.
        """
        try:
            block = self._chain_db.get_block_by_root(block_root, BeaconBlock)
            blocks: Tuple[SignedBeaconBlock, ...] = ()
            # NOTE: re: bounds here; worst case, we return the genesis state.
            for slot in range(block.slot, GENESIS_SLOT - 1, -1):
                try:
                    state_machine = self._get_state_machine(Slot(slot))
                    state = self._chain_db.get_state_by_root(
                        block.state_root, state_machine.state_class
                    )
                    return (state, blocks)
                except StateNotFound:
                    signature = self._chain_db.get_block_signature_by_root(
                        block.hash_tree_root
                    )
                    blocks += (
                        SignedBeaconBlock.create(message=block, signature=signature),
                    )
                    block = self._chain_db.get_block_by_root(
                        block.parent_root, BeaconBlock
                    )
        except BlockNotFound:
            raise Exception(
                "invariant violated: querying a block that has not been persisted"
            )
        # NOTE: `mypy` complains without this although execution should never get here...
        return (None, ())

    def _compute_missing_state(self, target_block: BeaconBlock) -> BeaconState:
        """
        Calculate the state for the ``target_block``.

        The chain persist states for canonical blocks.
        In the even that we need a state that has not been persisted;
        for example, if we are executing a re-org, then we will
        need to compute it.

        NOTE: this method will persist the new (potentially non-canonical) states.
        """
        state, blocks = self._find_present_ancestor_state(target_block.parent_root)

        for block in reversed(blocks):
            state_machine = self._get_state_machine(block.slot)
            state, _ = state_machine.apply_state_transition(state, block)
            self._chain_db.persist_state(state)

        return state

    def _get_indices_from_attestation(
        self, attestation: Attestation
    ) -> Collection[ValidatorIndex]:
        target_block = self._chain_db.get_block_by_root(
            attestation.data.target.root, BeaconBlock
        )
        try:
            target_state = self._chain_db.get_state_by_root(
                target_block.state_root, BeaconState
            )
        except StateNotFound:
            target_state = self._compute_missing_state(target_block)
        sm = self._get_state_machine(target_block.slot)
        return get_attesting_indices(
            target_state, attestation.data, attestation.aggregation_bits, sm.config
        )

    def on_block(
        self, block: BaseSignedBeaconBlock, perform_validation: bool = True
    ) -> None:
        self.logger.debug("attempting import of block %s", block)
        try:
            imported_block = self._import_block(block, perform_validation)
            self.logger.debug("imported new block: %s", imported_block)
            self._update_fork_choice_with_block(block.message)
        except SlashableBlockError:
            # still register a block if it is a duplicate, in event of a re-org
            # other exceptions should not add the block to the fork choice
            self._update_fork_choice_with_block(block.message)
            raise

    def on_attestation(self, attestation: Attestation) -> None:
        """
        This method expects ``attestation`` to come from the wire, not one in a
        (valid) block; attestations in blocks are handled in ``on_block``
        """
        fork_choice = self._get_fork_choice(attestation.data.slot)
        self._update_fork_choice_with_attestation(fork_choice, attestation)
