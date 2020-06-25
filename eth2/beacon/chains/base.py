from abc import ABC, abstractmethod
import logging
from typing import TYPE_CHECKING, Container, Dict, Iterable, Tuple, Type

from eth._utils.datatypes import Configurable
from eth.abc import AtomicDatabaseAPI
from eth.exceptions import BlockNotFound
from eth.validation import validate_word
from eth_utils import ValidationError, to_dict, toolz

from eth2._utils.ssz import validate_imported_block_unchanged
from eth2.beacon.chains.exceptions import ParentNotFoundError, SlashableBlockError
from eth2.beacon.constants import FAR_FUTURE_SLOT, GENESIS_SLOT
from eth2.beacon.db.chain import BaseBeaconChainDB, BeaconChainDB, StateNotFound
from eth2.beacon.fork_choice.scoring import BaseForkChoiceScoring
from eth2.beacon.helpers import get_state_root_at_slot
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.blocks import BaseBeaconBlock, BaseSignedBeaconBlock
from eth2.beacon.types.nonspec.epoch_info import EpochInfo
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Root, Slot
from eth2.clock import Tick

if TYPE_CHECKING:
    from eth2.beacon.state_machines.base import BaseBeaconStateMachine  # noqa: F401

StateMachineConfiguration = Tuple[Tuple[Slot, Type["BaseBeaconStateMachine"]], ...]


class BaseBeaconChain(Configurable, ABC):
    """
    The base class for all BeaconChain objects
    """

    chaindb: BaseBeaconChainDB
    chaindb_class: Type[BaseBeaconChainDB]
    sm_configuration: StateMachineConfiguration

    @abstractmethod
    def __init__(self, chaindb: BaseBeaconChainDB) -> None:
        ...

    @classmethod
    @abstractmethod
    def from_genesis(
        cls, base_db: AtomicDatabaseAPI, genesis_state: BeaconState
    ) -> "BaseBeaconChain":
        ...

    @abstractmethod
    def get_state_machine(self, at_slot: Slot = None) -> "BaseBeaconStateMachine":
        ...

    @abstractmethod
    def on_tick(self, tick: Tick) -> None:
        ...

    @abstractmethod
    def on_block(
        self, block: BaseSignedBeaconBlock, perform_validation: bool = True
    ) -> Tuple[
        BaseSignedBeaconBlock,
        Tuple[BaseSignedBeaconBlock, ...],
        Tuple[BaseSignedBeaconBlock, ...],
    ]:
        ...

    @abstractmethod
    def on_attestation(self, attestation: Attestation) -> None:
        ...

    #
    # State API
    #
    @abstractmethod
    def get_state_by_root(self, root: Root) -> BeaconState:
        ...

    @abstractmethod
    def get_state_by_slot(self, slot: Slot) -> BeaconState:
        ...

    @abstractmethod
    def get_head_state_slot(self) -> Slot:
        ...

    @abstractmethod
    def get_head_state(self) -> BeaconState:
        ...

    @abstractmethod
    def advance_state_to_slot(
        self, target_slot: Slot, state: BeaconState = None
    ) -> BeaconState:
        ...

    @abstractmethod
    def get_canonical_epoch_info(self) -> EpochInfo:
        ...

    #
    # Block API
    #
    @abstractmethod
    def get_block_class(self, block_root: Root) -> Type[BaseSignedBeaconBlock]:
        ...

    @abstractmethod
    def get_block_by_root(self, block_root: Root) -> BaseSignedBeaconBlock:
        ...

    @abstractmethod
    def get_canonical_head(self) -> BaseSignedBeaconBlock:
        ...

    @abstractmethod
    def get_canonical_block_by_slot(self, slot: Slot) -> BaseSignedBeaconBlock:
        ...

    @abstractmethod
    def get_canonical_block_root(self, slot: Slot) -> Root:
        ...

    @abstractmethod
    def import_block(
        self, block: BaseSignedBeaconBlock, perform_validation: bool = True
    ) -> Tuple[
        BaseSignedBeaconBlock,
        Tuple[BaseSignedBeaconBlock, ...],
        Tuple[BaseSignedBeaconBlock, ...],
    ]:
        ...

    #
    # Attestation API
    #
    @abstractmethod
    def get_attestation_by_root(self, attestation_root: Root) -> Attestation:
        ...

    @abstractmethod
    def attestation_exists(self, attestation_root: Root) -> bool:
        ...


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
    sm_configuration: StateMachineConfiguration, chain_db: BaseBeaconChainDB
) -> Iterable[Tuple[Container[int], "BaseBeaconStateMachine"]]:
    sm_configuration += ((FAR_FUTURE_SLOT, None),)
    for (first_fork, second_fork) in toolz.sliding_window(2, sm_configuration):
        valid_range = range(first_fork[0], second_fork[0])
        valid_sm = first_fork[1](chain_db)
        yield (valid_range, valid_sm)


class BeaconChain(BaseBeaconChain):
    """
    A Chain is a combination of one or more ``StateMachine`` classes.  Each ``StateMachine``
    is associated with a range of slots. The Chain class acts as a wrapper around these other
    StateMachine classes, delegating operations to the appropriate StateMachine depending on the
    current block slot number.
    """

    logger = logging.getLogger("eth2.beacon.chains.BeaconChain")

    chaindb_class: Type[BaseBeaconChainDB] = BeaconChainDB
    sm_configuration: StateMachineConfiguration = ()

    def __init__(self, chaindb: BaseBeaconChainDB) -> None:
        self.chaindb = chaindb

        _validate_sm_configuration(self.sm_configuration)
        self._state_machines_by_range: Dict[
            Container[int], "BaseBeaconStateMachine"
        ] = _load_state_machines(self.sm_configuration, self.chaindb)

    @classmethod
    def from_genesis(
        cls, base_db: AtomicDatabaseAPI, genesis_state: BeaconState
    ) -> "BeaconChain":
        # NOTE we supply this dependency so we can use the rest of the ``chain``
        # although the assumption is that ``base_db`` is empty at this point in time.
        empty_chaindb = cls.chaindb_class(base_db)

        chain = cls(empty_chaindb)
        assert genesis_state.slot == GENESIS_SLOT
        state_machine = chain.get_state_machine(at_slot=genesis_state.slot)

        genesis_scoring = state_machine.get_fork_choice_scoring()

        # NOTE: chaindb is initialized correctly now...
        chaindb = cls.chaindb_class.from_genesis(
            base_db, genesis_state, state_machine.signed_block_class, genesis_scoring
        )
        return cls(chaindb)

    def _persist_validated_state_and_block(
        self,
        state: BeaconState,
        block: BaseSignedBeaconBlock,
        scoring: BaseForkChoiceScoring,
    ) -> Tuple[Tuple[BaseBeaconBlock, ...], Tuple[BaseBeaconBlock, ...]]:
        self.chaindb.persist_state(state)

        (new_canonical_blocks, old_canonical_blocks) = self.chaindb.persist_block(
            block, block.__class__, scoring
        )

        if len(new_canonical_blocks) > 0:
            self.chaindb.update_head_state(state.slot, state.hash_tree_root)

        return new_canonical_blocks, old_canonical_blocks

    def get_state_machine(self, at_slot: Slot = None) -> "BaseBeaconStateMachine":
        """
        Return the ``StateMachine`` instance for the given slot number.
        """
        if at_slot is None:
            slot = self.chaindb.get_head_state_slot()
        else:
            slot = at_slot

        # TODO iterate over ``reversed(....items())`` once we support only >=py3.8
        for (slot_range, state_machine) in self._state_machines_by_range.items():
            if slot in slot_range:
                return state_machine
        else:
            raise Exception("state machine configuration was incorrect")

    def on_tick(self, tick: Tick) -> None:
        # TODO(ralexstokes) resolve API here
        # state_machine = self.get_state_machine(at_slot=tick.slot)
        # state_machine.on_tick(tick)
        pass

    def on_block(
        self, block: BaseSignedBeaconBlock, perform_validation: bool = True
    ) -> Tuple[
        BaseSignedBeaconBlock,
        Tuple[BaseSignedBeaconBlock, ...],
        Tuple[BaseSignedBeaconBlock, ...],
    ]:
        imported_block, new_canonical_blocks, old_canonical_blocks = self.import_block(
            block, perform_validation
        )
        # state_machine = self.get_state_machine(at_slot=block.slot)
        # state_machine.on_block(block)
        return imported_block, new_canonical_blocks, old_canonical_blocks

    def on_attestation(self, attestation: Attestation) -> None:
        # state_machine = self.get_state_machine(at_slot=attestation.data.slot)
        # state_machine.on_attestation(attestation)
        pass

    #
    # State API
    #
    def get_state_by_root(self, root: Root) -> BeaconState:
        # TODO (hwwhww): using state class of head state for now, should be configurable if we have
        # more forks.
        head_state_slot = self.chaindb.get_head_state_slot()
        state_class = self.get_state_machine(at_slot=head_state_slot).state_class
        return self.chaindb.get_state_by_root(root, state_class)

    def get_state_by_slot(self, slot: Slot) -> BeaconState:
        """
        Return the requested state as specified by slot number.
        Raise ``StateNotFound`` if there's no state with the given slot number in the db.
        """
        state_machine = self.get_state_machine(slot)
        state_class = state_machine.state_class

        # Get state_root
        try:
            # If the requested state_root is computed with a canonical block,
            # try to query the block.
            block = self.get_canonical_block_by_slot(slot)
            state_root = block.message.state_root
        except BlockNotFound:
            try:
                #  If the requested state_root is computed with skipped slot recently,
                #  try to get recent state root from state.
                config = state_machine.config
                head_state = self.get_head_state()
                state_root = get_state_root_at_slot(
                    head_state, slot, config.SLOTS_PER_HISTORICAL_ROOT
                )
            except ValidationError:
                # NOTE: We can apply state transition instead of raising `StateNotFound`
                # but this api is being indirectly exposed to outside callers, e.g.
                # a eth2 monitor requesting state at certain slot.
                # And so applying state transition could be potential DoS vector in this case.
                raise StateNotFound(f"No state root for slot #{slot})")

        return self.chaindb.get_state_by_root(state_root, state_class)

    def advance_state_to_slot(
        self, target_slot: Slot, state: BeaconState = None
    ) -> BeaconState:
        """
        NOTE: this method is a bit of a utility and it isn't clear it should live here indefinitely.
        See the comment in ``get_state_by_slot`` about DoS vectors on rolling these
        semantics into that method.
        """
        if state is None:
            state = self.get_head_state()

        current_slot = state.slot
        for slot in range(current_slot, target_slot + 1):
            slot = Slot(slot)
            state_machine = self.get_state_machine(at_slot=slot)
            state, _ = state_machine.apply_state_transition(state, future_slot=slot)
        return state

    def get_head_state_slot(self) -> Slot:
        return self.chaindb.get_head_state_slot()

    def get_head_state(self) -> BeaconState:
        head_state_slot = self.chaindb.get_head_state_slot()
        head_state_root = self.chaindb.get_head_state_root()
        state_class = self.get_state_machine(at_slot=head_state_slot).state_class
        return self.chaindb.get_state_by_root(head_state_root, state_class)

    def get_canonical_epoch_info(self) -> EpochInfo:
        return self.chaindb.get_canonical_epoch_info()

    #
    # Block API
    #
    def get_block_class(self, block_root: Root) -> Type[BaseSignedBeaconBlock]:
        slot = self.chaindb.get_slot_by_root(block_root)
        return self.get_state_machine(slot).signed_block_class

    def get_block_by_root(self, block_root: Root) -> BaseSignedBeaconBlock:
        """
        Return the requested block as specified by block hash.

        Raise ``BlockNotFound`` if there's no block with the given hash in the db.
        """
        validate_word(block_root, title="Block Root")

        block_class = self.get_block_class(block_root)
        return self.chaindb.get_block_by_root(block_root, block_class)

    def get_canonical_head(self) -> BaseSignedBeaconBlock:
        """
        Return the block at the canonical chain head.

        Raise ``CanonicalHeadNotFound`` if there's no head defined for the canonical chain.
        """
        block_root = self.chaindb.get_canonical_head_root()

        block_class = self.get_block_class(block_root)
        return self.chaindb.get_block_by_root(block_root, block_class)

    def get_canonical_block_by_slot(self, slot: Slot) -> BaseSignedBeaconBlock:
        """
        Return the block with the given number in the canonical chain.

        Raise ``BlockNotFound`` if there's no block with the given number in the
        canonical chain.
        """
        return self.get_block_by_root(self.chaindb.get_canonical_block_root(slot))

    def get_canonical_block_root(self, slot: Slot) -> Root:
        """
        Return the block hash with the given number in the canonical chain.

        Raise ``BlockNotFound`` if there's no block with the given number in the
        canonical chain.
        """
        return self.chaindb.get_canonical_block_root(slot)

    def import_block(
        self, block: BaseSignedBeaconBlock, perform_validation: bool = True
    ) -> Tuple[
        BaseSignedBeaconBlock,
        Tuple[BaseSignedBeaconBlock, ...],
        Tuple[BaseSignedBeaconBlock, ...],
    ]:
        """
        Import a complete block and returns a 3-tuple

        - the imported block
        - a tuple of blocks which are now part of the canonical chain.
        - a tuple of blocks which were canonical and now are no longer canonical.
        """
        self.logger.debug("attempting import of block %s", block)

        try:
            parent_block = self.get_block_by_root(block.parent_root)
        except BlockNotFound:
            raise ParentNotFoundError(
                f"attempt to import block {block} but missing parent block"
            )

        try:
            existing_block = self.get_canonical_block_by_slot(block.slot)
            if existing_block != block:
                # NOTE: we want to keep the block but avoid heavy state transition for now...
                # Rationale: this block may simply be a slashable block. It could also be on
                # a fork. Choose to defer the state materialization until we re-org via fork choice.
                self.chaindb.write_signed_block(block)

                raise SlashableBlockError(
                    block,
                    f"attempt to import {block} but canonical chain"
                    " already has a block at this slot",
                )
            else:
                # NOTE: block already imported!
                return block, (), ()
        except BlockNotFound:
            # NOTE: block has not been imported for ``block.slot``
            pass

        state_machine = self.get_state_machine(at_slot=parent_block.slot)
        state_class = state_machine.state_class
        state = self.chaindb.get_state_by_root(
            parent_block.message.state_root, state_class
        )

        state, imported_block = state_machine.apply_state_transition(
            state, block, check_proposer_signature=perform_validation
        )

        # Validate the imported block.
        if perform_validation:
            validate_imported_block_unchanged(imported_block, block)

        fork_choice_scoring = state_machine.get_fork_choice_scoring()
        (
            new_canonical_blocks,
            old_canonical_blocks,
        ) = self._persist_validated_state_and_block(
            state, imported_block, fork_choice_scoring
        )

        self.logger.debug("successfully imported block %s", imported_block)

        return imported_block, new_canonical_blocks, old_canonical_blocks

    #
    # Attestation API
    #
    def get_attestation_by_root(self, attestation_root: Root) -> Attestation:
        block_root, index = self.chaindb.get_attestation_key_by_root(attestation_root)
        block = self.get_block_by_root(block_root)
        return block.body.attestations[index]

    def attestation_exists(self, attestation_root: Root) -> bool:
        return self.chaindb.attestation_exists(attestation_root)
