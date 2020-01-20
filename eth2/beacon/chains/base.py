from abc import ABC, abstractmethod
import logging
from typing import TYPE_CHECKING, Tuple, Type

from eth._utils.datatypes import Configurable
from eth.abc import AtomicDatabaseAPI
from eth.exceptions import BlockNotFound
from eth.validation import validate_word
from eth_utils import ValidationError, humanize_hash

from eth2._utils.ssz import validate_imported_block_unchanged
from eth2.beacon.db.chain import BaseBeaconChainDB, BeaconChainDB, StateNotFound
from eth2.beacon.exceptions import BlockClassError, StateMachineNotFound
from eth2.beacon.fork_choice.constant import ConstantScoring
from eth2.beacon.fork_choice.scoring import BaseScore
from eth2.beacon.helpers import get_state_root_at_slot
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.blocks import BaseBeaconBlock, BaseSignedBeaconBlock
from eth2.beacon.types.nonspec.epoch_info import EpochInfo
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Root, Slot, Timestamp
from eth2.configs import Eth2Config, Eth2GenesisConfig

if TYPE_CHECKING:
    from eth2.beacon.state_machines.base import BaseBeaconStateMachine  # noqa: F401


class BaseBeaconChain(Configurable, ABC):
    """
    The base class for all BeaconChain objects
    """

    chaindb = None  # type: BaseBeaconChainDB
    chaindb_class = None  # type: Type[BaseBeaconChainDB]
    sm_configuration = (
        None
    )  # type: Tuple[Tuple[Slot, Type[BaseBeaconStateMachine]], ...]
    chain_id = None  # type: int

    @abstractmethod
    def __init__(
        self, base_db: AtomicDatabaseAPI, genesis_config: Eth2GenesisConfig
    ) -> None:
        ...

    #
    # Helpers
    #
    @classmethod
    @abstractmethod
    def get_chaindb_class(cls) -> Type[BaseBeaconChainDB]:
        ...

    #
    # Chain API
    #
    @classmethod
    @abstractmethod
    def from_genesis(
        cls,
        base_db: AtomicDatabaseAPI,
        genesis_state: BeaconState,
        genesis_block: BaseBeaconBlock,
        genesis_config: Eth2GenesisConfig,
    ) -> "BaseBeaconChain":
        ...

    #
    # State Machine API
    #
    @classmethod
    @abstractmethod
    def get_state_machine_class(
        cls, block: BaseSignedBeaconBlock
    ) -> Type["BaseBeaconStateMachine"]:
        ...

    @abstractmethod
    def get_state_machine(self, at_slot: Slot = None) -> "BaseBeaconStateMachine":
        ...

    @classmethod
    @abstractmethod
    def get_state_machine_class_for_block_slot(
        cls, slot: Slot
    ) -> Type["BaseBeaconStateMachine"]:
        ...

    @classmethod
    @abstractmethod
    def get_genesis_state_machine_class(self) -> Type["BaseBeaconStateMachine"]:
        ...

    @abstractmethod
    def on_tick(self, time: Timestamp, slot: Slot = None) -> None:
        ...

    @abstractmethod
    def on_block(self, block: BaseSignedBeaconBlock, slot: Slot = None) -> None:
        ...

    @abstractmethod
    def on_attestation(self, attestation: Attestation, slot: Slot = None) -> None:
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
    def get_score(self, block_root: Root) -> BaseScore:
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


class BeaconChain(BaseBeaconChain):
    """
    A Chain is a combination of one or more ``StateMachine`` classes.  Each ``StateMachine``
    is associated with a range of slots. The Chain class acts as a wrapper around these other
    StateMachine classes, delegating operations to the appropriate StateMachine depending on the
    current block slot number.
    """

    logger = logging.getLogger("eth2.beacon.chains.BeaconChain")

    chaindb_class = BeaconChainDB  # type: Type[BaseBeaconChainDB]

    def __init__(
        self, base_db: AtomicDatabaseAPI, genesis_config: Eth2GenesisConfig
    ) -> None:
        if not self.sm_configuration:
            raise ValueError(
                "The Chain class cannot be instantiated with an empty `sm_configuration`"
            )
        else:
            # TODO implment validate_sm_configuration(self.sm_configuration)
            # validate_sm_configuration(self.sm_configuration)
            pass

        self.chaindb = self.get_chaindb_class()(base_db, genesis_config)

    #
    # Helpers
    #
    @classmethod
    def get_chaindb_class(cls) -> Type["BaseBeaconChainDB"]:
        if cls.chaindb_class is None:
            raise AttributeError("`chaindb_class` not set")
        return cls.chaindb_class

    def get_config_by_slot(self, slot: Slot) -> Eth2Config:
        return self.get_state_machine_class_for_block_slot(slot).config

    #
    # Chain API
    #
    @classmethod
    def from_genesis(
        cls,
        base_db: AtomicDatabaseAPI,
        genesis_state: BeaconState,
        genesis_block: BaseBeaconBlock,
        genesis_config: Eth2GenesisConfig,
    ) -> "BaseBeaconChain":
        """
        Initialize the ``BeaconChain`` from a genesis state and block.
        """
        sm_class = cls.get_state_machine_class_for_block_slot(genesis_block.slot)
        if type(genesis_block) != sm_class.block_class:
            raise BlockClassError(
                f"Given genesis block class: {type(genesis_block)}, "
                f"StateMachine.block_class: {sm_class.block_class}"
            )

        chaindb = cls.get_chaindb_class()(db=base_db, genesis_config=genesis_config)
        chaindb.persist_state(genesis_state)

        genesis_scoring_class = sm_class.get_fork_choice_scoring_class()
        genesis_score_class = genesis_scoring_class.get_score_class()
        genesis_score = genesis_score_class.from_genesis(genesis_state, genesis_block)
        genesis_scoring = ConstantScoring(genesis_score)
        chaindb.persist_block(
            sm_class.signed_block_class.create(message=genesis_block),
            sm_class.signed_block_class,
            genesis_scoring,
        )

        return cls(base_db, genesis_config)

    #
    # StateMachine API
    #
    @classmethod
    def get_state_machine_class(
        cls, block: BaseSignedBeaconBlock
    ) -> Type["BaseBeaconStateMachine"]:
        """
        Returns the ``StateMachine`` instance for the given block slot number.
        """
        return cls.get_state_machine_class_for_block_slot(block.slot)

    @classmethod
    def get_state_machine_class_for_block_slot(
        cls, slot: Slot
    ) -> Type["BaseBeaconStateMachine"]:
        """
        Return the ``StateMachine`` class for the given block slot number.
        """
        if cls.sm_configuration is None:
            raise AttributeError(
                "Chain classes must define the StateMachines in sm_configuration"
            )

        for start_slot, sm_class in reversed(cls.sm_configuration):
            if slot >= start_slot:
                return sm_class
        raise StateMachineNotFound(
            "No StateMachine available for block slot: #{0}".format(slot)
        )

    def get_state_machine(self, at_slot: Slot = None) -> "BaseBeaconStateMachine":
        """
        Return the ``StateMachine`` instance for the given slot number.
        """
        if at_slot is None:
            slot = self.chaindb.get_head_state_slot()
        else:
            slot = at_slot
        sm_class = self.get_state_machine_class_for_block_slot(slot)

        return sm_class(chaindb=self.chaindb)

    @classmethod
    def get_genesis_state_machine_class(cls) -> Type["BaseBeaconStateMachine"]:
        return cls.sm_configuration[0][1]

    # TODO how to handle the current slot in fork choice handlers
    def on_tick(self, time: Timestamp, slot: Slot = None) -> None:
        state_machine = self.get_state_machine(at_slot=slot)
        state_machine.on_tick(time)

    def on_block(self, block: BaseSignedBeaconBlock, slot: Slot = None) -> None:
        state_machine = self.get_state_machine(at_slot=slot)
        state_machine.on_block(block)

    def on_attestation(self, attestation: Attestation, slot: Slot = None) -> None:
        state_machine = self.get_state_machine(at_slot=slot)
        state_machine.on_attestation(attestation)

    #
    # State API
    #
    def get_state_by_root(self, root: Root) -> BeaconState:
        # TODO (hwwhww): using state class of head state for now, should be configurable if we have
        # more forks.
        head_state_slot = self.chaindb.get_head_state_slot()
        state_class = self.get_state_machine(at_slot=head_state_slot).get_state_class()
        return self.chaindb.get_state_by_root(root, state_class)

    def get_state_by_slot(self, slot: Slot) -> BeaconState:
        """
        Return the requested state as specified by slot number.
        Raise ``StateNotFound`` if there's no state with the given slot number in the db.
        """
        sm_class = self.get_state_machine_class_for_block_slot(slot)
        state_class = sm_class.get_state_class()

        # Get state_root
        try:
            # If the requested state_root is computed with a canonical block,
            # try to query the block.
            config = self.get_config_by_slot(slot)
            block = self.get_canonical_block_by_slot(slot)
            state_root = block.message.state_root
        except BlockNotFound:
            try:
                #  If the requested state_root is computed with skipped slot recently,
                #  try to get recent state root from state.
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

    def get_head_state_slot(self) -> Slot:
        return self.chaindb.get_head_state_slot()

    def get_head_state(self) -> BeaconState:
        head_state_slot = self.chaindb.get_head_state_slot()
        head_state_root = self.chaindb.get_head_state_root()
        state_class = self.get_state_machine(at_slot=head_state_slot).get_state_class()
        return self.chaindb.get_state_by_root(head_state_root, state_class)

    def get_canonical_epoch_info(self) -> EpochInfo:
        return self.chaindb.get_canonical_epoch_info()

    #
    # Block API
    #
    def get_block_class(self, block_root: Root) -> Type[BaseSignedBeaconBlock]:
        slot = self.chaindb.get_slot_by_root(block_root)
        sm_class = self.get_state_machine_class_for_block_slot(slot)
        signed_block_class = sm_class.signed_block_class
        return signed_block_class

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

    def get_score(self, block_root: Root) -> BaseScore:
        """
        Return the score of the block with the given hash.

        Raise ``BlockNotFound`` if there is no matching black hash.
        """
        block = self.get_block_by_root(block_root)
        slot = block.slot
        state_machine = self.get_state_machine(at_slot=slot)
        fork_choice_scoring = state_machine.get_fork_choice_scoring()
        return self.chaindb.get_score(block_root, fork_choice_scoring.get_score_class())

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
        self.logger.debug(
            "attempting import of block with slot %s and root %s",
            block.slot,
            humanize_hash(block.message.hash_tree_root),
        )

        try:
            parent_block = self.get_block_by_root(block.parent_root)
        except BlockNotFound:
            raise ValidationError(
                "Attempt to import block #{}.  Cannot import block {} before importing "
                "its parent block at {}".format(
                    block.slot,
                    humanize_hash(block.message.hash_tree_root),
                    humanize_hash(block.parent_root),
                )
            )

        state_machine = self.get_state_machine(at_slot=parent_block.slot)
        state_class = state_machine.get_state_class()
        state = self.chaindb.get_state_by_root(
            parent_block.message.state_root, state_class
        )

        state, imported_block = state_machine.import_block(
            block, state, check_proposer_signature=perform_validation
        )

        # Validate the imported block.
        if perform_validation:
            validate_imported_block_unchanged(imported_block, block)

        # TODO: Now it just persists all state. Should design how to clean up the old state.
        self.chaindb.persist_state(state)

        fork_choice_scoring = state_machine.get_fork_choice_scoring()
        (new_canonical_blocks, old_canonical_blocks) = self.chaindb.persist_block(
            imported_block, imported_block.__class__, fork_choice_scoring
        )

        # Set the state of new (canonical) block as head state.
        if len(new_canonical_blocks) > 0:
            self.chaindb.update_head_state(state.slot, state.hash_tree_root)

        self.logger.debug(
            "successfully imported block at slot %s with root %s",
            imported_block.slot,
            humanize_hash(imported_block.message.hash_tree_root),
        )

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
