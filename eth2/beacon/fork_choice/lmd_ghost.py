from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple, Type, cast

from eth_typing import Hash32
from eth_utils import ValidationError
import ssz

from eth2.beacon.attestation_helpers import validate_indexed_attestation
from eth2.beacon.constants import ZERO_ROOT
from eth2.beacon.db.chain import BaseBeaconChainDB
from eth2.beacon.epoch_processing_helpers import get_indexed_attestation
from eth2.beacon.fork_choice.scoring import BaseForkChoiceScoring, BaseScore
from eth2.beacon.helpers import (
    compute_epoch_at_slot,
    compute_start_slot_at_epoch,
    get_active_validator_indices,
)
from eth2.beacon.state_machines.base import BaseBeaconStateMachine
from eth2.beacon.state_machines.forks.serenity.slot_processing import process_slots
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.blocks import BaseBeaconBlock, BaseSignedBeaconBlock
from eth2.beacon.types.checkpoints import Checkpoint
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Epoch, Gwei, Root, Slot, Timestamp, ValidatorIndex
from eth2.configs import CommitteeConfig, Eth2Config

LMD_GHOST_SCORE_DATA_LENGTH = 2


def score_block_by_root(root: Root) -> int:
    return int.from_bytes(root, byteorder="big")


class LMDGHOSTScore(BaseScore):
    # First score by the latest attesting balance, and then score by block root
    _score: Tuple[Gwei, int]

    def __init__(self, score: Tuple[Gwei, int]) -> None:
        self._score = score

    def __lt__(self, other: "LMDGHOSTScore") -> bool:
        return self._score < other._score

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, LMDGHOSTScore):
            return NotImplemented
        return self._score == other._score

    def serialize(self) -> bytes:
        return ssz.encode(
            self._score,
            sedes=ssz.sedes.Vector(ssz.sedes.uint256, LMD_GHOST_SCORE_DATA_LENGTH),
        )

    @classmethod
    def deserialize(cls, data: bytes) -> "LMDGHOSTScore":
        score = ssz.decode(
            data, sedes=ssz.sedes.Vector(ssz.sedes.uint256, LMD_GHOST_SCORE_DATA_LENGTH)
        )
        score_tuple = cast(Tuple[Gwei, int], tuple(score))
        return cls(score_tuple)

    @classmethod
    def from_genesis(
        cls, genesis_state: BeaconState, genesis_block: BaseBeaconBlock
    ) -> BaseScore:
        score = (Gwei(0), score_block_by_root(ZERO_ROOT))
        return cls(score)


def compute_slots_since_epoch_start(slot: Slot, slots_per_epoch: int) -> Slot:
    return Slot(
        slot
        - compute_start_slot_at_epoch(
            compute_epoch_at_slot(slot, slots_per_epoch), slots_per_epoch
        )
    )


@dataclass(eq=True, frozen=True)
class LatestMessage:
    epoch: Epoch
    root: Root


@dataclass
class Context:
    time: Timestamp
    genesis_time: Timestamp
    justified_checkpoint: Checkpoint
    finalized_checkpoint: Checkpoint
    best_justified_checkpoint: Checkpoint
    blocks: Dict[Root, BaseBeaconBlock] = field(default_factory=dict)
    block_states: Dict[Hash32, BeaconState] = field(default_factory=dict)
    checkpoint_states: Dict[Checkpoint, BeaconState] = field(default_factory=dict)
    latest_messages: Dict[ValidatorIndex, LatestMessage] = field(default_factory=dict)

    @classmethod
    def from_genesis(
        cls, genesis_state: BeaconState, genesis_block: BaseBeaconBlock
    ) -> "Context":
        return cls.at_time(genesis_state.genesis_time, genesis_state, genesis_block)

    @classmethod
    def at_time(
        cls, current_time: Timestamp, state: BeaconState, block: BaseBeaconBlock
    ) -> "Context":
        """
        Return a ``Context`` based on the ``current_time`` and the latest known tip of
        the chain at that point in time.
        """
        assert state.slot == block.slot
        root = block.message.hash_tree_root
        justified_checkpoint = state.current_justified_checkpoint
        finalized_checkpoint = state.finalized_checkpoint
        return Context(
            time=current_time,
            genesis_time=state.genesis_time,
            justified_checkpoint=justified_checkpoint,
            finalized_checkpoint=finalized_checkpoint,
            best_justified_checkpoint=justified_checkpoint,
            blocks={root: block},
            block_states={root: state},
            checkpoint_states={justified_checkpoint: state},
        )

    def to_bytes(self) -> bytes:
        raise NotImplementedError()

    @classmethod
    def from_bytes(cls, data: bytes) -> "Context":
        raise NotImplementedError()

    @property
    def finalized_slot(self) -> Slot:
        return self.blocks[self.finalized_checkpoint.root].slot

    @property
    def justified_slot(self) -> Slot:
        return self.blocks[self.justified_checkpoint.root].slot


def _effective_balance_for_validator(
    state: BeaconState, validator_index: ValidatorIndex
) -> Gwei:
    return state.validators[validator_index].effective_balance


class Store:
    _db: BaseBeaconChainDB
    _block_class: Type[BaseSignedBeaconBlock]
    _config: Eth2Config
    _context: Context

    def __init__(
        self,
        chain_db: BaseBeaconChainDB,
        block_class: Type[BaseSignedBeaconBlock],
        config: Eth2Config,
        context: Context,
    ):
        self._db = chain_db
        self._block_class = block_class
        self._config = config
        self._context = context

    @property
    def slots_per_epoch(self) -> int:
        return self._config.SLOTS_PER_EPOCH

    def _get_slots_since_genesis(self) -> int:
        return (
            self._context.time - self._context.genesis_time
        ) // self._config.SECONDS_PER_SLOT

    def get_current_slot(self) -> Slot:
        return Slot(self._config.GENESIS_SLOT + self._get_slots_since_genesis())

    def _get_block_by_root(self, root: Root) -> BaseBeaconBlock:
        return self._db.get_block_by_root(root, self._block_class).message

    def get_ancestor_root(self, root: Root, slot: Slot) -> Optional[Root]:
        """
        Return the block root in the chain that is a
        predecessor of the block with ``root`` at the requested ``slot``.
        """
        block = self._get_block_by_root(root)
        if block.slot > slot:
            return self.get_ancestor_root(block.parent_root, slot)
        elif block.slot == slot:
            return root
        else:
            return None

    def _get_checkpoint_state_for(self, checkpoint: Checkpoint) -> BeaconState:
        return self._context.checkpoint_states[checkpoint]

    def _latest_message_for_index(self, index: ValidatorIndex) -> LatestMessage:
        return self._context.latest_messages[index]

    def get_latest_attesting_balance(self, root: Root) -> Gwei:
        state = self._get_checkpoint_state_for(self._context.justified_checkpoint)
        active_indices = get_active_validator_indices(
            state.validators, state.current_epoch(self.slots_per_epoch)
        )
        return Gwei(
            sum(
                _effective_balance_for_validator(state, i)
                for i in active_indices
                if (
                    i in self._context.latest_messages
                    and self.get_ancestor_root(
                        self._latest_message_for_index(i).root,
                        self._get_block_by_root(root).slot,
                    )
                    == root
                )
            )
        )

    def _should_update_justified_checkpoint(
        self, new_justified_checkpoint: Checkpoint
    ) -> bool:
        """
        To address the bouncing attack, only update conflicting justified
        checkpoints in the fork choice if in the early slots of the epoch.
        Otherwise, delay incorporation of new justified checkpoint until next epoch boundary.
        See https://ethresear.ch/t/prevention-of-bouncing-attack-on-ffg/6114 for more
        detailed analysis and discussion.
        """
        current_slot = self.get_current_slot()
        slots_since_epoch_start = compute_slots_since_epoch_start(
            current_slot, self._config.SLOTS_PER_EPOCH
        )
        within_safe_slots = (
            slots_since_epoch_start < self._config.SAFE_SLOTS_TO_UPDATE_JUSTIFIED
        )
        if within_safe_slots:
            return True

        new_justified_block = self._context.blocks[new_justified_checkpoint.root]
        justified_epoch = self._context.justified_checkpoint.epoch
        if new_justified_block.slot <= compute_start_slot_at_epoch(
            justified_epoch, self._config.SLOTS_PER_EPOCH
        ):
            return False

        justified_root = self._context.justified_checkpoint.root
        justified_ancestor = self.get_ancestor_root(
            new_justified_checkpoint.root, self._context.justified_slot
        )
        return justified_ancestor == justified_root

    def on_tick(self, time: Timestamp) -> None:
        previous_slot = self.get_current_slot()

        self._context.time = time

        current_slot = self.get_current_slot()

        is_new_epoch = (
            current_slot > previous_slot
            and compute_slots_since_epoch_start(
                current_slot, self._config.SLOTS_PER_EPOCH
            )
            == 0
        )
        if not is_new_epoch:
            return

        is_better_checkpoint_known = (
            self._context.best_justified_checkpoint.epoch
            > self._context.justified_checkpoint.epoch
        )
        if is_better_checkpoint_known:
            self._context.justified_checkpoint = self._context.best_justified_checkpoint

    def on_block(
        self,
        signed_block: BaseSignedBeaconBlock,
        post_state: BeaconState = None,
        state_machine: BaseBeaconStateMachine = None,
    ) -> None:
        """
        Handler to update the fork choice context upon receiving a new ``block``.

        This handler requests the ``post_state`` of this block to avoid recomputing
        it if it is already known.
        """
        block = signed_block.message
        # NOTE: this invariant should hold based on how we handle
        # block importing in the chain but we will sanity check for now
        assert block.parent_root in self._context.block_states

        pre_state = self._context.block_states[block.parent_root]

        # NOTE: this invariant should hold based on how we handle
        # block importing in the chain but we will sanity check for now
        assert self.get_current_slot() >= block.slot

        root = block.hash_tree_root

        self._context.blocks[root] = block

        finalized_slot = self._context.finalized_slot
        finalized_ancestor = self.get_ancestor_root(root, finalized_slot)
        is_ancestor_of_finalized_block = (
            finalized_ancestor == self._context.finalized_checkpoint.root
        )
        if not is_ancestor_of_finalized_block:
            raise ValidationError(
                f"block with root {root.hex()} is not a descendant of the finalized"
                f" checkpoint with root {finalized_ancestor.hex()}"
            )

        # NOTE: sanity check implied by the previous verification on finalized ancestor
        assert block.slot > compute_start_slot_at_epoch(
            self._context.finalized_checkpoint.epoch, self._config.SLOTS_PER_EPOCH
        )

        if not post_state:
            # NOTE: error to not provide a post_state and not provide a way to compute it
            assert state_machine is not None
            post_state, _ = state_machine.import_block(signed_block, pre_state)

        self._context.block_states[root] = post_state

        if (
            post_state.current_justified_checkpoint.epoch
            > self._context.justified_checkpoint.epoch
        ):
            if (
                post_state.current_justified_checkpoint.epoch
                > self._context.best_justified_checkpoint.epoch
            ):
                self._context.best_justified_checkpoint = (
                    post_state.current_justified_checkpoint
                )
            if self._should_update_justified_checkpoint(
                post_state.current_justified_checkpoint
            ):
                self._context.justified_checkpoint = (
                    post_state.current_justified_checkpoint
                )

        if (
            post_state.finalized_checkpoint.epoch
            > self._context.finalized_checkpoint.epoch
        ):
            self._context.finalized_checkpoint = post_state.finalized_checkpoint

    def on_attestation(
        self, attestation: Attestation, validate_signature: bool = True
    ) -> None:
        target = attestation.data.target
        current_epoch = compute_epoch_at_slot(
            self.get_current_slot(), self._config.SLOTS_PER_EPOCH
        )
        previous_epoch = (
            current_epoch - 1
            if current_epoch > self._config.GENESIS_EPOCH
            else self._config.GENESIS_EPOCH
        )
        if target.epoch not in (current_epoch, previous_epoch):
            raise ValidationError(
                "Attestations must be from the current or previous epoch"
            )

        if target.epoch != compute_epoch_at_slot(
            attestation.data.slot, self._config.SLOTS_PER_EPOCH
        ):
            raise ValidationError(
                "Attestation's slot is not for the epoch given in the target"
            )

        if target.root not in self._context.blocks:
            raise ValidationError("Attestation targets a block we have not seen")

        base_state = self._context.block_states[target.root]
        slot_of_target_epoch = compute_start_slot_at_epoch(
            target.epoch, self._config.SLOTS_PER_EPOCH
        )
        if self.get_current_slot() < slot_of_target_epoch:
            raise ValidationError("Attestation cannot be for a future epoch")

        beacon_block_root = attestation.data.beacon_block_root
        if beacon_block_root not in self._context.blocks:
            raise ValidationError("Attestations must be for a known block")

        if self._context.blocks[beacon_block_root].slot > attestation.data.slot:
            raise ValidationError("Attestations must not be for a block in the future")

        if target not in self._context.checkpoint_states:
            base_state = process_slots(
                base_state,
                compute_start_slot_at_epoch(target.epoch, self._config.SLOTS_PER_EPOCH),
                self._config,
            )
            self._context.checkpoint_states[target] = base_state
        target_state = self._context.checkpoint_states[target]

        if self.get_current_slot() < attestation.data.slot + 1:
            raise ValidationError(
                "Attestations can only affect the fork choice of future slots"
            )

        # TODO: has this validation already been performed?
        indexed_attestation = get_indexed_attestation(
            target_state, attestation, CommitteeConfig(self._config)
        )
        validate_indexed_attestation(
            target_state,
            indexed_attestation,
            self._config.MAX_VALIDATORS_PER_COMMITTEE,
            self._config.SLOTS_PER_EPOCH,
            validate_signature=validate_signature,
        )

        for i in indexed_attestation.attesting_indices:
            if (
                i not in self._context.latest_messages
                or target.epoch > self._context.latest_messages[i].epoch
            ):
                self._context.latest_messages[i] = LatestMessage(
                    epoch=target.epoch, root=attestation.data.beacon_block_root
                )

    def scoring(self, block: BaseBeaconBlock) -> LMDGHOSTScore:
        """
        Return the score of the target ``block`` according to the LMD GHOST algorithm,
        using the lexicographic ordering of the block root to break ties.
        """
        root = block.message.hash_tree_root

        attestation_score = self.get_latest_attesting_balance(root)
        block_root_score = score_block_by_root(root)

        return LMDGHOSTScore((attestation_score, block_root_score))


class LMDGHOSTScoring(BaseForkChoiceScoring):
    def __init__(self, store: Store) -> None:
        self._store = store

    @classmethod
    def get_score_class(cls) -> Type[BaseScore]:
        return LMDGHOSTScore

    def score(self, block: BaseBeaconBlock) -> BaseScore:
        return self._store.scoring(block)
