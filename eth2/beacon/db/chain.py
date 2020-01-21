from abc import ABC, abstractmethod
from typing import Iterable, Optional, Tuple, Type, cast

from cytoolz import concat, first, sliding_window
from eth.abc import AtomicDatabaseAPI, DatabaseAPI
from eth.exceptions import BlockNotFound, CanonicalHeadNotFound, ParentNotFound
from eth.validation import validate_word
from eth_typing import Hash32
from eth_utils import ValidationError, encode_hex, to_tuple
from lru import LRU
import ssz

from eth2.beacon.constants import ZERO_ROOT
from eth2.beacon.db.exceptions import (
    AttestationRootNotFound,
    EpochInfoNotFound,
    FinalizedHeadNotFound,
    HeadStateSlotNotFound,
    JustifiedHeadNotFound,
    MissingForkChoiceContext,
    MissingForkChoiceScorings,
    StateNotFound,
)
from eth2.beacon.db.schema import SchemaV1
from eth2.beacon.fork_choice.scoring import BaseForkChoiceScoring, BaseScore
from eth2.beacon.helpers import compute_epoch_at_slot
from eth2.beacon.types.blocks import BaseBeaconBlock, BaseSignedBeaconBlock
from eth2.beacon.types.nonspec.epoch_info import EpochInfo
from eth2.beacon.types.states import BeaconState  # noqa: F401
from eth2.beacon.typing import Epoch, Root, Slot
from eth2.configs import Eth2GenesisConfig

# When performing a chain sync (either fast or regular modes), we'll very often need to look
# up recent blocks to validate the chain, and decoding their SSZ representation is
# relatively expensive so we cache that here, but use a small cache because we *should* only
# be looking up recent blocks. We cache by root instead of ssz representation as ssz
# representation is not unique if different length configs are considered
state_cache = LRU(128)
block_cache = LRU(128)


class AttestationKey(ssz.Serializable):
    fields = [("block_root", ssz.sedes.bytes32), ("index", ssz.sedes.uint8)]


class BaseBeaconChainDB(ABC):
    db: AtomicDatabaseAPI = None

    @abstractmethod
    def __init__(
        self, db: AtomicDatabaseAPI, genesis_config: Eth2GenesisConfig
    ) -> None:
        ...

    #
    # Block API
    #
    @abstractmethod
    def persist_block(
        self,
        block: BaseBeaconBlock,
        block_class: Type[BaseBeaconBlock],
        fork_choice_scoring: BaseForkChoiceScoring,
    ) -> Tuple[Tuple[BaseBeaconBlock, ...], Tuple[BaseBeaconBlock, ...]]:
        ...

    @abstractmethod
    def get_canonical_block_root(self, slot: Slot) -> Root:
        ...

    @abstractmethod
    def get_genesis_block_root(self) -> Root:
        ...

    @abstractmethod
    def get_canonical_block_by_slot(
        self, slot: Slot, block_class: Type[BaseBeaconBlock]
    ) -> BaseBeaconBlock:
        ...

    @abstractmethod
    def get_canonical_head(self, block_class: Type[BaseBeaconBlock]) -> BaseBeaconBlock:
        ...

    @abstractmethod
    def get_canonical_head_root(self) -> Root:
        ...

    @abstractmethod
    def get_finalized_head(self, block_class: Type[BaseBeaconBlock]) -> BaseBeaconBlock:
        ...

    @abstractmethod
    def get_justified_head(self, block_class: Type[BaseBeaconBlock]) -> BaseBeaconBlock:
        ...

    @abstractmethod
    def get_block_by_root(
        self, block_root: Root, block_class: Type[BaseBeaconBlock]
    ) -> BaseBeaconBlock:
        ...

    @abstractmethod
    def get_slot_by_root(self, block_root: Root) -> Slot:
        ...

    @abstractmethod
    def get_score(self, block_root: Root, score_class: Type[BaseScore]) -> BaseScore:
        ...

    @abstractmethod
    def block_exists(self, block_root: Root) -> bool:
        ...

    @abstractmethod
    def persist_block_chain(
        self,
        blocks: Iterable[BaseBeaconBlock],
        block_class: Type[BaseBeaconBlock],
        fork_choice_scoring: Iterable[BaseForkChoiceScoring],
    ) -> Tuple[Tuple[BaseBeaconBlock, ...], Tuple[BaseBeaconBlock, ...]]:
        ...

    @abstractmethod
    def set_score(self, block: BaseBeaconBlock, score: BaseScore) -> None:
        ...

    #
    # Beacon State
    #
    @abstractmethod
    def update_head_state(self, slot: Slot, root: Hash32) -> None:
        ...

    @abstractmethod
    def get_head_state_slot(self) -> Slot:
        ...

    @abstractmethod
    def get_head_state_root(self) -> Hash32:
        ...

    @abstractmethod
    def get_state_by_root(
        self, state_root: Hash32, state_class: Type[BeaconState]
    ) -> BeaconState:
        ...

    @abstractmethod
    def persist_state(self, state: BeaconState) -> None:
        ...

    #
    # Attestation API
    #
    @abstractmethod
    def get_attestation_key_by_root(self, attestation_root: Root) -> Tuple[Root, int]:
        ...

    @abstractmethod
    def attestation_exists(self, attestation_root: Root) -> bool:
        ...

    #
    # Fork choice API
    #
    @abstractmethod
    def get_fork_choice_context_data_for(self, fork: str) -> bytes:
        ...

    @abstractmethod
    def persist_fork_choice_context(self, serialized_context: bytes, fork: str) -> None:
        ...

    @abstractmethod
    def get_canonical_epoch_info(self) -> EpochInfo:
        ...

    #
    # Raw Database API
    #
    @abstractmethod
    def exists(self, key: bytes) -> bool:
        ...

    @abstractmethod
    def get(self, key: bytes) -> bytes:
        ...


class BeaconChainDB(BaseBeaconChainDB):
    def __init__(
        self, db: AtomicDatabaseAPI, genesis_config: Eth2GenesisConfig
    ) -> None:
        self.db = db
        self.genesis_config = genesis_config

        self._finalized_root = self._get_finalized_root_if_present(db)
        self._highest_justified_epoch = self._get_highest_justified_epoch(db)

    def _get_finalized_root_if_present(self, db: DatabaseAPI) -> Root:
        try:
            return self._get_finalized_head_root(db)
        except FinalizedHeadNotFound:
            return ZERO_ROOT

    def _get_highest_justified_epoch(self, db: DatabaseAPI) -> Epoch:
        try:
            justified_head_root = self._get_justified_head_root(db)
            slot = self.get_slot_by_root(justified_head_root)
            return compute_epoch_at_slot(slot, self.genesis_config.SLOTS_PER_EPOCH)
        except JustifiedHeadNotFound:
            return self.genesis_config.GENESIS_EPOCH

    def persist_block(
        self,
        block: BaseSignedBeaconBlock,
        block_class: Type[BaseSignedBeaconBlock],
        fork_choice_scoring: BaseForkChoiceScoring,
    ) -> Tuple[Tuple[BaseSignedBeaconBlock, ...], Tuple[BaseSignedBeaconBlock, ...]]:
        """
        Persist the given block.
        """
        with self.db.atomic_batch() as db:
            if block.is_genesis:
                self._handle_exceptional_justification_and_finality(block)

            return self._persist_block(db, block, block_class, fork_choice_scoring)

    @classmethod
    def _persist_block(
        cls,
        db: DatabaseAPI,
        block: BaseBeaconBlock,
        block_class: Type[BaseBeaconBlock],
        fork_choice_scoring: BaseForkChoiceScoring,
    ) -> Tuple[Tuple[BaseBeaconBlock, ...], Tuple[BaseBeaconBlock, ...]]:
        block_chain = (block,)
        scorings = (fork_choice_scoring,)
        new_canonical_blocks, old_canonical_blocks = cls._persist_block_chain(
            db, block_chain, block_class, scorings
        )

        return new_canonical_blocks, old_canonical_blocks

    #
    #
    # Copied from HeaderDB
    #
    #

    #
    # Canonical Chain API
    #
    def get_canonical_block_root(self, slot: Slot) -> Root:
        """
        Return the block root for the canonical block at the given number.

        Raise BlockNotFound if there's no block with the given number in the
        canonical chain.
        """
        return self._get_canonical_block_root(self.db, slot)

    def get_genesis_block_root(self) -> Root:
        return self._get_canonical_block_root(self.db, self.genesis_config.GENESIS_SLOT)

    @staticmethod
    def _get_canonical_block_root(db: DatabaseAPI, slot: Slot) -> Root:
        slot_to_root_key = SchemaV1.make_block_slot_to_root_lookup_key(slot)
        try:
            root = db[slot_to_root_key]
        except KeyError:
            raise BlockNotFound("No canonical block for block slot #{0}".format(slot))
        else:
            return cast(Root, root)

    def get_canonical_block_by_slot(
        self, slot: Slot, block_class: Type[BaseBeaconBlock]
    ) -> BaseBeaconBlock:
        """
        Return the block with the given slot in the canonical chain.

        Raise BlockNotFound if there's no block with the given slot in the
        canonical chain.
        """
        return self._get_canonical_block_by_slot(self.db, slot, block_class)

    @classmethod
    def _get_canonical_block_by_slot(
        cls, db: DatabaseAPI, slot: Slot, block_class: Type[BaseBeaconBlock]
    ) -> BaseBeaconBlock:
        canonical_block_root = cls._get_canonical_block_root(db, slot)
        return cls._get_block_by_root(db, canonical_block_root, block_class)

    def get_canonical_head(self, block_class: Type[BaseBeaconBlock]) -> BaseBeaconBlock:
        """
        Return the current block at the head of the chain.
        """
        return self._get_canonical_head(self.db, block_class)

    @classmethod
    def _get_canonical_head(
        cls, db: DatabaseAPI, block_class: Type[BaseBeaconBlock]
    ) -> BaseBeaconBlock:
        canonical_head_root = cls._get_canonical_head_root(db)
        return cls._get_block_by_root(db, Root(canonical_head_root), block_class)

    def get_canonical_head_root(self) -> Root:
        """
        Return the current block root at the head of the chain.
        """
        return self._get_canonical_head_root(self.db)

    @staticmethod
    def _get_canonical_head_root(db: DatabaseAPI) -> Root:
        try:
            canonical_head_root = db[SchemaV1.make_canonical_head_root_lookup_key()]
        except KeyError:
            raise CanonicalHeadNotFound("No canonical head set for this chain")
        return cast(Root, canonical_head_root)

    def get_finalized_head(self, block_class: Type[BaseBeaconBlock]) -> BaseBeaconBlock:
        """
        Return the finalized head.
        """
        return self._get_finalized_head(self.db, block_class)

    @classmethod
    def _get_finalized_head(
        cls, db: DatabaseAPI, block_class: Type[BaseBeaconBlock]
    ) -> BaseBeaconBlock:
        finalized_head_root = cls._get_finalized_head_root(db)
        return cls._get_block_by_root(db, finalized_head_root, block_class)

    @staticmethod
    def _get_finalized_head_root(db: DatabaseAPI) -> Root:
        try:
            finalized_head_root = db[SchemaV1.make_finalized_head_root_lookup_key()]
        except KeyError:
            raise FinalizedHeadNotFound("No finalized head set for this chain")
        return cast(Root, finalized_head_root)

    def get_justified_head(self, block_class: Type[BaseBeaconBlock]) -> BaseBeaconBlock:
        """
        Return the justified head.
        """
        return self._get_justified_head(self.db, block_class)

    @classmethod
    def _get_justified_head(
        cls, db: DatabaseAPI, block_class: Type[BaseBeaconBlock]
    ) -> BaseBeaconBlock:
        justified_head_root = cls._get_justified_head_root(db)
        return cls._get_block_by_root(db, Root(justified_head_root), block_class)

    @staticmethod
    def _get_justified_head_root(db: DatabaseAPI) -> Root:
        try:
            justified_head_root = db[SchemaV1.make_justified_head_root_lookup_key()]
        except KeyError:
            raise JustifiedHeadNotFound("No justified head set for this chain")
        return cast(Root, justified_head_root)

    def get_block_by_root(
        self, block_root: Root, block_class: Type[BaseBeaconBlock]
    ) -> BaseBeaconBlock:
        return self._get_block_by_root(self.db, block_root, block_class)

    @staticmethod
    def _get_block_by_root(
        db: DatabaseAPI, block_root: Root, block_class: Type[BaseSignedBeaconBlock]
    ) -> BaseBeaconBlock:
        """
        Return the requested block header as specified by block root.

        Raise BlockNotFound if it is not present in the db.
        """
        validate_word(block_root, title="block root")

        if block_root in block_cache and block_root in db:
            return block_cache[block_root]

        try:
            block_ssz = db[block_root]
        except KeyError:
            raise BlockNotFound(
                "No block with root {0} found".format(encode_hex(block_root))
            )

        block = ssz.decode(block_ssz, block_class)
        block_cache[block_root] = block
        return block

    def get_slot_by_root(self, block_root: Root) -> Slot:
        """
        Return the requested block header as specified by block root.

        Raise BlockNotFound if it is not present in the db.
        """
        return self._get_slot_by_root(self.db, block_root)

    @staticmethod
    def _get_slot_by_root(db: DatabaseAPI, block_root: Root) -> Slot:
        validate_word(block_root, title="block root")
        try:
            encoded_slot = db[SchemaV1.make_block_root_to_slot_lookup_key(block_root)]
        except KeyError:
            raise BlockNotFound(
                "No block with root {0} found".format(encode_hex(block_root))
            )
        return Slot(ssz.decode(encoded_slot, sedes=ssz.sedes.uint64))

    def get_score(self, block_root: Root, score_class: Type[BaseScore]) -> BaseScore:
        return self._get_score(self.db, block_root, score_class)

    @staticmethod
    def _get_score(
        db: DatabaseAPI, block_root: Root, score_class: Type[BaseScore]
    ) -> BaseScore:
        try:
            encoded_score = db[SchemaV1.make_block_root_to_score_lookup_key(block_root)]
        except KeyError:
            raise BlockNotFound(
                "No block with root {0} found".format(encode_hex(block_root))
            )
        return cast(BaseScore, score_class.deserialize(encoded_score))

    def block_exists(self, block_root: Root) -> bool:
        return self._block_exists(self.db, block_root)

    @staticmethod
    def _block_exists(db: DatabaseAPI, block_root: Root) -> bool:
        validate_word(block_root, title="block root")
        return block_root in db

    def persist_block_chain(
        self,
        blocks: Iterable[BaseBeaconBlock],
        block_class: Type[BaseBeaconBlock],
        fork_choice_scorings: Iterable[BaseForkChoiceScoring],
    ) -> Tuple[Tuple[BaseBeaconBlock, ...], Tuple[BaseBeaconBlock, ...]]:
        """
        Return two iterable of blocks, the first containing the new canonical blocks,
        the second containing the old canonical headers
        """
        with self.db.atomic_batch() as db:
            try:
                first_block = next(iter(blocks))
                if first_block.is_genesis:
                    self._handle_exceptional_justification_and_finality(first_block)
            except StopIteration:
                pass
            return self._persist_block_chain(
                db, blocks, block_class, fork_choice_scorings
            )

    @staticmethod
    def _set_block_score_to_db(
        db: DatabaseAPI, block: BaseBeaconBlock, score: BaseScore
    ) -> BaseScore:
        # NOTE if we change the score serialization, we will likely need to
        # patch up the fork choice logic.
        # We will decide the score serialization is fixed for now.
        db.set(
            SchemaV1.make_block_root_to_score_lookup_key(block.message.hash_tree_root),
            score.serialize(),
        )
        return score

    def set_score(self, block: BaseBeaconBlock, score: BaseScore) -> None:
        self.__class__._set_block_score_to_db(self.db, block, score)

    @classmethod
    def _persist_block_chain(
        cls,
        db: DatabaseAPI,
        blocks: Iterable[BaseSignedBeaconBlock],
        block_class: Type[BaseSignedBeaconBlock],
        fork_choice_scorings: Iterable[BaseForkChoiceScoring],
    ) -> Tuple[Tuple[BaseSignedBeaconBlock, ...], Tuple[BaseSignedBeaconBlock, ...]]:
        blocks_iterator = iter(blocks)
        scorings_iterator = iter(fork_choice_scorings)

        try:
            first_block = first(blocks_iterator)
            first_scoring = first(scorings_iterator)
        except StopIteration:
            return tuple(), tuple()

        try:
            previous_canonical_head = cls._get_canonical_head(
                db, block_class
            ).message.hash_tree_root
            score_class = first_scoring.get_score_class()
            head_score = cls._get_score(db, previous_canonical_head, score_class)
        except CanonicalHeadNotFound:
            no_canonical_head = True
        else:
            no_canonical_head = False

        is_genesis = first_block.is_genesis
        if not is_genesis and not cls._block_exists(db, first_block.parent_root):
            raise ParentNotFound(
                "Cannot persist block ({}) with unknown parent ({})".format(
                    encode_hex(first_block.message.hash_tree_root),
                    encode_hex(first_block.parent_root),
                )
            )

        score = first_scoring.score(first_block.message)

        curr_block_head = first_block
        db.set(curr_block_head.message.hash_tree_root, ssz.encode(curr_block_head))
        cls._add_block_root_to_slot_lookup(db, curr_block_head)
        cls._set_block_score_to_db(db, curr_block_head, score)
        cls._add_attestations_root_to_block_lookup(db, curr_block_head)

        orig_blocks_seq = concat([(first_block,), blocks_iterator])

        for parent, child in sliding_window(2, orig_blocks_seq):
            if parent.message.hash_tree_root != child.parent_root:
                raise ValidationError(
                    "Non-contiguous chain. Expected {} to have {} as parent but was {}".format(
                        encode_hex(child.message.hash_tree_root),
                        encode_hex(parent.message.hash_tree_root),
                        encode_hex(child.parent_root),
                    )
                )

            curr_block_head = child
            db.set(curr_block_head.message.hash_tree_root, ssz.encode(curr_block_head))
            cls._add_block_root_to_slot_lookup(db, curr_block_head)
            cls._add_attestations_root_to_block_lookup(db, curr_block_head)

            # NOTE: len(scorings_iterator) should equal len(blocks_iterator)
            try:
                next_scoring = next(scorings_iterator)
            except StopIteration:
                raise MissingForkChoiceScorings

            score = next_scoring.score(curr_block_head.message)
            cls._set_block_score_to_db(db, curr_block_head, score)

        if no_canonical_head:
            return cls._set_as_canonical_chain_head(
                db, curr_block_head.message.hash_tree_root, block_class
            )

        if score > head_score:
            return cls._set_as_canonical_chain_head(
                db, curr_block_head.message.hash_tree_root, block_class
            )
        else:
            return tuple(), tuple()

    @classmethod
    def _set_as_canonical_chain_head(
        cls, db: DatabaseAPI, block_root: Root, block_class: Type[BaseBeaconBlock]
    ) -> Tuple[Tuple[BaseBeaconBlock, ...], Tuple[BaseBeaconBlock, ...]]:
        """
        Set the canonical chain HEAD to the block as specified by the
        given block root.

        :return: a tuple of the blocks that are newly in the canonical chain, and the blocks that
            are no longer in the canonical chain
        """
        try:
            block = cls._get_block_by_root(db, block_root, block_class)
        except BlockNotFound:
            raise ValueError(
                "Cannot use unknown block root as canonical head: {block_root.hex()}"
            )

        new_canonical_blocks = tuple(
            reversed(cls._find_new_ancestors(db, block, block_class))
        )
        old_canonical_blocks = []

        for block in new_canonical_blocks:
            try:
                old_canonical_root = cls._get_canonical_block_root(db, block.slot)
            except BlockNotFound:
                # no old_canonical block, and no more possible
                break
            else:
                old_canonical_block = cls._get_block_by_root(
                    db, old_canonical_root, block_class
                )
                old_canonical_blocks.append(old_canonical_block)

        for block in new_canonical_blocks:
            cls._add_block_slot_to_root_lookup(db, block)

        db.set(
            SchemaV1.make_canonical_head_root_lookup_key(), block.message.hash_tree_root
        )

        return new_canonical_blocks, tuple(old_canonical_blocks)

    @classmethod
    @to_tuple
    def _find_new_ancestors(
        cls, db: DatabaseAPI, block: BaseBeaconBlock, block_class: Type[BaseBeaconBlock]
    ) -> Iterable[BaseBeaconBlock]:
        """
        Return the chain leading up from the given block until (but not including)
        the first ancestor it has in common with our canonical chain.

        If D is the canonical head in the following chain, and F is the new block,
        then this function returns (F, E).

        A - B - C - D
               \
                E - F
        """
        while True:
            try:
                orig = cls._get_canonical_block_by_slot(db, block.slot, block_class)
            except BlockNotFound:
                # This just means the block is not on the canonical chain.
                pass
            else:
                if orig.message.hash_tree_root == block.message.hash_tree_root:
                    # Found the common ancestor, stop.
                    break

            # Found a new ancestor
            yield block

            if block.is_genesis:
                break
            else:
                block = cls._get_block_by_root(db, block.parent_root, block_class)

    @staticmethod
    def _add_block_slot_to_root_lookup(db: DatabaseAPI, block: BaseBeaconBlock) -> None:
        """
        Set a record in the database to allow looking up this block by its
        block slot.
        """
        block_slot_to_root_key = SchemaV1.make_block_slot_to_root_lookup_key(block.slot)
        db.set(block_slot_to_root_key, block.message.hash_tree_root)

    @staticmethod
    def _add_block_root_to_slot_lookup(db: DatabaseAPI, block: BaseBeaconBlock) -> None:
        """
        Set a record in the database to allow looking up the slot number by its
        block root.
        """
        block_root_to_slot_key = SchemaV1.make_block_root_to_slot_lookup_key(
            block.message.hash_tree_root
        )
        db.set(block_root_to_slot_key, ssz.encode(block.slot, sedes=ssz.sedes.uint64))

    #
    # Beacon State API
    #
    def _add_head_state_slot_lookup(self, slot: Slot) -> None:
        """
        Write head state slot into the database.
        """
        self.db.set(
            SchemaV1.make_head_state_slot_lookup_key(),
            ssz.encode(slot, sedes=ssz.sedes.uint64),
        )

    def _add_head_state_root_lookup(self, root: Hash32) -> None:
        """
        Write head state root into the database.
        """
        self.db.set(SchemaV1.make_head_state_root_lookup_key(), root)

    def update_head_state(self, slot: Slot, root: Hash32) -> None:
        """
        Write head state slot and head state root into the database.
        """
        self._add_head_state_slot_lookup(slot)
        self._add_head_state_root_lookup(root)

    def get_head_state_slot(self) -> Slot:
        return self._get_head_state_slot(self.db)

    @staticmethod
    def _get_head_state_slot(db: DatabaseAPI) -> Slot:
        try:
            encoded_head_state_slot = db[SchemaV1.make_head_state_slot_lookup_key()]
            head_state_slot = ssz.decode(
                encoded_head_state_slot, sedes=ssz.sedes.uint64
            )
        except KeyError:
            raise HeadStateSlotNotFound("No head state slot found")
        return head_state_slot

    def get_head_state_root(self) -> Hash32:
        return self._get_head_state_root(self.db)

    @staticmethod
    def _get_head_state_root(db: DatabaseAPI) -> Hash32:
        try:
            head_state_root = db[SchemaV1.make_head_state_root_lookup_key()]
        except KeyError:
            raise HeadStateSlotNotFound("No head state slot found")
        return Hash32(head_state_root)

    def get_state_by_root(
        self, state_root: Hash32, state_class: Type[BeaconState]
    ) -> BeaconState:
        return self._get_state_by_root(self.db, state_root, state_class)

    @staticmethod
    def _get_state_by_root(
        db: DatabaseAPI, state_root: Hash32, state_class: Type[BeaconState]
    ) -> BeaconState:
        """
        Return the requested beacon state as specified by state hash.

        Raises StateNotFound if it is not present in the db.
        """
        # TODO: validate_state_root
        if state_root in state_cache and state_root in db:
            return state_cache[state_root]

        try:
            state_ssz = db[state_root]
        except KeyError:
            raise StateNotFound(f"No state with root {encode_hex(state_root)} found")

        state = ssz.decode(state_ssz, state_class)
        state_cache[state] = state
        return state

    def persist_state(self, state: BeaconState) -> None:
        """
        Persist the given BeaconState.

        This includes the finality data contained in the BeaconState.
        """
        return self._persist_state(state)

    def _persist_state(self, state: BeaconState) -> None:
        self.db.set(state.hash_tree_root, ssz.encode(state))

        self._persist_finalized_head(state)
        self._persist_justified_head(state)

        # Check if head state info is set.
        try:
            self.get_head_state_root()
        except HeadStateSlotNotFound:
            # Hasn't store any head state slot yet.
            self.update_head_state(state.slot, state.hash_tree_root)

        # For metrics
        # TODO: only persist per epoch transition
        self._persist_canonical_epoch_info(self.db, state)

    def _update_finalized_head(self, finalized_root: Root) -> None:
        """
        Unconditionally write the ``finalized_root`` as the root of the currently
        finalized block.
        """
        self.db.set(SchemaV1.make_finalized_head_root_lookup_key(), finalized_root)
        self._finalized_root = finalized_root

    def _persist_finalized_head(self, state: BeaconState) -> None:
        """
        If there is a new ``state.finalized_root``, then we can update it in the DB.
        This policy is safe because a large number of validators on the network
        will have violated a slashing condition if the invariant does not hold.
        """
        if state.finalized_checkpoint.root == ZERO_ROOT:
            # ignore finality in the genesis state
            return

        if state.finalized_checkpoint.root != self._finalized_root:
            self._update_finalized_head(state.finalized_checkpoint.root)

    def _update_justified_head(self, justified_root: Root, epoch: Epoch) -> None:
        """
        Unconditionally write the ``justified_root`` as the root of the highest
        justified block.
        """
        self.db.set(SchemaV1.make_justified_head_root_lookup_key(), justified_root)
        self._highest_justified_epoch = epoch

    def _find_updated_justified_root(
        self, state: BeaconState
    ) -> Optional[Tuple[Root, Epoch]]:
        """
        Find the highest epoch that has been justified so far.

        If:
        (i) we find one higher than the epoch of the current justified head
        and
        (ii) it has been justified for more than one epoch,

        then return that (root, epoch) pair.
        """
        if state.current_justified_checkpoint.epoch > self._highest_justified_epoch:
            checkpoint = state.current_justified_checkpoint
            return (checkpoint.root, checkpoint.epoch)
        elif state.previous_justified_checkpoint.epoch > self._highest_justified_epoch:
            checkpoint = state.previous_justified_checkpoint
            return (checkpoint.root, checkpoint.epoch)
        return None

    def _persist_justified_head(self, state: BeaconState) -> None:
        """
        If there is a new justified root that has been justified for at least one
        epoch _and_ the justification is for a higher epoch than we have previously
        seen, go ahead and update the justified head.
        """
        result = self._find_updated_justified_root(state)

        if result:
            self._update_justified_head(*result)

    def _handle_exceptional_justification_and_finality(
        self, genesis_block: BaseBeaconBlock
    ) -> None:
        """
        The genesis ``BeaconState`` lacks the correct justification and finality
        data in the early epochs. The invariants of this class require an exceptional
        handling to mark the genesis block's root and the genesis epoch as
        finalized and justified.
        """
        genesis_root = genesis_block.message.hash_tree_root
        self._update_finalized_head(genesis_root)
        self._update_justified_head(genesis_root, self.genesis_config.GENESIS_EPOCH)

    @staticmethod
    def _persist_canonical_epoch_info(db: DatabaseAPI, state: BeaconState) -> None:
        epoch_info = EpochInfo(
            previous_justified_checkpoint=state.previous_justified_checkpoint,
            current_justified_checkpoint=state.current_justified_checkpoint,
            finalized_checkpoint=state.finalized_checkpoint,
        )
        db.set(SchemaV1.make_canonical_epoch_info_lookup_key(), ssz.encode(epoch_info))

    def get_canonical_epoch_info(self) -> EpochInfo:
        return self._get_canonical_epoch_info(self.db)

    @staticmethod
    def _get_canonical_epoch_info(db: DatabaseAPI) -> EpochInfo:
        key = SchemaV1.make_canonical_epoch_info_lookup_key()
        try:
            epoch_info = db[key]
        except KeyError:
            raise EpochInfoNotFound("Canonical EpochInfo not found")
        else:
            return ssz.decode(epoch_info, EpochInfo)

    #
    # Attestation API
    #

    @staticmethod
    def _add_attestations_root_to_block_lookup(
        db: DatabaseAPI, block: BaseBeaconBlock
    ) -> None:
        root = block.message.hash_tree_root
        for index, attestation in enumerate(block.body.attestations):
            attestation_key = AttestationKey(root, index)
            db.set(
                SchemaV1.make_attestation_root_to_block_lookup_key(
                    attestation.hash_tree_root
                ),
                ssz.encode(attestation_key),
            )

    def get_attestation_key_by_root(self, attestation_root: Root) -> Tuple[Root, int]:
        return self._get_attestation_key_by_root(self.db, attestation_root)

    @staticmethod
    def _get_attestation_key_by_root(
        db: DatabaseAPI, attestation_root: Root
    ) -> Tuple[Root, int]:
        try:
            encoded_key = db[
                SchemaV1.make_attestation_root_to_block_lookup_key(attestation_root)
            ]
        except KeyError:
            raise AttestationRootNotFound(
                "Attestation root {0} not found".format(encode_hex(attestation_root))
            )
        attestation_key = ssz.decode(encoded_key, sedes=AttestationKey)
        return attestation_key.block_root, attestation_key.index

    def attestation_exists(self, attestation_root: Root) -> bool:
        lookup_key = SchemaV1.make_attestation_root_to_block_lookup_key(
            attestation_root
        )
        return self.exists(lookup_key)

    #
    # Fork choice API
    #
    def get_fork_choice_context_data_for(self, fork: str) -> bytes:
        # TODO implement
        raise MissingForkChoiceContext(f"Missing context for fork `{fork}`")

    def persist_fork_choice_context(self, serialized_context: bytes, fork: str) -> None:
        # TODO implement
        pass

    #
    # Raw Database API
    #
    def exists(self, key: bytes) -> bool:
        """
        Return True if the given key exists in the database.
        """
        return self.db.exists(key)

    def get(self, key: bytes) -> bytes:
        """
        Return the value for the given key or a KeyError if it doesn't exist in the database.
        """
        return self.db[key]
