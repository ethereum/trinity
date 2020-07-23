from typing import Optional, Type

from eth.abc import AtomicDatabaseAPI
from eth.exceptions import BlockNotFound
from eth.typing import Hash32
from lru import LRU
import ssz

from eth2.beacon.db.abc import BaseBeaconChainDB
import eth2.beacon.db.schema2 as SchemaV1
from eth2.beacon.genesis import get_genesis_block
from eth2.beacon.types.blocks import BaseBeaconBlock, BaseSignedBeaconBlock
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import BLSSignature, Root, Slot


class StateNotFound(Exception):
    pass


# two epochs of blocks
BLOCK_CACHE_SIZE = 64
# two epochs of states
STATE_CACHE_SIZE = 64


class BeaconChainDB(BaseBeaconChainDB):
    def __init__(self, db: AtomicDatabaseAPI) -> None:
        self.db = db
        self._block_cache = LRU(BLOCK_CACHE_SIZE)
        self._state_cache = LRU(STATE_CACHE_SIZE)

    @classmethod
    def from_genesis(
        cls,
        db: AtomicDatabaseAPI,
        genesis_state: BeaconState,
        signed_block_class: Type[BaseSignedBeaconBlock],
    ) -> "BeaconChainDB":
        chain_db = cls(db)

        genesis_block = get_genesis_block(
            genesis_state.hash_tree_root, signed_block_class.block_class
        )

        chain_db.persist_block(signed_block_class.create(message=genesis_block))
        chain_db.persist_state(genesis_state)

        chain_db.mark_canonical_block(genesis_block)
        chain_db.mark_finalized_head(genesis_block)

        return chain_db

    def get_block_by_slot(
        self, slot: Slot, block_class: Type[BaseBeaconBlock]
    ) -> Optional[BaseBeaconBlock]:
        key = SchemaV1.slot_to_block_root(slot)
        try:
            root = Root(Hash32(self.db[key]))
        except KeyError:
            return None
        return self.get_block_by_root(root, block_class)

    def get_block_by_root(
        self, block_root: Root, block_class: Type[BaseBeaconBlock]
    ) -> BaseBeaconBlock:
        if block_root in self._block_cache:
            return self._block_cache[block_root]

        key = SchemaV1.block_root_to_block(block_root)
        try:
            block_data = self.db[key]
        except KeyError:
            raise BlockNotFound()
        return ssz.decode(block_data, block_class)

    def get_block_signature_by_root(self, block_root: Root) -> BLSSignature:
        """
        ``block_root`` is the hash tree root of a beacon block.
        This method provides a way to reconstruct the ``SignedBeaconBlock`` if required.
        """
        key = SchemaV1.block_root_to_signature(block_root)
        try:
            return BLSSignature(self.db[key])
        except KeyError:
            raise BlockNotFound()

    def persist_block(self, signed_block: BaseSignedBeaconBlock) -> None:
        block = signed_block.message
        block_root = block.hash_tree_root

        self._block_cache[block_root] = block

        block_root_to_block = SchemaV1.block_root_to_block(block_root)
        self.db[block_root_to_block] = ssz.encode(block)

        signature = signed_block.signature
        block_root_to_signature = SchemaV1.block_root_to_signature(block_root)
        self.db[block_root_to_signature] = signature

    def mark_canonical_block(self, block: BaseBeaconBlock) -> None:
        slot_to_block_root = SchemaV1.slot_to_block_root(block.slot)
        self.db[slot_to_block_root] = block.hash_tree_root

        slot_to_state_root = SchemaV1.slot_to_state_root(block.slot)
        self.db[slot_to_state_root] = block.state_root

    def mark_finalized_head(self, block: BaseBeaconBlock) -> None:
        finalized_head_root = SchemaV1.finalized_head_root()
        self.db[finalized_head_root] = block.hash_tree_root

    def get_finalized_head(self, block_class: Type[BaseBeaconBlock]) -> BaseBeaconBlock:
        finalized_head_root_key = SchemaV1.finalized_head_root()
        finalized_head_root = Root(Hash32(self.db[finalized_head_root_key]))
        return self.get_block_by_root(finalized_head_root, block_class)

    def get_state_by_slot(
        self, slot: Slot, state_class: Type[BeaconState]
    ) -> Optional[BeaconState]:
        key = SchemaV1.slot_to_state_root(slot)
        try:
            root = Root(Hash32(self.db[key]))
        except KeyError:
            return None
        return self.get_state_by_root(root, state_class)

    def get_state_by_root(
        self, state_root: Root, state_class: Type[BeaconState]
    ) -> BeaconState:
        if state_root in self._state_cache:
            return self._state_cache[state_root]

        key = SchemaV1.state_root_to_state(state_root)
        try:
            state_data = self.db[key]
        except KeyError:
            raise StateNotFound()
        return ssz.decode(state_data, state_class)

    def persist_state(self, state: BeaconState) -> None:
        state_root = state.hash_tree_root

        self._state_cache[state_root] = state

        state_root_to_state = SchemaV1.state_root_to_state(state_root)
        self.db[state_root_to_state] = ssz.encode(state)
