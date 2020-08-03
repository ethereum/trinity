from typing import Iterable, Optional, Sequence, Tuple, Type

from eth.abc import AtomicDatabaseAPI
from eth.exceptions import BlockNotFound
from eth.typing import Hash32
from eth_utils import to_tuple
from lru import LRU
import ssz
from ssz.hashable_list import HashableList
from ssz.hashable_vector import HashableVector
from ssz.sedes import Bitvector

from eth2.beacon.constants import JUSTIFICATION_BITS_LENGTH
from eth2.beacon.db.abc import BaseBeaconChainDB
import eth2.beacon.db.schema2 as SchemaV1
from eth2.beacon.genesis import get_genesis_block
from eth2.beacon.helpers import compute_epoch_at_slot
from eth2.beacon.types.block_headers import BeaconBlockHeader
from eth2.beacon.types.blocks import BaseBeaconBlock, BaseSignedBeaconBlock
from eth2.beacon.types.checkpoints import Checkpoint
from eth2.beacon.types.eth1_data import Eth1Data
from eth2.beacon.types.forks import Fork
from eth2.beacon.types.pending_attestations import PendingAttestation
from eth2.beacon.types.states import BeaconState
from eth2.beacon.types.validators import Validator
from eth2.beacon.typing import (
    Bitfield,
    BLSSignature,
    Epoch,
    Gwei,
    Root,
    Slot,
    Timestamp,
    default_root,
    default_timestamp,
)
from eth2.configs import Eth2Config


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
        self._state_bytes_written = 0

    @classmethod
    def from_genesis(
        cls,
        db: AtomicDatabaseAPI,
        genesis_state: BeaconState,
        signed_block_class: Type[BaseSignedBeaconBlock],
        config: Eth2Config,
    ) -> "BeaconChainDB":
        chain_db = cls(db)

        genesis_block = get_genesis_block(
            genesis_state.hash_tree_root, signed_block_class.block_class
        )

        chain_db.persist_block(signed_block_class.create(message=genesis_block))
        chain_db._persist_genesis_data(genesis_state)
        chain_db.persist_state(genesis_state, config)

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
        self, slot: Slot, config: Eth2Config
    ) -> Optional[BeaconState]:
        key = SchemaV1.slot_to_state_root(slot)
        try:
            root = Root(Hash32(self.db[key]))
        except KeyError:
            return None
        return self.get_state_by_root(root, config)

    def get_state_by_root(self, state_root: Root, config: Eth2Config) -> BeaconState:
        if state_root in self._state_cache:
            return self._state_cache[state_root]

        return self._read_state(state_root, config)

    def _write_state_slot(self, state_root: Root, slot: Slot) -> None:
        state_to_slot = SchemaV1.state_root_to_slot(state_root)
        slot_to_state = SchemaV1.slot_to_state_root(slot)
        encoding = ssz.encode(slot, ssz.uint64)
        self._state_bytes_written += len(encoding)
        self.db[state_to_slot] = encoding
        self.db[slot_to_state] = state_root

    def _write_state_fork(self, state_root: Root, fork: Fork) -> None:
        fork_root = fork.hash_tree_root
        if fork_root not in self.db:
            encoding = ssz.encode(fork)
            self._state_bytes_written += len(encoding)
            self.db[fork_root] = encoding

        key = SchemaV1.state_root_to_fork_root(state_root)
        self.db[key] = fork_root

    def _write_state_block_header(
        self, state_root: Root, block_header: BeaconBlockHeader
    ) -> None:
        # NOTE: can further optimize by filling state_root into block_header
        # and skipping encoding if block is present (it likely will be...)
        block_header_root = block_header.hash_tree_root
        if block_header_root not in self.db:
            encoding = ssz.encode(block_header)
            self._state_bytes_written += len(encoding)
            self.db[block_header_root] = encoding

        state_root_to_block_header_root = SchemaV1.state_root_to_block_header_root(
            state_root
        )
        slot_to_block_header_root = SchemaV1.slot_to_block_header_root(
            block_header.slot
        )
        self.db[state_root_to_block_header_root] = block_header_root
        self.db[slot_to_block_header_root] = block_header_root

    def _write_state_historical_roots(
        self, state_root: Root, historical_roots: HashableList[Root]
    ) -> None:
        root = historical_roots.hash_tree_root
        if root not in self.db:
            encoding = ssz.encode(historical_roots)
            self._state_bytes_written += len(encoding)
            self.db[root] = encoding

        key = SchemaV1.state_root_to_historical_roots_root(state_root)
        self.db[key] = root

    def _write_state_eth1_data(self, state_root: Root, eth1_data: Eth1Data) -> None:
        eth1_data_root = eth1_data.hash_tree_root
        if eth1_data_root not in self.db:
            encoding = ssz.encode(eth1_data)
            self._state_bytes_written += len(encoding)
            self.db[eth1_data_root] = encoding

        key = SchemaV1.state_root_to_eth1_data_root(state_root)
        self.db[key] = eth1_data_root

    def _write_state_eth1_data_votes(
        self, state_root: Root, eth1_data_votes: Sequence[Eth1Data]
    ) -> None:
        roots = bytearray()
        for vote in eth1_data_votes:
            root = vote.hash_tree_root
            if root not in self.db:
                encoding = ssz.encode(vote)
                self._state_bytes_written += len(encoding)
                self.db[root] = encoding
            roots.extend(root)

        # TODO optimize further w/ list diffs?
        key = SchemaV1.state_root_to_eth1_data_votes(state_root)
        self.db[key] = roots

    def _write_state_eth1_deposit_index(self, state_root: Root, index: int) -> None:
        key = SchemaV1.state_root_to_eth1_deposit_index(state_root)
        encoding = ssz.encode(index, ssz.uint64)
        self._state_bytes_written += len(encoding)
        self.db[key] = encoding

    def _write_state_validators(
        self, state_root: Root, validators: HashableList[Validator]
    ) -> None:
        """
        Given the size of the validator set and the frequency with which it is expected to change
        (some but not as frequent, as say, the balances), we want to keep the root of the
        list of the validators and skip any work we can if no fact about the set has changed.
        """
        validators_root = validators.hash_tree_root
        if validators_root not in self.db:
            roots = bytearray()
            for validator in validators:
                root = validator.hash_tree_root
                if root not in self.db:
                    encoding = ssz.encode(validator)
                    self._state_bytes_written += len(encoding)
                    self.db[root] = encoding
                roots.extend(root)
            key = SchemaV1.validators_root_to_roots_of_validators(validators_root)
            self.db[key] = roots

        # TODO optimize further w/ list diffs?
        key = SchemaV1.state_root_to_validators_root(state_root)
        self._state_bytes_written += len(validators_root)
        self.db[key] = validators_root

    def _write_state_balances(
        self, state_root: Root, balances: HashableList[Gwei]
    ) -> None:
        """
        Balances will be changing frequently and are numerous.

        Best approach is likely a tree diff; for now, just write the entire set
        with a quick check for identity w/ the list root.
        Note there is no gain in checking if a particular balance has already
        been seen (or changed) since the last state as the main cost is
        the iteration (rather than serializing an 8-byte Gwei value).
        """
        balances_root = balances.hash_tree_root
        if balances_root not in self.db:
            key = SchemaV1.balances_root_to_balances(balances_root)
            encoding = ssz.encode(balances)
            self._state_bytes_written += len(encoding)
            self.db[key] = encoding

        key = SchemaV1.state_root_to_balances_root(state_root)
        self._state_bytes_written += len(balances_root)
        self.db[key] = balances_root

    def _write_state_randao_mix(self, slot: Slot, mix: Root) -> None:
        """
        NOTE: only the root at the current epoch can have changed.
        """
        key = SchemaV1.slot_to_randao_mix(slot)
        self.db[key] = mix

    def _write_state_slashings(
        self, state_root: Root, slashings: HashableVector[Gwei]
    ) -> None:
        """
        NOTE: we rely on low frequency of slashing to minimize bandwidth by
        only checking the slashings root as a summary of the vector.

        If there are a lot of slashings then this list will change frequently and consequently
        we will incur the encoding cost more frequently -- exactly when we do not
        want such a penalty.

        The most efficient storage of slashings is likely "tree diffing" each vector.
        """
        root = slashings.hash_tree_root
        if root not in self.db:
            encoding = ssz.encode(slashings)
            self._state_bytes_written += len(encoding)
            self.db[root] = encoding

        key = SchemaV1.state_root_to_slashings_root(state_root)
        self._state_bytes_written += len(root)
        self.db[key] = root

    def _write_state_previous_epoch_attestations(
        self, state_root: Root, attestations: HashableList[PendingAttestation]
    ) -> None:
        # TODO optimize w/ pending attestation -> indexed attestation that already exists?
        roots = bytearray()
        for attestation in attestations:
            root = attestation.hash_tree_root
            if root not in self.db:
                encoding = ssz.encode(attestation)
                self._state_bytes_written += len(encoding)
                self.db[root] = encoding
            roots.extend(root)

        # TODO optimize further w/ list diffs?
        key = SchemaV1.state_root_to_previous_epoch_attestations(state_root)
        self._state_bytes_written += len(roots)
        self.db[key] = roots

    def _write_state_current_epoch_attestations(
        self, state_root: Root, attestations: HashableList[PendingAttestation]
    ) -> None:
        # TODO optimize w/ pending attestation -> indexed attestation that already exists?
        roots = bytearray()
        for attestation in attestations:
            root = attestation.hash_tree_root
            if root not in self.db:
                encoding = ssz.encode(attestation)
                self._state_bytes_written += len(encoding)
                self.db[root] = encoding
            roots.extend(root)

        # TODO optimize further w/ list diffs?
        key = SchemaV1.state_root_to_current_epoch_attestations(state_root)
        self._state_bytes_written += len(roots)
        self.db[key] = roots

    def _write_state_justification_bits(
        self, state_root: Root, justification_bits: Bitfield
    ) -> None:
        key = SchemaV1.state_root_to_justification_bitfield(state_root)
        encoding = ssz.encode(justification_bits, Bitvector(JUSTIFICATION_BITS_LENGTH))
        self._state_bytes_written += len(encoding)
        self.db[key] = encoding

    def _write_state_previous_justified_checkpoint(
        self, state_root: Root, previous_justified_checkpoint: Checkpoint
    ) -> None:
        root = previous_justified_checkpoint.hash_tree_root
        if root not in self.db:
            encoding = ssz.encode(previous_justified_checkpoint)
            self._state_bytes_written += len(encoding)
            self.db[root] = encoding

        key = SchemaV1.state_root_to_previous_justified_checkpoint_root(state_root)
        self._state_bytes_written += len(root)
        self.db[key] = root

    def _write_state_current_justified_checkpoint(
        self, state_root: Root, current_justified_checkpoint: Checkpoint
    ) -> None:
        root = current_justified_checkpoint.hash_tree_root
        if root not in self.db:
            encoding = ssz.encode(current_justified_checkpoint)
            self._state_bytes_written += len(encoding)
            self.db[root] = encoding

        key = SchemaV1.state_root_to_current_justified_checkpoint_root(state_root)
        self._state_bytes_written += len(root)
        self.db[key] = root

    def _write_state_finalized_checkpoint(
        self, state_root: Root, finalized_checkpoint: Checkpoint
    ) -> None:
        root = finalized_checkpoint.hash_tree_root
        if root not in self.db:
            encoding = ssz.encode(finalized_checkpoint)
            self._state_bytes_written += len(encoding)
            self.db[root] = encoding

        key = SchemaV1.state_root_to_finalized_checkpoint_root(state_root)
        self._state_bytes_written += len(root)
        self.db[key] = root

    def _write_state(self, state: BeaconState, config: Eth2Config) -> None:
        """
        Each field of the state is treated as to minimize redundant encodings of
        data we likely already have in the database.
        """
        state_root = state.hash_tree_root
        current_epoch = state.current_epoch(config.SLOTS_PER_EPOCH)

        self._write_state_slot(state_root, state.slot)
        self._write_state_fork(state_root, state.fork)
        self._write_state_block_header(state_root, state.latest_block_header)
        self._write_state_historical_roots(state_root, state.historical_roots)
        self._write_state_eth1_data(state_root, state.eth1_data)
        self._write_state_eth1_data_votes(state_root, state.eth1_data_votes)
        self._write_state_eth1_deposit_index(state_root, state.eth1_deposit_index)
        self._write_state_validators(state_root, state.validators)
        self._write_state_balances(state_root, state.balances)
        self._write_state_randao_mix(
            state.slot,
            state.randao_mixes[current_epoch % config.EPOCHS_PER_HISTORICAL_VECTOR],
        )
        self._write_state_slashings(state_root, state.slashings)
        self._write_state_previous_epoch_attestations(
            state_root, state.previous_epoch_attestations
        )
        self._write_state_current_epoch_attestations(
            state_root, state.current_epoch_attestations
        )
        self._write_state_justification_bits(state_root, state.justification_bits)
        self._write_state_previous_justified_checkpoint(
            state_root, state.previous_justified_checkpoint
        )
        self._write_state_current_justified_checkpoint(
            state_root, state.current_justified_checkpoint
        )
        self._write_state_finalized_checkpoint(state_root, state.finalized_checkpoint)

    def _read_state_slot(self, state_root: Root) -> Slot:
        key = SchemaV1.state_root_to_slot(state_root)
        return Slot(ssz.decode(self.db[key], ssz.uint64))

    def _read_state_fork(self, state_root: Root) -> Fork:
        key = SchemaV1.state_root_to_fork_root(state_root)
        fork_root = self.db[key]
        return ssz.decode(self.db[fork_root], Fork)

    def _read_state_block_header(self, state_root: Root) -> BeaconBlockHeader:
        key = SchemaV1.state_root_to_block_header_root(state_root)
        block_header_root = self.db[key]
        return ssz.decode(self.db[block_header_root], BeaconBlockHeader)

    @to_tuple
    def _read_state_block_roots(
        self, state_root: Root, SLOTS_PER_HISTORICAL_ROOT: int
    ) -> Iterable[Root]:
        """
        Reconstructs ``state.block_roots`` at a given state root.

        For the sake of efficiency, the ``block_roots`` vector is not stored in
        the database. Instead, we reconstruct what the vector would have been based on the
        the block root stored at each slot within the range of ``SLOTS_PER_HISTORIC_ROOT``.
        """
        state_slot = self._read_state_slot(state_root)
        slots = [
            Slot(n) for n in range(state_slot - SLOTS_PER_HISTORICAL_ROOT, state_slot)
        ]
        offset = SLOTS_PER_HISTORICAL_ROOT - slots[0] % SLOTS_PER_HISTORICAL_ROOT
        slots = slots[offset:] + slots[:offset]
        for slot in slots:
            if slot < 0:
                yield default_root
            else:
                key = SchemaV1.slot_to_block_header_root(slot)
                yield Root(Hash32(self.db[key]))

    @to_tuple
    def _read_state_state_roots(
        self, state_root: Root, SLOTS_PER_HISTORICAL_ROOT: int
    ) -> Iterable[Root]:
        """
        Reconstructs ``state.state_roots`` at a given state root.

        For the sake of efficiency, the ``state_roots`` vector is not stored in
        the database. Instead, we reconstruct what the vector would have been based on the
        the state root stored at each slot within the range of ``SLOTS_PER_HISTORIC_ROOT``.
        """
        state_slot = self._read_state_slot(state_root)

        # create a list of slots corresponding to each root in ``state.state_roots``
        slots = [
            Slot(n) for n in range(state_slot - SLOTS_PER_HISTORICAL_ROOT, state_slot)
        ]
        offset = SLOTS_PER_HISTORICAL_ROOT - slots[0] % SLOTS_PER_HISTORICAL_ROOT
        slots = slots[offset:] + slots[:offset]

        for slot in slots:
            if slot < 0:
                yield default_root
            else:
                key = SchemaV1.slot_to_state_root(slot)
                yield Root(Hash32(self.db[key]))

    def _read_state_historical_roots(
        self, state_root: Root, HISTORICAL_ROOTS_LIMIT: int
    ) -> Tuple[Root]:
        key = SchemaV1.state_root_to_historical_roots_root(state_root)
        historical_roots_root = self.db[key]
        return ssz.decode(
            self.db[historical_roots_root],
            ssz.List(ssz.bytes32, HISTORICAL_ROOTS_LIMIT),
        )

    def _read_state_eth1_data(self, state_root: Root) -> Eth1Data:
        key = SchemaV1.state_root_to_eth1_data_root(state_root)
        eth1_data_root = self.db[key]
        return ssz.decode(self.db[eth1_data_root], Eth1Data)

    @to_tuple
    def _read_state_eth1_data_votes(self, state_root: Root) -> Iterable[Eth1Data]:
        key = SchemaV1.state_root_to_eth1_data_votes(state_root)
        roots = _bytes_to_roots(self.db[key])

        for root in roots:
            yield ssz.decode(self.db[bytes(root)], Eth1Data)

    def _read_state_eth1_deposit_index(self, state_root: Root) -> int:
        key = SchemaV1.state_root_to_eth1_deposit_index(state_root)
        return ssz.decode(self.db[key], ssz.uint64)

    @to_tuple
    def _read_state_validators(self, state_root: Root) -> Iterable[Validator]:
        state_root_to_validators_root = SchemaV1.state_root_to_validators_root(
            state_root
        )
        validators_root = Root(Hash32(self.db[state_root_to_validators_root]))
        validators_root_to_roots_of_validators = SchemaV1.validators_root_to_roots_of_validators(
            validators_root
        )
        roots = _bytes_to_roots(self.db[validators_root_to_roots_of_validators])

        for root in roots:
            yield ssz.decode(self.db[bytes(root)], Validator)

    def _read_state_balances(
        self, state_root: Root, VALIDATOR_REGISTRY_LIMIT: int
    ) -> Tuple[Gwei]:
        state_root_to_balances_root = SchemaV1.state_root_to_balances_root(state_root)
        balances_root = Root(Hash32(self.db[state_root_to_balances_root]))
        balances_root_to_balances = SchemaV1.balances_root_to_balances(balances_root)
        return ssz.decode(
            self.db[balances_root_to_balances],
            ssz.List(ssz.uint64, VALIDATOR_REGISTRY_LIMIT),
        )

    @to_tuple
    def _read_state_randao_mixes(
        self,
        state_root: Root,
        genesis_eth1_block_hash: Root,
        EPOCHS_PER_HISTORICAL_VECTOR: int,
        SLOTS_PER_EPOCH: int,
    ) -> Iterable[Root]:
        """
        Reconstructs the ``randa_mixes`` at a given state root.

        Everytime a block is processed, the ``randao_mixes`` vector is updated with a new
        mix at the current epoch...

        ``state.randao_mixes[state.epoch % EPOCHS_PER_HISTORICAL_ROOT] = mix``

        To reconstruct the mixes of an arbitrary state, we must find the mix value for the
        state's slot and the mix value for the last slot in each epoch within range, then
        we return a tuple that is properly offset.
        """
        state_slot = self._read_state_slot(state_root)
        state_epoch = compute_epoch_at_slot(state_slot, SLOTS_PER_EPOCH)

        # create a list of epochs that corresponds to each mix in ``state.randao_mixes``
        epochs = [
            Epoch(n)
            for n in range(
                state_epoch - EPOCHS_PER_HISTORICAL_VECTOR + 1, state_epoch + 1
            )
        ]
        offset = EPOCHS_PER_HISTORICAL_VECTOR - epochs[0] % EPOCHS_PER_HISTORICAL_VECTOR
        epochs = epochs[offset:] + epochs[:offset]

        for epoch in epochs:
            if epoch < 0:
                yield genesis_eth1_block_hash
            elif epoch == state_epoch:
                # yield the randao mix at the particular slot
                key = SchemaV1.slot_to_randao_mix(state_slot)
                yield Root(Hash32(self.db[key]))
            else:
                # yield the randao mix at the last slot in the epoch
                key = SchemaV1.slot_to_randao_mix(
                    Slot((epoch + 1) * SLOTS_PER_EPOCH - 1)
                )
                yield Root(Hash32(self.db[key]))

    def _read_state_slashings(
        self, state_root: Root, EPOCHS_PER_SLASHING_VECTOR: int
    ) -> Tuple[Gwei]:
        key = SchemaV1.state_root_to_slashings_root(state_root)
        root = self.db[key]
        return ssz.decode(
            self.db[root], ssz.Vector(ssz.uint64, EPOCHS_PER_SLASHING_VECTOR)
        )

    @to_tuple
    def _read_state_previous_epoch_attestations(
        self, state_root: Root
    ) -> Iterable[PendingAttestation]:
        key = SchemaV1.state_root_to_previous_epoch_attestations(state_root)
        roots = _bytes_to_roots(self.db[key])

        for root in roots:
            yield ssz.decode(self.db[bytes(root)], PendingAttestation)

    @to_tuple
    def _read_state_current_epoch_attestations(
        self, state_root: Root
    ) -> Iterable[PendingAttestation]:
        key = SchemaV1.state_root_to_current_epoch_attestations(state_root)
        roots = _bytes_to_roots(self.db[key])

        for root in roots:
            yield ssz.decode(self.db[bytes(root)], PendingAttestation)

    def _read_state_justification_bits(self, state_root: Root) -> Bitfield:
        key = SchemaV1.state_root_to_justification_bitfield(state_root)
        return ssz.decode(self.db[key], Bitvector(JUSTIFICATION_BITS_LENGTH))

    def _read_state_previous_justified_checkpoint(self, state_root: Root) -> Checkpoint:
        key = SchemaV1.state_root_to_previous_justified_checkpoint_root(state_root)
        root = self.db[key]
        return ssz.decode(self.db[root], Checkpoint)

    def _read_state_current_justified_checkpoint(self, state_root: Root) -> Checkpoint:
        key = SchemaV1.state_root_to_current_justified_checkpoint_root(state_root)
        root = self.db[key]
        return ssz.decode(self.db[root], Checkpoint)

    def _read_state_finalized_checkpoint(self, state_root: Root) -> Checkpoint:
        key = SchemaV1.state_root_to_finalized_checkpoint_root(state_root)
        root = self.db[key]
        return ssz.decode(self.db[root], Checkpoint)

    def _read_state(self, state_root: Root, config: Eth2Config) -> BeaconState:
        (
            genesis_time,
            genesis_validators_root,
            genesis_eth1_data_hash,
        ) = self._get_genesis_data()

        return BeaconState.create(
            slot=self._read_state_slot(state_root),
            fork=self._read_state_fork(state_root),
            latest_block_header=self._read_state_block_header(state_root),
            block_roots=self._read_state_block_roots(
                state_root, config.SLOTS_PER_HISTORICAL_ROOT
            ),
            state_roots=self._read_state_state_roots(
                state_root, config.SLOTS_PER_HISTORICAL_ROOT
            ),
            historical_roots=self._read_state_historical_roots(
                state_root, config.HISTORICAL_ROOTS_LIMIT
            ),
            eth1_data=self._read_state_eth1_data(state_root),
            eth1_data_votes=self._read_state_eth1_data_votes(state_root),
            eth1_deposit_index=self._read_state_eth1_deposit_index(state_root),
            validators=self._read_state_validators(state_root),
            balances=self._read_state_balances(
                state_root, config.VALIDATOR_REGISTRY_LIMIT
            ),
            randao_mixes=self._read_state_randao_mixes(
                state_root,
                genesis_eth1_data_hash,
                config.EPOCHS_PER_HISTORICAL_VECTOR,
                config.SLOTS_PER_EPOCH,
            ),
            slashings=self._read_state_slashings(
                state_root, config.EPOCHS_PER_SLASHINGS_VECTOR
            ),
            previous_epoch_attestations=self._read_state_previous_epoch_attestations(
                state_root
            ),
            current_epoch_attestations=self._read_state_current_epoch_attestations(
                state_root
            ),
            justification_bits=self._read_state_justification_bits(state_root),
            previous_justified_checkpoint=self._read_state_previous_justified_checkpoint(
                state_root
            ),
            current_justified_checkpoint=self._read_state_current_justified_checkpoint(
                state_root
            ),
            finalized_checkpoint=self._read_state_finalized_checkpoint(state_root),
            genesis_time=genesis_time,
            genesis_validators_root=genesis_validators_root,
            config=config,
        )

    def persist_state(self, state: BeaconState, config: Eth2Config) -> None:
        state_root = state.hash_tree_root
        self._state_cache[state_root] = state

        self._write_state(state, config)

    def _get_genesis_data(self) -> Tuple[Timestamp, Root, Root]:
        key = SchemaV1.genesis_data()
        try:
            data = self.db[key]
        except KeyError:
            return default_timestamp, default_root, default_root
        genesis_time = ssz.decode(data[:8], ssz.sedes.uint64)
        genesis_validators_root = ssz.decode(data[8:40], ssz.sedes.bytes32)
        genesis_eth1_block_hash = ssz.decode(data[40:], ssz.sedes.bytes32)
        return (
            Timestamp(genesis_time),
            Root(genesis_validators_root),
            Root(genesis_eth1_block_hash),
        )

    def _persist_genesis_data(self, genesis_state: BeaconState) -> None:
        """
        Store data in the database that will never change.
        """
        genesis_time = ssz.encode(genesis_state.genesis_time, ssz.uint64)
        genesis_validators_root = ssz.encode(
            genesis_state.genesis_validators_root, ssz.bytes32
        )
        genesis_eth1_block_hash = genesis_state.eth1_data.block_hash
        key = SchemaV1.genesis_data()
        self.db[key] = genesis_time + genesis_validators_root + genesis_eth1_block_hash


@to_tuple
def _bytes_to_roots(roots_bytes: bytes) -> Iterable[Root]:
    bytes_len = len(roots_bytes)
    if bytes_len % 32 != 0:
        raise ValueError("bytes not divisible by 32")

    for i in range(0, int(bytes_len / 32)):
        start = i * 32
        end = start + 32
        yield Root(Hash32(roots_bytes[start:end]))
