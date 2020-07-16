from typing import Iterator, List as PyList, NamedTuple

from hashlib import sha256

import milagro_bls_binding as milagro_bls

from eth2spec.phase0.spec import *


apply_constants_config(globals())

def bls_Verify(PK, message, signature):
    try:
        result = milagro_bls.Verify(PK, message, signature)
    except Exception:
        result = False
    finally:
        return result


def bls_FastAggregateVerify(pubkeys, message, signature):
    try:
        result = milagro_bls.FastAggregateVerify(list(pubkeys), message, signature)
    except Exception:
        result = False
    finally:
        return result


def integer_squareroot(n: uint64) -> uint64:
    """
    Return the largest integer ``x`` such that ``x**2 <= n``.
    """
    x = n
    y = (x + 1) // 2
    while y < x:
        x = y
        y = (x + n // x) // 2
    return x


def xor(bytes_1: Bytes32, bytes_2: Bytes32) -> Bytes32:
    """
    Return the exclusive-or of two 32-byte strings.
    """
    return Bytes32(a ^ b for a, b in zip(bytes_1, bytes_2))


def hash(x):
    return sha256(x).digest()


def compute_fork_data_root(current_version: Version, genesis_validators_root: Root) -> Root:
    """
    Return the 32-byte fork data root for the ``current_version`` and ``genesis_validators_root``.
    This is used primarily in signature domains to avoid collisions across forks/chains.
    """
    return hash_tree_root(ForkData(
        current_version=current_version,
        genesis_validators_root=genesis_validators_root,
    ))


# ShuffleList shuffles a list, using the given seed for randomness. Mutates the input list.
def shuffle_list(input: Sequence[ValidatorIndex], seed: Root):
    _inner_shuffle_list(input, seed, True)


# UnshuffleList undoes a list shuffling using the seed of the shuffling. Mutates the input list.
def unshuffle_list(input: Sequence[ValidatorIndex], seed: Root):
    _inner_shuffle_list(input, seed, False)


_SHUFFLE_H_SEED_SIZE = 32
_SHUFFLE_H_ROUND_SIZE = 1
_SHUFFLE_H_POSITION_WINDOW_SIZE = 4
_SHUFFLE_H_PIVOT_VIEW_SIZE = _SHUFFLE_H_SEED_SIZE + _SHUFFLE_H_ROUND_SIZE
_SHUFFLE_H_TOTAL_SIZE = _SHUFFLE_H_SEED_SIZE + _SHUFFLE_H_ROUND_SIZE + _SHUFFLE_H_POSITION_WINDOW_SIZE


# Shuffles or unshuffles, depending on the `dir` (true for shuffling, false for unshuffling
def _inner_shuffle_list(input: Sequence[ValidatorIndex], seed: Root, dir: bool):
    if len(input) <= 1:
        # nothing to (un)shuffle
        return

    listSize = len(input)
    buf = bytearray([0] * _SHUFFLE_H_TOTAL_SIZE)
    r = 0
    if not dir:
        # Start at last round.
        # Iterating through the rounds in reverse, un-swaps everything, effectively un-shuffling the list.
        r = SHUFFLE_ROUND_COUNT - 1

    # Seed is always the first 32 bytes of the hash input, we never have to change this part of the buffer.
    buf[:_SHUFFLE_H_SEED_SIZE] = seed[:]
    while True:
        # spec: pivot = bytes_to_int(hash(seed + int_to_bytes1(round))[0:8]) % list_size
        # This is the "int_to_bytes1(round)", appended to the seed.
        buf[_SHUFFLE_H_SEED_SIZE] = r
        # Seed is already in place, now just hash the correct part of the buffer, and take a uint64 from it,
        #  and modulo it to get a pivot within range.
        h = hash(buf[:_SHUFFLE_H_PIVOT_VIEW_SIZE])
        pivot = int.from_bytes(h[:8], byteorder=ENDIANNESS) % listSize

        # Split up the for-loop in two:
        #  1. Handle the part from 0 (incl) to pivot (incl). This is mirrored around (pivot / 2)
        #  2. Handle the part from pivot (excl) to N (excl). This is mirrored around ((pivot / 2) + (size/2))
        # The pivot defines a split in the array, with each of the splits mirroring their data within the split.
        # Print out some example even/odd sized index lists, with some even/odd pivots,
        #  and you can deduce how the mirroring works exactly.
        # Note that the mirror is strict enough to not consider swapping the index @mirror with itself.
        mirror = (pivot + 1) >> 1
        # Since we are iterating through the "positions" in order, we can just repeat the hash every 256th position.
        # No need to pre-compute every possible hash for efficiency like in the example code.
        # We only need it consecutively (we are going through each in reverse order however, but same thing)
        #
        # spec: source = hash(seed + int_to_bytes1(round) + int_to_bytes4(position # 256))
        # - seed is still in 0:32 (excl., 32 bytes)
        # - round number is still in 32
        # - mix in the position for randomness, except the last byte of it,
        #     which will be used later to select a bit from the resulting hash.
        # We start from the pivot position, and work back to the mirror position (of the part left to the pivot).
        # This makes us process each pear exactly once (instead of unnecessarily twice, like in the spec)
        buf[_SHUFFLE_H_PIVOT_VIEW_SIZE:] = ((pivot >> 8) & 0xffff_ffff).to_bytes(length=4, byteorder=ENDIANNESS)
        source = hash(buf)
        byteV = source[(pivot&0xff)>>3]
        i, j = 0, pivot
        while i < mirror:
            # The pair is i,j. With j being the bigger of the two, hence the "position" identifier of the pair.
            # Every 256th bit (aligned to j).
            if j&0xff == 0xff:
                # just overwrite the last part of the buffer, reuse the start (seed, round)
                buf[_SHUFFLE_H_PIVOT_VIEW_SIZE:] = ((j >> 8) & 0xffff_ffff).to_bytes(length=4, byteorder=ENDIANNESS)
                source = hash(buf)

            # Same trick with byte retrieval. Only every 8th.
            if j&0x7 == 0x7:
                byteV = source[(j&0xff)>>3]

            bitV = (byteV >> (j & 0x7)) & 0x1

            if bitV == 1:
                # swap the pair items
                input[i], input[j] = input[j], input[i]

            i, j = i+1, j-1

        # Now repeat, but for the part after the pivot.
        mirror = (pivot + listSize + 1) >> 1
        end = listSize - 1
        # Again, seed and round input is in place, just update the position.
        # We start at the end, and work back to the mirror point.
        # This makes us process each pear exactly once (instead of unnecessarily twice, like in the spec)
        buf[_SHUFFLE_H_PIVOT_VIEW_SIZE:] = ((end >> 8) & 0xffff_ffff).to_bytes(length=4, byteorder=ENDIANNESS)
        source = hash(buf)
        byteV = source[(end&0xff)>>3]
        i, j = pivot+1, end
        while i < mirror:
            # Exact same thing (copy of above loop body)
            #--------------------------------------------
            # The pair is i,j. With j being the bigger of the two, hence the "position" identifier of the pair.
            # Every 256th bit (aligned to j).
            if j&0xff == 0xff:
                # just overwrite the last part of the buffer, reuse the start (seed, round)
                buf[_SHUFFLE_H_PIVOT_VIEW_SIZE:] = ((j >> 8) & 0xffff_ffff).to_bytes(length=4, byteorder=ENDIANNESS)
                source = hash(buf)

            # Same trick with byte retrieval. Only every 8th.
            if j&0x7 == 0x7:
                byteV = source[(j&0xff)>>3]

            bitV = (byteV >> (j & 0x7)) & 0x1

            if bitV == 1:
                # swap the pair items
                input[i], input[j] = input[j], input[i]

            i, j = i+1, j-1
            #--------------------------------------------

        # go forwards?
        if dir:
            # -> shuffle
            r += 1
            if r == SHUFFLE_ROUND_COUNT:
                break
        else:
            if r == 0:
                break
            # -> un-shuffle
            r -= 1


def compute_shuffled_index(index: ValidatorIndex, index_count: int, seed: Bytes32) -> ValidatorIndex:
    """
    Return the shuffled validator index corresponding to ``seed`` (and ``index_count``).
    """
    assert index < index_count

    # Swap or not (https://link.springer.com/content/pdf/10.1007%2F978-3-642-32009-5_1.pdf)
    # See the 'generalized domain' algorithm on page 3
    for current_round in range(SHUFFLE_ROUND_COUNT):
        pivot_bytez = hash(seed + int_to_bytes(current_round, length=1))[0:8]
        pivot = int.from_bytes(pivot_bytez, byteorder=ENDIANNESS) % index_count
        flip = int((pivot + index_count - index) % index_count)
        position = max(index, flip)
        source = hash(seed + int_to_bytes(current_round, length=1) + int_to_bytes(position // 256, length=4))
        byte = source[(position % 256) // 8]
        bit = (byte >> (position % 8)) % 2
        index = flip if bit else index

    return ValidatorIndex(index)


def compute_committee_count(active_validators_count: int) -> int:
    validators_per_slot = active_validators_count // SLOTS_PER_EPOCH
    committees_per_slot = validators_per_slot // TARGET_COMMITTEE_SIZE
    if MAX_COMMITTEES_PER_SLOT < committees_per_slot:
        committees_per_slot = MAX_COMMITTEES_PER_SLOT
    if committees_per_slot == 0:
        committees_per_slot = 1
    return committees_per_slot


Committee = Sequence[ValidatorIndex]  # as with indexed attestation order (index of validator within committee)
SlotCommittees = Sequence[Committee]  # by index of committee (len <= MAX_COMMITTEES_PER_SLOT)
EpochCommittees = Sequence[SlotCommittees]  # (len == SLOTS_PER_EPOCH)


# With a high amount of shards, or low amount of validators,
# some shards may not have a committee this epoch.
class ShufflingEpoch(object):
    epoch:      Epoch
    active_indices: Sequence[ValidatorIndex]  # non-shuffled active validator indices
    shuffling:  Sequence[ValidatorIndex]  # the active validator indices, shuffled into their committee
    committees: EpochCommittees  # list of lists of slices of Shuffling

    # indices_bounded: (index, activation_epoch, exit_epoch) per validator.
    def __init__(self,
                 state: BeaconState,
                 indices_bounded: Sequence[Tuple[ValidatorIndex, Epoch, Epoch]],
                 epoch: Epoch):
        self.epoch = epoch

        seed = get_seed(state, epoch, DOMAIN_BEACON_ATTESTER)

        self.active_indices = [index for (index, activation_epoch, exit_epoch) in indices_bounded
                               if activation_epoch <= epoch < exit_epoch]

        shuffling = list(self.active_indices)  # copy
        unshuffle_list(shuffling, seed)
        self.shuffling = shuffling

        active_validator_count = len(self.active_indices)
        committees_per_slot = compute_committee_count(active_validator_count)

        committee_count = committees_per_slot * uint64(SLOTS_PER_EPOCH)

        def slice_committee(slot: int, comm_index: int):
            index = (slot * committees_per_slot) + comm_index
            start_offset = (active_validator_count * index) // committee_count
            end_offset = (active_validator_count * (index + 1)) // committee_count
            assert start_offset <= end_offset
            return self.shuffling[start_offset:end_offset]

        self.committees = [[slice_committee(slot, comm_index) for comm_index in range(committees_per_slot)]
                           for slot in range(SLOTS_PER_EPOCH)]


def compute_proposer_index(state: BeaconState, indices: Sequence[ValidatorIndex], seed: Bytes32) -> ValidatorIndex:
    """
    Return from ``indices`` a random index sampled by effective balance.
    """
    assert len(indices) > 0
    MAX_RANDOM_BYTE = 2**8 - 1
    i = 0
    while True:
        candidate_index = indices[compute_shuffled_index(ValidatorIndex(i % len(indices)), len(indices), seed)]
        random_byte = hash(seed + int_to_bytes(i // 32, length=8))[i % 32]
        effective_balance = state.validators[candidate_index].effective_balance
        if effective_balance * MAX_RANDOM_BYTE >= MAX_EFFECTIVE_BALANCE * random_byte:
            return ValidatorIndex(candidate_index)
        i += 1


class EpochsContext(object):
    pubkey2index: Dict[BLSPubkey, ValidatorIndex]
    index2pubkey: PyList[BLSPubkey]
    proposers:  Sequence[ValidatorIndex]  # 1 proposer per slot, only of current epoch.
    previous_shuffling: Optional[ShufflingEpoch]
    current_shuffling:  Optional[ShufflingEpoch]
    next_shuffling:     Optional[ShufflingEpoch]

    def __init__(self):
        self.pubkey2index = {}
        self.index2pubkey = []
        self.proposers = []
        self.previous_shuffling = None
        self.current_shuffling = None
        self.next_shuffling = None

    def load_state(self, state: BeaconState):
        self.sync_pubkeys(state)
        current_epoch = compute_epoch_at_slot(state.slot)
        previous_epoch = GENESIS_EPOCH if current_epoch == GENESIS_EPOCH else Epoch(current_epoch - 1)
        next_epoch = Epoch(current_epoch + 1)

        indices_bounded = [(ValidatorIndex(i), v.activation_epoch, v.exit_epoch)
                           for i, v in enumerate(state.validators.readonly_iter())]

        self.current_shuffling = ShufflingEpoch(state, indices_bounded, current_epoch)
        if previous_epoch == current_epoch:  # In case of genesis
            self.previous_shuffling = self.current_shuffling
        else:
            self.previous_shuffling = ShufflingEpoch(state, indices_bounded, previous_epoch)
        self.next_shuffling = ShufflingEpoch(state, indices_bounded, next_epoch)
        self._reset_proposers(state)

    def _reset_proposers(self, state: BeaconState):
        epoch_seed = get_seed(state, self.current_shuffling.epoch, DOMAIN_BEACON_PROPOSER)
        start_slot = compute_start_slot_at_epoch(self.current_shuffling.epoch)
        self.proposers = [
            compute_proposer_index(state, self.current_shuffling.active_indices,
                                   hash(epoch_seed + slot.to_bytes(length=8, byteorder=ENDIANNESS)))
            for slot in range(start_slot, start_slot+SLOTS_PER_EPOCH)
        ]

    def copy(self) -> "EpochsContext":
        epochs_ctx = EpochsContext()
        # Full copy of pubkeys, this can mutate
        epochs_ctx.pubkey2index = self.pubkey2index.copy()
        epochs_ctx.index2pubkey = self.index2pubkey.copy()
        # Only shallow-copy the other data, it doesn't mutate (only completely replaced on rotation)
        epochs_ctx.proposers = self.proposers
        epochs_ctx.previous_shuffling = self.previous_shuffling
        epochs_ctx.current_shuffling = self.current_shuffling
        epochs_ctx.next_shuffling = self.next_shuffling
        return epochs_ctx

    def sync_pubkeys(self, state: BeaconState):
        if self.pubkey2index is None:
            self.pubkey2index = {}
        if self.index2pubkey is None:
            self.index2pubkey = []

        current_count = len(self.pubkey2index)
        assert current_count == len(self.index2pubkey)
        for i in range(current_count, len(state.validators)):
            pubkey: BLSPubkey = state.validators[i].pubkey
            index = ValidatorIndex(i)
            self.pubkey2index[pubkey] = index
            self.index2pubkey.append(pubkey)

    def rotate_epochs(self, state: BeaconState):
        self.previous_shuffling = self.current_shuffling
        self.current_shuffling = self.next_shuffling
        next_epoch = Epoch(self.current_shuffling.epoch + 1)
        indices_bounded = [(ValidatorIndex(i), v.activation_epoch, v.exit_epoch)
                           for i, v in enumerate(state.validators.readonly_iter())]
        self.next_shuffling = ShufflingEpoch(state, indices_bounded, next_epoch)
        self._reset_proposers(state)

    def _get_slot_comms(self, slot: Slot) -> SlotCommittees:
        epoch = compute_epoch_at_slot(slot)
        epoch_slot = slot % SLOTS_PER_EPOCH
        if epoch == self.previous_shuffling.epoch:
            return self.previous_shuffling.committees[epoch_slot]
        elif epoch == self.current_shuffling.epoch:
            return self.current_shuffling.committees[epoch_slot]
        elif epoch == self.next_shuffling.epoch:
            return self.next_shuffling.committees[epoch_slot]
        else:
            raise Exception(f"crosslink committee retrieval: out of range epoch: {epoch}")

    # Return the beacon committee at slot for index.
    def get_beacon_committee(self, slot: Slot, index: CommitteeIndex) -> Committee:
        slot_comms = self._get_slot_comms(slot)

        if index >= len(slot_comms):
            raise Exception(f"crosslink committee retrieval: out of range committee index: {index}")

        return slot_comms[index]

    def get_committee_count_at_slot(self, slot: Slot) -> uint64:
        return uint64(len(self._get_slot_comms(slot)))

    def get_beacon_proposer(self, slot: Slot) -> ValidatorIndex:
        epoch = compute_epoch_at_slot(slot)
        assert epoch == self.current_shuffling.epoch
        return self.proposers[slot % SLOTS_PER_EPOCH]


FLAG_PREV_SOURCE_ATTESTER = 1 << 0
FLAG_PREV_TARGET_ATTESTER = 1 << 1
FLAG_PREV_HEAD_ATTESTER = 1 << 2
FLAG_CURR_SOURCE_ATTESTER = 1 << 3
FLAG_CURR_TARGET_ATTESTER = 1 << 4
FLAG_CURR_HEAD_ATTESTER = 1 << 5
FLAG_UNSLASHED = 1 << 6
FLAG_ELIGIBLE_ATTESTER = 1 << 7


class FlatValidator(object):

    __slots__ = 'effective_balance', 'slashed', 'activation_eligibility_epoch',\
                'activation_epoch', 'exit_epoch', 'withdrawable_epoch'

    effective_balance: Gwei  # Balance at stake
    slashed: boolean
    # Status epochs
    activation_eligibility_epoch: Epoch  # When criteria for activation were met
    activation_epoch: Epoch
    exit_epoch: Epoch
    withdrawable_epoch: Epoch  # When validator can withdraw funds

    def __init__(self, v: Validator):
        _, _, self.effective_balance, self.slashed, self.activation_eligibility_epoch, \
            self.activation_epoch, self.exit_epoch, self.withdrawable_epoch = v


class AttesterStatus(object):

    __slots__ = 'flags', 'proposer_index', 'inclusion_delay', 'validator', 'active'

    flags: int
    proposer_index: int  # -1 when not included by any proposer
    inclusion_delay: int
    validator: FlatValidator
    active: bool

    def __init__(self, v: FlatValidator):
        self.flags = 0
        self.proposer_index = -1
        self.inclusion_delay = 0
        self.validator = v
        self.active = False


def has_markers(flags: int, markers: int) -> bool:
    return flags & markers == markers


class EpochStakeSummary(object):

    __slots__ = 'source_stake', 'target_stake', 'head_stake'

    source_stake: Gwei
    target_stake: Gwei
    head_stake: Gwei

    def __init__(self):
        self.source_stake = Gwei(0)
        self.target_stake = Gwei(0)
        self.head_stake = Gwei(0)


class EpochProcess(object):
    prev_epoch: Epoch
    current_epoch: Epoch
    statuses: PyList[AttesterStatus]
    total_active_stake: Gwei
    prev_epoch_unslashed_stake: EpochStakeSummary
    curr_epoch_unslashed_target_stake: Gwei
    active_validators: int  # Thanks to exit delay, this does not change within the epoch processing.
    indices_to_slash: PyList[ValidatorIndex]
    indices_to_set_activation_eligibility: PyList[ValidatorIndex]
    # ignores churn. Apply churn-limit manually.
    # Maybe, because finality affects it still.
    indices_to_maybe_activate: PyList[ValidatorIndex]

    indices_to_eject: PyList[ValidatorIndex]
    exit_queue_end: Epoch
    exit_queue_end_churn: int
    churn_limit: int

    def __init__(self):
        self.current_epoch = Epoch(0)
        self.prev_epoch = Epoch(0)
        self.statuses = []
        self.total_active_stake = Gwei(0)
        self.prev_epoch_unslashed_stake = EpochStakeSummary()
        self.curr_epoch_unslashed_target_stake = Gwei(0)
        self.active_validators = 0
        self.indices_to_slash = []
        self.indices_to_set_activation_eligibility = []
        self.indices_to_maybe_activate = []
        self.indices_to_eject = []
        self.exit_queue_end = Epoch(0)
        self.exit_queue_end_churn = 0
        self.churn_limit = 0


def compute_epoch_at_slot(slot: Slot) -> Epoch:
    """
    Return the epoch number at ``slot``.
    """
    return Epoch(slot // SLOTS_PER_EPOCH)


def get_churn_limit(active_validator_count: uint64) -> uint64:
    return max(MIN_PER_EPOCH_CHURN_LIMIT, active_validator_count // CHURN_LIMIT_QUOTIENT)


def is_active_validator(v: Validator, epoch: Epoch) -> bool:
    return v.activation_epoch <= epoch < v.exit_epoch


def is_active_flat_validator(v: FlatValidator, epoch: Epoch) -> bool:
    return v.activation_epoch <= epoch < v.exit_epoch


def compute_activation_exit_epoch(epoch: Epoch) -> Epoch:
    """
    Return the epoch during which validator activations and exits initiated in ``epoch`` take effect.
    """
    return Epoch(epoch + 1 + MAX_SEED_LOOKAHEAD)


def compute_start_slot_at_epoch(epoch: Epoch) -> Slot:
    """
    Return the start slot of ``epoch``.
    """
    return Slot(epoch * SLOTS_PER_EPOCH)


def get_block_root_at_slot(state: BeaconState, slot: Slot) -> Root:
    """
    Return the block root at a recent ``slot``.
    """
    assert slot < state.slot <= slot + SLOTS_PER_HISTORICAL_ROOT
    return state.block_roots[slot % SLOTS_PER_HISTORICAL_ROOT]


def get_block_root(state: BeaconState, epoch: Epoch) -> Root:
    """
    Return the block root at the start of a recent ``epoch``.
    """
    return get_block_root_at_slot(state, compute_start_slot_at_epoch(epoch))


def prepare_epoch_process_state(epochs_ctx: EpochsContext, state: BeaconState) -> EpochProcess:
    # TODO maybe allocate status array at exact capacity? count = len(state.validators)
    out = EpochProcess()

    current_epoch = epochs_ctx.current_shuffling.epoch
    prev_epoch = epochs_ctx.previous_shuffling.epoch
    out.current_epoch = current_epoch
    out.prev_epoch = prev_epoch

    slashings_epoch = current_epoch + (EPOCHS_PER_SLASHINGS_VECTOR // 2)
    exit_queue_end = compute_activation_exit_epoch(current_epoch)

    active_count = uint64(0)
    # fast read-only iterate over tree-structured validator set.
    for i, tree_v in enumerate(state.validators.readonly_iter()):
        v = FlatValidator(tree_v)
        status = AttesterStatus(v)
        
        if v.slashed:
            if slashings_epoch == v.withdrawable_epoch:
                out.indices_to_slash.append(ValidatorIndex(i))
        else:
            status.flags |= FLAG_UNSLASHED

        if is_active_flat_validator(v, prev_epoch) or (v.slashed and (prev_epoch + 1 < v.withdrawable_epoch)):
            status.flags |= FLAG_ELIGIBLE_ATTESTER

        active = is_active_flat_validator(v, current_epoch)
        if active:
            status.active = True
            out.total_active_stake += v.effective_balance
            active_count += 1

        if v.exit_epoch != FAR_FUTURE_EPOCH and v.exit_epoch > exit_queue_end:
            exit_queue_end = v.exit_epoch

        if v.activation_eligibility_epoch == FAR_FUTURE_EPOCH and v.effective_balance == MAX_EFFECTIVE_BALANCE:
            out.indices_to_set_activation_eligibility.append(ValidatorIndex(i))

        if v.activation_epoch == FAR_FUTURE_EPOCH and v.activation_eligibility_epoch <= current_epoch:
            out.indices_to_maybe_activate.append(ValidatorIndex(i))

        if status.active and v.effective_balance <= EJECTION_BALANCE and v.exit_epoch == FAR_FUTURE_EPOCH:
            out.indices_to_eject.append(ValidatorIndex(i))

        out.statuses.append(status)

    out.active_validators = active_count

    if out.total_active_stake < EFFECTIVE_BALANCE_INCREMENT:
        out.total_active_stake = EFFECTIVE_BALANCE_INCREMENT

    # order by the sequence of activation_eligibility_epoch setting and then index
    out.indices_to_maybe_activate = sorted(out.indices_to_maybe_activate,
                                           key=lambda i: (out.statuses[i].validator.activation_eligibility_epoch, i))

    exit_queue_end_churn = uint64(0)
    for status in out.statuses:
        if status.validator.exit_epoch == exit_queue_end:
            exit_queue_end_churn += 1
    
    churn_limit = get_churn_limit(active_count)
    if exit_queue_end_churn >= churn_limit:
        exit_queue_end += 1
        exit_queue_end_churn = 0

    out.exit_queue_end_churn = exit_queue_end_churn
    out.exit_queue_end = exit_queue_end
    out.churn_limit = churn_limit

    def status_process_epoch(statuses: Sequence[AttesterStatus],
                             attestations: Iterator[PendingAttestation],
                             epoch: Epoch, source_flag: int, target_flag: int, head_flag: int):
        actual_target_block_root = get_block_root_at_slot(state, compute_start_slot_at_epoch(epoch))

        for att in attestations:
            # Load all the attestation details from the state tree once, do not reload for each participant.
            aggregation_bits, att_data, inclusion_delay, proposer_index = att

            att_slot, committee_index, att_beacon_block_root, _, att_target = att_data

            att_bits = list(aggregation_bits)
            att_voted_target_root = att_target.root == actual_target_block_root
            att_voted_head_root = att_beacon_block_root == get_block_root_at_slot(state, att_slot)

            # attestation-target is already known to be this epoch, get it from the pre-computed shuffling directly.
            committee = epochs_ctx.get_beacon_committee(att_slot, committee_index)

            participants = list(index for i, index in enumerate(committee) if att_bits[i])

            if epoch == prev_epoch:
                for p in participants:
                    status = statuses[p]

                    # If the attestation is the earliest, i.e. has the smallest delay
                    if status.proposer_index == -1 or status.inclusion_delay > inclusion_delay:
                        status.proposer_index = proposer_index
                        status.inclusion_delay = inclusion_delay

            for p in participants:
                status = statuses[p]

                # remember the participant as one of the good validators
                status.flags |= source_flag

                # If the attestation is for the boundary:
                if att_voted_target_root:
                    status.flags |= target_flag

                    # Head votes must be a subset of target votes
                    if att_voted_head_root:
                        status.flags |= head_flag

    # When used in a non-epoch transition on top of genesis state, avoid reaching to a block from before genesis.
    if state.slot > 0:
        status_process_epoch(out.statuses, state.previous_epoch_attestations.readonly_iter(), prev_epoch,
                             FLAG_PREV_SOURCE_ATTESTER, FLAG_PREV_TARGET_ATTESTER, FLAG_PREV_HEAD_ATTESTER)
    # When used in a non-epoch transition, it may be the absolute start of the epoch,
    # and the current epoch will not have any attestations (or a target block root to match them against)
    if compute_start_slot_at_epoch(current_epoch) < state.slot:
        status_process_epoch(out.statuses, state.current_epoch_attestations.readonly_iter(), current_epoch,
                             FLAG_CURR_SOURCE_ATTESTER, FLAG_CURR_TARGET_ATTESTER, FLAG_CURR_HEAD_ATTESTER)

    # Python quirk; avoid Gwei during summation here, not worth the __add__ overhead.
    prev_source_unsl_stake, prev_target_unsl_stake, prev_head_unsl_stake = 0, 0, 0
    curr_epoch_unslashed_target_stake = 0

    for status in out.statuses:
        if has_markers(status.flags, FLAG_PREV_SOURCE_ATTESTER | FLAG_UNSLASHED):
            prev_source_unsl_stake += status.validator.effective_balance
            if has_markers(status.flags, FLAG_PREV_TARGET_ATTESTER):
                prev_target_unsl_stake += status.validator.effective_balance
                if has_markers(status.flags, FLAG_PREV_HEAD_ATTESTER):
                    prev_head_unsl_stake += status.validator.effective_balance
        if has_markers(status.flags, FLAG_CURR_TARGET_ATTESTER | FLAG_UNSLASHED):
            curr_epoch_unslashed_target_stake += status.validator.effective_balance

    out.prev_epoch_unslashed_stake.source_stake = max(prev_source_unsl_stake, EFFECTIVE_BALANCE_INCREMENT)
    out.prev_epoch_unslashed_stake.target_stake = max(prev_target_unsl_stake, EFFECTIVE_BALANCE_INCREMENT)
    out.prev_epoch_unslashed_stake.head_stake = max(prev_head_unsl_stake, EFFECTIVE_BALANCE_INCREMENT)
    out.curr_epoch_unslashed_target_stake = max(curr_epoch_unslashed_target_stake, EFFECTIVE_BALANCE_INCREMENT)

    return out


def get_randao_mix(state: BeaconState, epoch: Epoch) -> Bytes32:
    """
    Return the randao mix at a recent ``epoch``.
    """
    return state.randao_mixes[epoch % EPOCHS_PER_HISTORICAL_VECTOR]


def int_to_bytes(n: int, length: int) -> bytes:
    """
    Return the ``length``-byte serialization of ``n`` in ``ENDIANNESS``-endian.
    """
    return n.to_bytes(length=length, byteorder=ENDIANNESS)


def get_seed(state: BeaconState, epoch: Epoch, domain_type: DomainType) -> Bytes32:
    """
    Return the seed at ``epoch``.
    """
    mix = get_randao_mix(state, Epoch(epoch + EPOCHS_PER_HISTORICAL_VECTOR - MIN_SEED_LOOKAHEAD - 1))  # Avoid underflow
    return hash(domain_type + int_to_bytes(epoch, length=8) + mix)


def increase_balance(state: BeaconState, index: ValidatorIndex, delta: Gwei) -> None:
    """
    Increase the validator balance at index ``index`` by ``delta``.
    """
    state.balances[index] += delta


def decrease_balance(state: BeaconState, index: ValidatorIndex, delta: Gwei) -> None:
    """
    Decrease the validator balance at index ``index`` by ``delta``, with underflow protection.
    """
    state.balances[index] = 0 if delta > state.balances[index] else state.balances[index] - delta


def process_justification_and_finalization(epochs_ctx: EpochsContext, process: EpochProcess, state: BeaconState) -> None:
    previous_epoch = process.prev_epoch
    current_epoch = process.current_epoch

    if current_epoch <= GENESIS_EPOCH + 1:
        return

    old_previous_justified_checkpoint = state.previous_justified_checkpoint
    old_current_justified_checkpoint = state.current_justified_checkpoint

    # Process justifications
    state.previous_justified_checkpoint = state.current_justified_checkpoint
    bits = state.justification_bits
    # shift bits, zero out new bit space
    bits[1:] = bits[:-1]
    bits[0] = 0b0
    if process.prev_epoch_unslashed_stake.target_stake * 3 >= process.total_active_stake * 2:
        state.current_justified_checkpoint = Checkpoint(epoch=previous_epoch,
                                                        root=get_block_root(state, previous_epoch))
        bits[1] = 0b1
    if process.curr_epoch_unslashed_target_stake * 3 >= process.total_active_stake * 2:
        state.current_justified_checkpoint = Checkpoint(epoch=current_epoch,
                                                        root=get_block_root(state, current_epoch))
        bits[0] = 0b1
    state.justification_bits = bits
    assert len(bits) == 4

    # Process finalizations
    # The 2nd/3rd/4th most recent epochs are justified, the 2nd using the 4th as source
    if all(bits[1:4]) and old_previous_justified_checkpoint.epoch + 3 == current_epoch:
        state.finalized_checkpoint = old_previous_justified_checkpoint
    # The 2nd/3rd most recent epochs are justified, the 2nd using the 3rd as source
    if all(bits[1:3]) and old_previous_justified_checkpoint.epoch + 2 == current_epoch:
        state.finalized_checkpoint = old_previous_justified_checkpoint
    # The 1st/2nd/3rd most recent epochs are justified, the 1st using the 3rd as source
    if all(bits[0:3]) and old_current_justified_checkpoint.epoch + 2 == current_epoch:
        state.finalized_checkpoint = old_current_justified_checkpoint
    # The 1st/2nd most recent epochs are justified, the 1st using the 2nd as source
    if all(bits[0:2]) and old_current_justified_checkpoint.epoch + 1 == current_epoch:
        state.finalized_checkpoint = old_current_justified_checkpoint


class Deltas(NamedTuple):
    rewards: Sequence[Gwei]
    penalties: Sequence[Gwei]


class RewardsAndPenalties(NamedTuple):
    source: Deltas
    target: Deltas
    head: Deltas
    inclusion_delay: Deltas
    inactivity: Deltas


def mk_rew_pen(size: int) -> RewardsAndPenalties:
    return RewardsAndPenalties(
        source=Deltas([Gwei(0)] * size, [Gwei(0)] * size),
        target=Deltas([Gwei(0)] * size, [Gwei(0)] * size),
        head=Deltas([Gwei(0)] * size, [Gwei(0)] * size),
        inclusion_delay=Deltas([Gwei(0)] * size, [Gwei(0)] * size),
        inactivity=Deltas([Gwei(0)] * size, [Gwei(0)] * size),
    )


def get_attestation_rewards_and_penalties(epochs_ctx: EpochsContext, process: EpochProcess, state: BeaconState) -> RewardsAndPenalties:
    validator_count = len(process.statuses)
    res = mk_rew_pen(validator_count)

    def has_markers(flags: int, markers: int) -> bool:
        return flags & markers == markers

    increment = EFFECTIVE_BALANCE_INCREMENT
    total_balance = max(process.total_active_stake, increment)

    prev_epoch_source_stake = max(process.prev_epoch_unslashed_stake.source_stake, increment)
    prev_epoch_target_stake = max(process.prev_epoch_unslashed_stake.target_stake, increment)
    prev_epoch_head_stake = max(process.prev_epoch_unslashed_stake.head_stake, increment)

    # Sqrt first, before factoring out the increment for later usage.
    balance_sq_root = integer_squareroot(total_balance)
    finality_delay = process.prev_epoch - state.finalized_checkpoint.epoch

    is_inactivity_leak = finality_delay > MIN_EPOCHS_TO_INACTIVITY_PENALTY

    # All summed effective balances are normalized to effective-balance increments, to avoid overflows.
    total_balance //= increment
    prev_epoch_source_stake //= increment
    prev_epoch_target_stake //= increment
    prev_epoch_head_stake //= increment

    for i, status in enumerate(process.statuses):

        eff_balance = status.validator.effective_balance
        base_reward = eff_balance * BASE_REWARD_FACTOR // balance_sq_root // BASE_REWARDS_PER_EPOCH
        proposer_reward = base_reward // PROPOSER_REWARD_QUOTIENT

        # Inclusion speed bonus
        if has_markers(status.flags, FLAG_PREV_SOURCE_ATTESTER | FLAG_UNSLASHED):
            res.inclusion_delay.rewards[status.proposer_index] += proposer_reward
            max_attester_reward = base_reward - proposer_reward
            res.inclusion_delay.rewards[i] += max_attester_reward // status.inclusion_delay

        if status.flags & FLAG_ELIGIBLE_ATTESTER != 0:
            # In case of `is_inactivity_leak`:
            # Since full base reward will be canceled out by inactivity penalty deltas,
            # optimal participation receives full base reward compensation here.

            # Expected FFG source
            if has_markers(status.flags, FLAG_PREV_SOURCE_ATTESTER | FLAG_UNSLASHED):
                if is_inactivity_leak:
                    res.source.rewards[i] += base_reward
                else:
                    # Justification-participation reward
                    res.source.rewards[i] += base_reward * prev_epoch_source_stake // total_balance
            else:
                # Justification-non-participation R-penalty
                res.source.penalties[i] += base_reward

            # Expected FFG target
            if has_markers(status.flags, FLAG_PREV_TARGET_ATTESTER | FLAG_UNSLASHED):
                if is_inactivity_leak:
                    res.target.rewards[i] += base_reward
                else:
                    # Boundary-attestation reward
                    res.target.rewards[i] += base_reward * prev_epoch_target_stake // total_balance
            else:
                # Boundary-attestation-non-participation R-penalty
                res.target.penalties[i] += base_reward

            # Expected head
            if has_markers(status.flags, FLAG_PREV_HEAD_ATTESTER | FLAG_UNSLASHED):
                if is_inactivity_leak:
                    res.head.rewards[i] += base_reward
                else:
                    # Canonical-participation reward
                    res.head.rewards[i] += base_reward * prev_epoch_head_stake // total_balance
            else:
                # Non-canonical-participation R-penalty
                res.head.penalties[i] += base_reward

            # Take away max rewards if we're not finalizing
            if is_inactivity_leak:
                # If validator is performing optimally this cancels all rewards for a neutral balance
                res.inclusion_delay.penalties[i] += base_reward * BASE_REWARDS_PER_EPOCH - proposer_reward
                if not has_markers(status.flags, FLAG_PREV_TARGET_ATTESTER | FLAG_UNSLASHED):
                    res.inclusion_delay.penalties[i] += eff_balance * finality_delay // INACTIVITY_PENALTY_QUOTIENT

    return res


def process_rewards_and_penalties(epochs_ctx: EpochsContext, process: EpochProcess, state: BeaconState) -> None:
    if process.current_epoch == GENESIS_EPOCH:
        return

    res = get_attestation_rewards_and_penalties(epochs_ctx, process, state)
    new_balances = list(map(int, state.balances.readonly_iter()))

    def add_rewards(deltas: Deltas):
        for i, reward in enumerate(deltas.rewards):
            new_balances[i] += reward

    def add_penalties(deltas: Deltas):
        for i, penalty in enumerate(deltas.penalties):
            if penalty > new_balances[i]:
                new_balances[i] = 0
            else:
                new_balances[i] -= penalty

    add_rewards(res.source)
    add_rewards(res.target)
    add_rewards(res.head)
    add_rewards(res.inclusion_delay)
    add_rewards(res.inactivity)

    add_penalties(res.source)
    add_penalties(res.target)
    add_penalties(res.head)
    add_penalties(res.inclusion_delay)
    add_penalties(res.inactivity)

    # Important: do not change state one balance at a time.
    # Set them all at once, constructing the tree in one go.
    state.balances = new_balances


def process_registry_updates(epochs_ctx: EpochsContext, process: EpochProcess, state: BeaconState) -> None:
    exit_end = process.exit_queue_end
    end_churn = process.exit_queue_end_churn
    # Process ejections
    for index in process.indices_to_eject:
        validator = state.validators[index]

        # Set validator exit epoch and withdrawable epoch
        validator.exit_epoch = exit_end
        validator.withdrawable_epoch = Epoch(exit_end + MIN_VALIDATOR_WITHDRAWABILITY_DELAY)

        end_churn += 1
        if end_churn >= process.churn_limit:
            end_churn = 0
            exit_end += 1

    # Set new activation eligibilities
    for index in process.indices_to_set_activation_eligibility:
        state.validators[index].activation_eligibility_epoch = epochs_ctx.current_shuffling.epoch + 1

    finality_epoch = state.finalized_checkpoint.epoch
    # Dequeue validators for activation up to churn limit
    for index in process.indices_to_maybe_activate[:process.churn_limit]:
        # Placement in queue is finalized
        if process.statuses[index].validator.activation_eligibility_epoch > finality_epoch:
            break  # remaining validators all have an activation_eligibility_epoch that is higher anyway, break early.
        validator = state.validators[index]
        validator.activation_epoch = compute_activation_exit_epoch(process.current_epoch)


def process_slashings(epochs_ctx: EpochsContext, process: EpochProcess, state: BeaconState) -> None:
    total_balance = process.total_active_stake
    slashings_scale = min(sum(state.slashings.readonly_iter()) * 3, total_balance)
    for index in process.indices_to_slash:
        increment = EFFECTIVE_BALANCE_INCREMENT  # Factored out from penalty numerator to avoid uint64 overflow
        effective_balance = process.statuses[index].validator.effective_balance
        penalty_numerator = effective_balance // increment * slashings_scale
        penalty = penalty_numerator // total_balance * increment
        decrease_balance(state, index, penalty)


HYSTERESIS_INCREMENT = EFFECTIVE_BALANCE_INCREMENT // HYSTERESIS_QUOTIENT
DOWNWARD_THRESHOLD = HYSTERESIS_INCREMENT * HYSTERESIS_DOWNWARD_MULTIPLIER
UPWARD_THRESHOLD = HYSTERESIS_INCREMENT * HYSTERESIS_UPWARD_MULTIPLIER


def process_final_updates(epochs_ctx: EpochsContext, process: EpochProcess, state: BeaconState) -> None:
    current_epoch = process.current_epoch
    next_epoch = Epoch(current_epoch + 1)

    # Reset eth1 data votes
    if next_epoch % EPOCHS_PER_ETH1_VOTING_PERIOD == 0:
        state.eth1_data_votes = []

    # Update effective balances with hysteresis
    for (index, status), balance in zip(enumerate(process.statuses), state.balances.readonly_iter()):
        effective_balance = status.validator.effective_balance
        if balance + DOWNWARD_THRESHOLD < effective_balance or effective_balance + UPWARD_THRESHOLD < balance:
            new_effective_balance = min(balance - balance % EFFECTIVE_BALANCE_INCREMENT, MAX_EFFECTIVE_BALANCE)
            state.validators[index].effective_balance = new_effective_balance

    # Reset slashings
    state.slashings[next_epoch % EPOCHS_PER_SLASHINGS_VECTOR] = Gwei(0)

    # Set randao mix
    state.randao_mixes[next_epoch % EPOCHS_PER_HISTORICAL_VECTOR] = get_randao_mix(state, current_epoch)

    # Set historical root accumulator
    if next_epoch % (SLOTS_PER_HISTORICAL_ROOT // SLOTS_PER_EPOCH) == 0:
        historical_batch = HistoricalBatch(block_roots=state.block_roots, state_roots=state.state_roots)
        state.historical_roots.append(hash_tree_root(historical_batch))

    # Rotate current/previous epoch attestations
    state.previous_epoch_attestations = state.current_epoch_attestations
    state.current_epoch_attestations = []


def process_block_header(epochs_ctx: EpochsContext, state: BeaconState, block: BeaconBlock) -> None:
    # Verify that the slots match
    assert block.slot == state.slot
    # Verify that the block is newer than latest block header
    assert block.slot > state.latest_block_header.slot
    # Verify that proposer index is the correct index
    proposer_index = epochs_ctx.get_beacon_proposer(state.slot)
    assert block.proposer_index == proposer_index
    # Verify that the parent matches
    assert block.parent_root == hash_tree_root(state.latest_block_header)
    # Cache current block as the new latest block
    state.latest_block_header = BeaconBlockHeader(
        slot=block.slot,
        proposer_index=block.proposer_index,
        parent_root=block.parent_root,
        state_root=Bytes32(),  # Overwritten in the next process_slot call
        body_root=hash_tree_root(block.body),
    )

    # Verify proposer is not slashed
    proposer = state.validators[proposer_index]
    assert not proposer.slashed


def process_randao(epochs_ctx: EpochsContext, state: BeaconState, body: BeaconBlockBody) -> None:
    epoch = epochs_ctx.current_shuffling.epoch
    # Verify RANDAO reveal
    proposer_index = epochs_ctx.get_beacon_proposer(state.slot)
    proposer_pubkey = epochs_ctx.index2pubkey[proposer_index]
    signing_root = compute_signing_root(epoch, get_domain(state, DOMAIN_RANDAO))
    assert bls_Verify(proposer_pubkey, signing_root, body.randao_reveal)
    # Mix in RANDAO reveal
    mix = xor(get_randao_mix(state, epoch), hash(body.randao_reveal))
    state.randao_mixes[epoch % EPOCHS_PER_HISTORICAL_VECTOR] = mix


SLOTS_PER_ETH1_VOTING_PERIOD = EPOCHS_PER_ETH1_VOTING_PERIOD * SLOTS_PER_EPOCH


def process_eth1_data(epochs_ctx: EpochsContext, state: BeaconState, body: BeaconBlockBody) -> None:
    new_eth1_data = body.eth1_data
    state.eth1_data_votes.append(new_eth1_data)
    if state.eth1_data == new_eth1_data:
        return  # Nothing to do if the state already has this as eth1data (happens a lot after majority vote is in)
    # `.count()` is slow due to type checks, calls len() repeatedly,
    # and wrong when applied to list(state.eth1_data_votes.readonly_iter())
    # Avoid it, and instead, count the votes manually
    votes = 0
    for vote in state.eth1_data_votes.readonly_iter():
        if vote.hash_tree_root() == new_eth1_data.hash_tree_root():
            votes += 1
    if votes * 2 > SLOTS_PER_ETH1_VOTING_PERIOD:
        state.eth1_data = new_eth1_data


def process_operations(epochs_ctx: EpochsContext, state: BeaconState, body: BeaconBlockBody) -> None:
    # Verify that outstanding deposits are processed up to the maximum number of deposits
    assert len(body.deposits) == min(MAX_DEPOSITS, state.eth1_data.deposit_count - state.eth1_deposit_index)

    for operations, function in (
            (body.proposer_slashings, process_proposer_slashing),
            (body.attester_slashings, process_attester_slashing),
            (body.attestations, process_attestation),
            (body.deposits, process_deposit),
            (body.voluntary_exits, process_voluntary_exit),
    ):
        for operation in operations.readonly_iter():
            function(epochs_ctx, state, operation)


def is_slashable_validator(validator: Validator, epoch: Epoch) -> bool:
    """
    Check if ``validator`` is slashable.
    """
    return (not validator.slashed) and (validator.activation_epoch <= epoch < validator.withdrawable_epoch)


def is_slashable_attestation_data(data_1: AttestationData, data_2: AttestationData) -> bool:
    """
    Check if ``data_1`` and ``data_2`` are slashable according to Casper FFG rules.
    """
    return (
        # Double vote
            (data_1 != data_2 and data_1.target.epoch == data_2.target.epoch) or
            # Surround vote
            (data_1.source.epoch < data_2.source.epoch and data_2.target.epoch < data_1.target.epoch)
    )


def initiate_validator_exit(epochs_ctx: EpochsContext, state: BeaconState, index: ValidatorIndex) -> None:
    """
    Initiate the exit of the validator with index ``index``.
    """
    # Return if validator already initiated exit
    validator = state.validators[index]
    if validator.exit_epoch != FAR_FUTURE_EPOCH:
        return

    current_epoch = epochs_ctx.current_shuffling.epoch

    # Compute exit queue epoch
    exit_epochs = [v.exit_epoch for v in state.validators if v.exit_epoch != FAR_FUTURE_EPOCH]
    exit_queue_epoch = max(exit_epochs + [compute_activation_exit_epoch(current_epoch)])
    exit_queue_churn = len([v for v in state.validators if v.exit_epoch == exit_queue_epoch])
    if exit_queue_churn >= get_churn_limit(uint64(len(epochs_ctx.current_shuffling.active_indices))):
        exit_queue_epoch += Epoch(1)

    # Set validator exit epoch and withdrawable epoch
    validator.exit_epoch = exit_queue_epoch
    validator.withdrawable_epoch = Epoch(validator.exit_epoch + MIN_VALIDATOR_WITHDRAWABILITY_DELAY)


def slash_validator(epochs_ctx: EpochsContext, state: BeaconState,
                    slashed_index: ValidatorIndex,
                    whistleblower_index: ValidatorIndex=None) -> None:
    """
    Slash the validator with index ``slashed_index``.
    """
    epoch = epochs_ctx.current_shuffling.epoch
    initiate_validator_exit(epochs_ctx, state, slashed_index)
    validator = state.validators[slashed_index]
    validator.slashed = True
    validator.withdrawable_epoch = max(validator.withdrawable_epoch, Epoch(epoch + EPOCHS_PER_SLASHINGS_VECTOR))
    state.slashings[epoch % EPOCHS_PER_SLASHINGS_VECTOR] += validator.effective_balance
    decrease_balance(state, slashed_index, validator.effective_balance // MIN_SLASHING_PENALTY_QUOTIENT)

    # Apply proposer and whistleblower rewards
    proposer_index = epochs_ctx.get_beacon_proposer(state.slot)
    if whistleblower_index is None:
        whistleblower_index = proposer_index
    whistleblower_reward = Gwei(validator.effective_balance // WHISTLEBLOWER_REWARD_QUOTIENT)
    proposer_reward = Gwei(whistleblower_reward // PROPOSER_REWARD_QUOTIENT)
    increase_balance(state, proposer_index, proposer_reward)
    increase_balance(state, whistleblower_index, Gwei(whistleblower_reward - proposer_reward))


def process_proposer_slashing(epochs_ctx: EpochsContext, state: BeaconState, proposer_slashing: ProposerSlashing) -> None:
    header_1 = proposer_slashing.signed_header_1.message
    header_2 = proposer_slashing.signed_header_2.message

    # Verify header slots match
    assert header_1.slot == header_2.slot
    # Verify header proposer indices match
    assert header_1.proposer_index == header_2.proposer_index
    # Verify the headers are different
    assert header_1 != header_2
    # Verify the proposer is slashable
    proposer = state.validators[header_1.proposer_index]
    assert is_slashable_validator(proposer, epochs_ctx.current_shuffling.epoch)
    # Verify signatures
    for signed_header in (proposer_slashing.signed_header_1, proposer_slashing.signed_header_2):
        domain = get_domain(state, DOMAIN_BEACON_PROPOSER, compute_epoch_at_slot(signed_header.message.slot))
        signing_root = compute_signing_root(signed_header.message, domain)
        assert bls_Verify(proposer.pubkey, signing_root, signed_header.signature)

    slash_validator(epochs_ctx, state, header_1.proposer_index)


def process_attester_slashing(epochs_ctx: EpochsContext, state: BeaconState, attester_slashing: AttesterSlashing) -> None:
    attestation_1 = attester_slashing.attestation_1
    attestation_2 = attester_slashing.attestation_2
    assert is_slashable_attestation_data(attestation_1.data, attestation_2.data)
    assert is_valid_indexed_attestation(epochs_ctx, state, attestation_1)
    assert is_valid_indexed_attestation(epochs_ctx, state, attestation_2)

    slashed_any = False
    att_set_1 = set(attestation_1.attesting_indices.readonly_iter())
    att_set_2 = set(attestation_2.attesting_indices.readonly_iter())
    indices = att_set_1.intersection(att_set_2)
    validators = state.validators
    for index in sorted(indices):
        if is_slashable_validator(validators[index], epochs_ctx.current_shuffling.epoch):
            slash_validator(epochs_ctx, state, index)
            slashed_any = True
    assert slashed_any


def is_valid_indexed_attestation(epochs_ctx: EpochsContext, state: BeaconState, indexed_attestation: IndexedAttestation) -> bool:
    """
    Check if ``indexed_attestation`` has sorted and unique indices and a valid aggregate signature.
    """
    # Verify indices are sorted and unique
    indices = list(indexed_attestation.attesting_indices.readonly_iter())
    if len(indices) == 0 or not indices == sorted(set(indices)):
        return False
    # Verify aggregate signature
    pubkeys = [epochs_ctx.index2pubkey[i] for i in indices]
    domain = get_domain(state, DOMAIN_BEACON_ATTESTER, indexed_attestation.data.target.epoch)  # TODO maybe optimize get_domain?
    signing_root = compute_signing_root(indexed_attestation.data, domain)
    return bls_FastAggregateVerify(pubkeys, signing_root, indexed_attestation.signature)


def process_attestation(epochs_ctx: EpochsContext, state: BeaconState, attestation: Attestation) -> None:
    slot = state.slot
    data = attestation.data
    assert data.index < epochs_ctx.get_committee_count_at_slot(data.slot)
    assert data.target.epoch in (epochs_ctx.previous_shuffling.epoch, epochs_ctx.current_shuffling.epoch)
    assert data.target.epoch == compute_epoch_at_slot(data.slot)
    assert data.slot + MIN_ATTESTATION_INCLUSION_DELAY <= slot <= data.slot + SLOTS_PER_EPOCH

    committee = epochs_ctx.get_beacon_committee(data.slot, data.index)
    assert len(attestation.aggregation_bits) == len(committee)

    pending_attestation = PendingAttestation(
        data=data,
        aggregation_bits=attestation.aggregation_bits,
        inclusion_delay=slot - data.slot,
        proposer_index=epochs_ctx.get_beacon_proposer(slot),
    )

    if data.target.epoch == epochs_ctx.current_shuffling.epoch:
        assert data.source == state.current_justified_checkpoint
        state.current_epoch_attestations.append(pending_attestation)
    else:
        assert data.source == state.previous_justified_checkpoint
        state.previous_epoch_attestations.append(pending_attestation)

    # Return the indexed attestation corresponding to ``attestation``.
    def get_indexed_attestation(attestation: Attestation) -> IndexedAttestation:
        bits = list(attestation.aggregation_bits)
        committee = epochs_ctx.get_beacon_committee(data.slot, data.index)
        attesting_indices = set(index for i, index in enumerate(committee) if bits[i])

        return IndexedAttestation(
            attesting_indices=sorted(attesting_indices),
            data=attestation.data,
            signature=attestation.signature,
        )

    # Verify signature
    assert is_valid_indexed_attestation(epochs_ctx, state, get_indexed_attestation(attestation))


def is_valid_merkle_branch(leaf: Bytes32, branch: Sequence[Bytes32], depth: uint64, index: uint64, root: Root) -> bool:
    """
    Check if ``leaf`` at ``index`` verifies against the Merkle ``root`` and ``branch``.
    """
    value = leaf
    for i in range(depth):
        if (index >> i) & 1 == 1:
            value = hash(branch[i] + value)
        else:
            value = hash(value + branch[i])
    return value == root


def process_deposit(epochs_ctx: EpochsContext, state: BeaconState, deposit: Deposit) -> None:
    # Verify the Merkle branch
    assert is_valid_merkle_branch(
        leaf=hash_tree_root(deposit.data),
        branch=deposit.proof,
        depth=DEPOSIT_CONTRACT_TREE_DEPTH + 1,  # Add 1 for the List length mix-in
        index=state.eth1_deposit_index,
        root=state.eth1_data.deposit_root,
    )

    # Deposits must be processed in order
    state.eth1_deposit_index += 1

    pubkey = deposit.data.pubkey
    amount = deposit.data.amount
    if pubkey not in epochs_ctx.pubkey2index:
        # Verify the deposit signature (proof of possession) which is not checked by the deposit contract
        deposit_message = DepositMessage(
            pubkey=deposit.data.pubkey,
            withdrawal_credentials=deposit.data.withdrawal_credentials,
            amount=deposit.data.amount,
        )
        domain = compute_domain(DOMAIN_DEPOSIT)  # Fork-agnostic domain since deposits are valid across forks
        signing_root = compute_signing_root(deposit_message, domain)
        if not bls_Verify(pubkey, signing_root, deposit.data.signature):
            return

        # Add validator and balance entries
        state.validators.append(Validator(
            pubkey=pubkey,
            withdrawal_credentials=deposit.data.withdrawal_credentials,
            activation_eligibility_epoch=FAR_FUTURE_EPOCH,
            activation_epoch=FAR_FUTURE_EPOCH,
            exit_epoch=FAR_FUTURE_EPOCH,
            withdrawable_epoch=FAR_FUTURE_EPOCH,
            effective_balance=min(amount - amount % EFFECTIVE_BALANCE_INCREMENT, MAX_EFFECTIVE_BALANCE),
        ))
        state.balances.append(amount)
    else:
        # Increase balance by deposit amount
        index = ValidatorIndex(epochs_ctx.pubkey2index[pubkey])
        increase_balance(state, index, amount)
    # Now that there is a new validator, update the epoch context with the new pubkey
    epochs_ctx.sync_pubkeys(state)


def process_voluntary_exit(epochs_ctx: EpochsContext, state: BeaconState, signed_voluntary_exit: SignedVoluntaryExit) -> None:
    voluntary_exit = signed_voluntary_exit.message
    validator = state.validators[voluntary_exit.validator_index]
    current_epoch = epochs_ctx.current_shuffling.epoch
    # Verify the validator is active
    assert is_active_validator(validator, current_epoch)
    # Verify exit has not been initiated
    assert validator.exit_epoch == FAR_FUTURE_EPOCH
    # Exits must specify an epoch when they become valid; they are not valid before then
    assert current_epoch >= voluntary_exit.epoch
    # Verify the validator has been active long enough
    assert current_epoch >= validator.activation_epoch + SHARD_COMMITTEE_PERIOD
    # Verify signature
    domain = get_domain(state, DOMAIN_VOLUNTARY_EXIT, voluntary_exit.epoch)
    signing_root = compute_signing_root(voluntary_exit, domain)
    assert bls_Verify(validator.pubkey, signing_root, signed_voluntary_exit.signature)
    # Initiate exit
    # TODO could be optimized, but happens too rarely
    initiate_validator_exit(epochs_ctx, state, voluntary_exit.validator_index)


def process_slot(epochs_ctx: EpochsContext, state: BeaconState) -> None:
    # Cache state root
    previous_state_root = hash_tree_root(state)
    state.state_roots[state.slot % SLOTS_PER_HISTORICAL_ROOT] = previous_state_root
    # Cache latest block header state root
    if state.latest_block_header.state_root == Bytes32():
        state.latest_block_header.state_root = previous_state_root
    # Cache block root
    previous_block_root = hash_tree_root(state.latest_block_header)
    state.block_roots[state.slot % SLOTS_PER_HISTORICAL_ROOT] = previous_block_root


def process_slots(epochs_ctx: EpochsContext, state: BeaconState, slot: Slot) -> None:
    assert state.slot < slot
    while state.slot < slot:
        process_slot(epochs_ctx, state)
        # Process epoch on the start slot of the next epoch
        if (state.slot + 1) % SLOTS_PER_EPOCH == 0:
            process_epoch(epochs_ctx, state)
            state.slot += 1
            epochs_ctx.rotate_epochs(state)
        else:
            state.slot += 1


def process_epoch(epochs_ctx: EpochsContext, state: BeaconState) -> None:
    process = prepare_epoch_process_state(epochs_ctx, state)
    process_justification_and_finalization(epochs_ctx, process, state)
    process_rewards_and_penalties(epochs_ctx, process, state)
    process_registry_updates(epochs_ctx, process, state)
    process_slashings(epochs_ctx, process, state)
    process_final_updates(epochs_ctx, process, state)


def process_block(epochs_ctx: EpochsContext, state: BeaconState, block: BeaconBlock) -> None:
    process_block_header(epochs_ctx, state, block)
    process_randao(epochs_ctx, state, block.body)
    process_eth1_data(epochs_ctx, state, block.body)
    process_operations(epochs_ctx, state, block.body)


def verify_block_signature(state: BeaconState, signed_block: SignedBeaconBlock) -> bool:
    proposer = state.validators[signed_block.message.proposer_index]
    signing_root = compute_signing_root(signed_block.message, get_domain(state, DOMAIN_BEACON_PROPOSER))
    return bls_Verify(proposer.pubkey, signing_root, signed_block.signature)


def state_transition(epochs_ctx: EpochsContext, state: BeaconState,
                     signed_block: SignedBeaconBlock, validate_result: bool = True) -> BeaconState:
    block = signed_block.message
    # Process slots (including those with no blocks) since block
    process_slots(epochs_ctx, state, block.slot)
    # Verify signature
    if validate_result:
        assert verify_block_signature(state, signed_block), "invalid block signature"
    # Process block
    process_block(epochs_ctx, state, block)
    # Verify state root
    if validate_result:
        assert block.state_root == hash_tree_root(state), "invalid block state root"
    # Return post-state
    return state
