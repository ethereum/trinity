import random

from eth_utils import to_dict
from eth_utils.toolz import (
    curry,
    first,
    keyfilter,
    merge,
    merge_with,
    partition,
    second,
    sliding_window,
)
import pytest

from eth2._utils import bitfield
from eth2.beacon.committee_helpers import (
    get_committee_count_at_slot,
    get_committee_count_per_slot_at_epoch,
    iterate_committees_at_epoch,
    iterate_committees_at_slot,
)
from eth2.beacon.epoch_processing_helpers import get_attesting_indices
from eth2.beacon.fork_choice.lmd_ghost import (
    Context,
    LatestMessage,
    LMDGHOSTScore,
    Store,
    _effective_balance_for_validator,
    score_block_by_root,
)
from eth2.beacon.helpers import compute_epoch_at_slot, compute_start_slot_at_epoch
from eth2.beacon.types.attestation_data import AttestationData
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.blocks import BeaconBlock, SignedBeaconBlock
from eth2.beacon.types.checkpoints import Checkpoint


def test_score_serialization():
    score = LMDGHOSTScore((2 ** 20, int.from_bytes(b"3" * 32, byteorder="big")))
    serialized_score = score.serialize()
    another_score = LMDGHOSTScore.deserialize(serialized_score)
    assert score == another_score


# TODO(ralexstokes) merge this and next into tools/builder
@to_dict
def _mk_attestation_inputs_in_epoch(epoch, block_producer, state, config):
    for committee, committee_index, slot in iterate_committees_at_epoch(
        state, epoch, config
    ):
        if not committee:
            # empty committee this slot
            continue

        block = block_producer(slot)
        root = block.message.hash_tree_root
        attestation_data = AttestationData.create(
            slot=slot,
            index=committee_index,
            target=Checkpoint.create(epoch=epoch, root=root),
            beacon_block_root=root,
        )
        committee_size = len(committee)
        aggregation_bits = bitfield.get_empty_bitfield(committee_size)
        for index in range(committee_size):
            aggregation_bits = bitfield.set_voted(aggregation_bits, index)
            for index in committee:
                yield (
                    index,
                    (attestation_data.slot, (aggregation_bits, attestation_data)),
                )


def _mk_attestations_for_epoch_by_count(
    number_of_committee_samples, epoch, block_producer, state, config
):
    results = {}
    for _ in range(number_of_committee_samples):
        sample = _mk_attestation_inputs_in_epoch(epoch, block_producer, state, config)
        results = merge(results, sample)
    return results


def _extract_attestations_from_index_keying(values):
    results = ()
    for value in values:
        aggregation_bits, data = second(value)
        attestation = Attestation.create(aggregation_bits=aggregation_bits, data=data)
        if attestation not in results:
            results += (attestation,)
    return results


def _keep_by_latest_slot(values):
    """
    we get a sequence of (Slot, (Bitfield, AttestationData))
    and return the AttestationData with the highest slot
    """
    return max(values, key=first)[1][1]


def _find_collision(state, config, validator_index, epoch, block_producer):
    """
    Given a target epoch, make the attestation expected for the
    validator w/ the given ``validator_index``.
    """
    for committee, committee_index, slot in iterate_committees_at_epoch(
        state, epoch, config
    ):
        if validator_index in committee:
            # TODO(ralexstokes) refactor w/ tools/builder
            block = block_producer(slot)
            root = block.message.hash_tree_root
            attestation_data = AttestationData.create(
                slot=slot,
                index=committee_index,
                target=Checkpoint.create(epoch=epoch, root=root),
                beacon_block_root=root,
            )
            committee_count = len(committee)
            aggregation_bits = bitfield.get_empty_bitfield(committee_count)
            for i in range(committee_count):
                aggregation_bits = bitfield.set_voted(aggregation_bits, i)

            return {
                index: (slot, (aggregation_bits, attestation_data))
                for index in committee
            }
    else:
        raise Exception("should have found a duplicate validator")


def _introduce_collisions(all_attestations_by_index, block_producer, state, config):
    """
    Find some attestations for later epochs for the validators
    that are current attesting in each source of attestation.
    """
    collisions = (all_attestations_by_index[0],)
    for src, dst in sliding_window(2, all_attestations_by_index):
        if not src:
            # src can be empty at low validator count
            collisions += (dst,)
            continue
        src_index = random.choice(list(src.keys()))
        src_val = src[src_index]
        src_slot, _ = src_val
        src_epoch = compute_epoch_at_slot(src_slot, config.SLOTS_PER_EPOCH)
        dst_epoch = src_epoch + 1

        collision = _find_collision(
            state,
            config,
            validator_index=src_index,
            epoch=dst_epoch,
            block_producer=block_producer,
        )
        collisions += (merge(dst, collision),)
    return collisions


def _get_committee_count(state, epoch, config):
    return (
        get_committee_count_per_slot_at_epoch(
            state,
            epoch,
            config.MAX_COMMITTEES_PER_SLOT,
            config.SLOTS_PER_EPOCH,
            config.TARGET_COMMITTEE_SIZE,
        )
        * config.SLOTS_PER_EPOCH
    )


def _compute_seconds_since_genesis_for_epoch(epoch, config):
    return epoch * config.SLOTS_PER_EPOCH * config.SECONDS_PER_SLOT


block_producer_cache = {}


@curry
def _mk_block_at_slot(block_template, slot):
    if slot in block_producer_cache:
        return block_producer_cache[slot]
    else:
        block = block_template.transform(["message", "slot"], slot)
        block_producer_cache[slot] = block
        return block


@pytest.mark.parametrize(
    ("validator_count",),
    [
        (8,),  # low number of validators
        (64,),  # medium number of validators
        # # NOTE: tests at higher validator counts takes too long :(
    ],
)
@pytest.mark.parametrize(("collisions_from_another_epoch",), [(True,), (False,)])
def test_store_get_latest_attestation(
    genesis_state, genesis_block, config, collisions_from_another_epoch
):
    """
    Given some attestations across the various sources, can we
    find the latest ones for each validator?
    """
    some_epoch = 3
    state = genesis_state.set(
        "slot", compute_start_slot_at_epoch(some_epoch, config.SLOTS_PER_EPOCH)
    )
    some_time = (
        _compute_seconds_since_genesis_for_epoch(some_epoch, config)
        + state.genesis_time
    )
    previous_epoch = state.previous_epoch(config.SLOTS_PER_EPOCH, config.GENESIS_EPOCH)
    previous_epoch_committee_count = _get_committee_count(state, previous_epoch, config)

    current_epoch = state.current_epoch(config.SLOTS_PER_EPOCH)
    current_epoch_committee_count = _get_committee_count(state, current_epoch, config)
    number_of_committee_samples = 4

    assert number_of_committee_samples <= previous_epoch_committee_count
    assert number_of_committee_samples <= current_epoch_committee_count

    block_producer = _mk_block_at_slot(genesis_block)

    # prepare samples from previous epoch
    previous_epoch_attestations_by_index = _mk_attestations_for_epoch_by_count(
        number_of_committee_samples, previous_epoch, block_producer, state, config
    )
    previous_epoch_attestations = _extract_attestations_from_index_keying(
        previous_epoch_attestations_by_index.values()
    )

    # prepare samples from current epoch
    current_epoch_attestations_by_index = _mk_attestations_for_epoch_by_count(
        number_of_committee_samples, current_epoch, block_producer, state, config
    )
    current_epoch_attestations_by_index = keyfilter(
        lambda index: index not in previous_epoch_attestations_by_index,
        current_epoch_attestations_by_index,
    )
    current_epoch_attestations = _extract_attestations_from_index_keying(
        current_epoch_attestations_by_index.values()
    )

    pool_attestations_by_index = _mk_attestations_for_epoch_by_count(
        number_of_committee_samples, current_epoch, block_producer, state, config
    )
    pool_attestations_by_index = keyfilter(
        lambda index: (
            index not in previous_epoch_attestations_by_index
            or index not in current_epoch_attestations_by_index
        ),
        pool_attestations_by_index,
    )
    pool_attestations = _extract_attestations_from_index_keying(
        pool_attestations_by_index.values()
    )

    all_attestations_by_index = (
        previous_epoch_attestations_by_index,
        current_epoch_attestations_by_index,
        pool_attestations_by_index,
    )

    if collisions_from_another_epoch:
        (
            previous_epoch_attestations_by_index,
            current_epoch_attestations_by_index,
            pool_attestations_by_index,
        ) = _introduce_collisions(
            all_attestations_by_index, block_producer, state, config
        )

        previous_epoch_attestations = _extract_attestations_from_index_keying(
            previous_epoch_attestations_by_index.values()
        )
        current_epoch_attestations = _extract_attestations_from_index_keying(
            current_epoch_attestations_by_index.values()
        )
        pool_attestations = _extract_attestations_from_index_keying(
            pool_attestations_by_index.values()
        )

    # build expected results
    expected_index = merge_with(
        _keep_by_latest_slot,
        previous_epoch_attestations_by_index,
        current_epoch_attestations_by_index,
        pool_attestations_by_index,
    )

    chain_db = None  # not relevant for this test
    context = Context.from_genesis(genesis_state, genesis_block)
    context.time = some_time
    store = Store(chain_db, SignedBeaconBlock, config, context)

    for attestations in (
        previous_epoch_attestations,
        current_epoch_attestations,
        pool_attestations,
    ):
        for attestation in attestations:
            # NOTE: we need to synchronize the context w/ chain data used to construct
            # attestations above; this synchronization takes advantage of some of the
            # internals of ``on_attestation`` to shortcut constructing the complete network
            # state needed to test the function of the ``Store``.
            block = block_producer(attestation.data.slot)
            context.blocks[block.message.hash_tree_root] = block
            context.block_states[block.message.hash_tree_root] = genesis_state

            store.on_attestation(attestation, validate_signature=False)

    # sanity check
    assert expected_index.keys() == store._context.latest_messages.keys()

    for validator_index in range(len(state.validators)):
        expected_attestation_data = expected_index.get(validator_index, None)
        target = expected_attestation_data.target
        expected_message = LatestMessage(epoch=target.epoch, root=target.root)
        stored_message = store._context.latest_messages.get(validator_index, None)
        assert expected_message == stored_message


def _mk_block(block_params, slot, parent, block_offset):
    block = BeaconBlock.create(**block_params).mset(
        "slot",
        slot,
        "parent_root",
        parent.message.hash_tree_root,
        # mix in something unique
        "state_root",
        block_offset.to_bytes(32, byteorder="big"),
    )
    return SignedBeaconBlock.create(message=block)


def _build_block_tree(
    block_params, root_block, base_slot, forking_descriptor, forking_asymmetry, config
):
    """
    build a block tree according to the data in ``forking_descriptor``, starting at
    the block with root ``base_root``.
    """
    tree = [[root_block]]
    for slot_offset, block_count in enumerate(forking_descriptor):
        slot = base_slot + slot_offset
        blocks = []
        for parent in tree[-1]:
            if forking_asymmetry:
                if random.choice([True, False]):
                    continue
            for block_offset in range(block_count):
                block = _mk_block(block_params, slot, parent, block_offset)
            blocks.append(block)
        tree.append(blocks)
    # other code written w/ expectation that root is not in the tree
    tree.pop(0)
    return tree


def _iter_block_tree_by_slot(tree):
    for level in tree:
        yield level


def _iter_block_level_by_block(level):
    for block in level:
        yield block


def _iter_block_tree_by_block(tree):
    for level in _iter_block_tree_by_slot(tree):
        for block in _iter_block_level_by_block(level):
            yield block


def _get_committees(state, target_slot, config, sampling_fraction):
    committees_per_slot = get_committee_count_at_slot(
        state,
        target_slot,
        config.MAX_COMMITTEES_PER_SLOT,
        config.SLOTS_PER_EPOCH,
        config.TARGET_COMMITTEE_SIZE,
    )
    committees_at_slot = ()
    for committee, _, _ in iterate_committees_at_slot(
        state, target_slot, committees_per_slot, config
    ):
        committees_at_slot += (committee,)
    return tuple(
        random.sample(
            committees_at_slot, int((sampling_fraction * committees_per_slot))
        )
    )


def _attach_committee_to_block(block, committee_and_index):
    block._committee_data = committee_and_index


def _get_committee_from_block(block):
    return getattr(block, "_committee_data", None)


def _attach_attestation_to_block(block, attestation):
    block._attestation = attestation


def _get_attestation_from_block(block):
    return getattr(block, "_attestation", None)


def _attach_committees_to_block_tree(
    state, block_tree, committees_by_slot, config, forking_asymmetry
):
    for level, committees in zip(
        _iter_block_tree_by_slot(block_tree), committees_by_slot
    ):
        block_count = len(level)
        partitions = partition(block_count, committees)
        for committee_index, (block, committee) in enumerate(
            zip(_iter_block_level_by_block(level), partitions)
        ):
            if forking_asymmetry:
                if random.choice([True, False]):
                    # random drop out
                    continue
            _attach_committee_to_block(block, (first(committee), committee_index))


# TODO(ralexstokes) merge in w/ tools/builder
def _mk_attestation_for_block_with_committee(block, committee, committee_index, config):
    committee_count = len(committee)
    aggregation_bits = bitfield.get_empty_bitfield(committee_count)
    for index in range(committee_count):
        aggregation_bits = bitfield.set_voted(aggregation_bits, index)

    attestation = Attestation.create(
        aggregation_bits=aggregation_bits,
        data=AttestationData.create(
            slot=block.slot,
            index=committee_index,
            beacon_block_root=block.message.hash_tree_root,
            target=Checkpoint.create(
                epoch=compute_epoch_at_slot(block.slot, config.SLOTS_PER_EPOCH)
            ),
        ),
    )
    return attestation


def _attach_attestations_to_block_tree_with_committees(block_tree, config):
    for block in _iter_block_tree_by_block(block_tree):
        committee_data = _get_committee_from_block(block)
        if not committee_data:
            # w/ asymmetry in forking we may need to skip this step
            continue
        committee, committee_index = committee_data
        attestation = _mk_attestation_for_block_with_committee(
            block, committee, committee_index, config
        )
        _attach_attestation_to_block(block, attestation)


def _score_block(block, store, state, config):
    return LMDGHOSTScore(
        (
            sum(
                _effective_balance_for_validator(state, validator_index)
                for validator_index, target in store._context.latest_messages
                if store.get_ancestor(target.root, block.slot) == block
            ),
            score_block_by_root(block.message.hash_tree_root),
        )
    )


def _build_score_index_from_decorated_block_tree(block_tree, store, state, config):
    return {
        block.message.hash_tree_root: _score_block(block, store, state, config)
        for block in _iter_block_tree_by_block(block_tree)
    }


def _iter_attestation_by_validator_index(state, attestation, config):
    for index in get_attesting_indices(
        state, attestation.data, attestation.aggregation_bits, config
    ):
        yield index


@pytest.mark.parametrize(
    ("validator_count",),
    [
        (8,),  # low number of validators
        (128,),  # medium number of validators
        (1024,),  # high number of validators
    ],
)
@pytest.mark.parametrize(
    (
        # controls how many children a parent has
        "forking_descriptor",
    ),
    [
        ((1,),),
        ((2,),),
        ((3,),),
        ((1, 1),),
        ((2, 1),),
        ((3, 2),),
        ((1, 4),),
        ((1, 2, 1),),
    ],
)
@pytest.mark.parametrize(
    (
        # controls how children should be allocated to a given parent
        "forking_asymmetry",
    ),
    [
        # Asymmetry means we may deviate from the description in ``forking_descriptor``.
        (True,),
        # No asymmetry means every parent has
        # the number of children prescribed in ``forking_descriptor``.
        # => randomly drop some blocks from receiving attestations
        (False,),
    ],
)
def test_lmd_ghost_fork_choice_scoring(
    sample_beacon_block_params,
    chaindb_at_genesis,
    # see note below on how this is used
    fork_choice_scoring,
    forking_descriptor,
    forking_asymmetry,
    genesis_state,
    genesis_block,
    empty_attestation_pool,
    config,
):
    """
    Given some blocks and some attestations, can we score them correctly?
    """
    chain_db = chaindb_at_genesis
    root_block = chain_db.get_canonical_head(SignedBeaconBlock)

    some_epoch = 3
    some_slot_offset = 10

    state = genesis_state.mset(
        "slot",
        compute_start_slot_at_epoch(some_epoch, config.SLOTS_PER_EPOCH)
        + some_slot_offset,
        "current_justified_checkpoint",
        Checkpoint.create(epoch=some_epoch, root=root_block.message.hash_tree_root),
    )
    assert some_epoch >= state.current_justified_checkpoint.epoch

    # NOTE: the attestations have to be aligned to the blocks which start from ``base_slot``.
    base_slot = compute_start_slot_at_epoch(some_epoch, config.SLOTS_PER_EPOCH) + 1
    block_tree = _build_block_tree(
        sample_beacon_block_params,
        root_block,
        base_slot,
        forking_descriptor,
        forking_asymmetry,
        config,
    )

    slot_count = len(forking_descriptor)
    committee_sampling_fraction = 1
    committees_by_slot = tuple(
        _get_committees(
            state, base_slot + slot_offset, config, committee_sampling_fraction
        )
        for slot_offset in range(slot_count)
    )

    _attach_committees_to_block_tree(
        state, block_tree, committees_by_slot, config, forking_asymmetry
    )

    _attach_attestations_to_block_tree_with_committees(block_tree, config)

    attestations = tuple(
        _get_attestation_from_block(block)
        for block in _iter_block_tree_by_block(block_tree)
        if _get_attestation_from_block(block)
    )

    attestation_pool = empty_attestation_pool
    for attestation in attestations:
        attestation_pool.add(attestation)

    context = Context.from_genesis(genesis_state, genesis_block)
    store = Store(chain_db, SignedBeaconBlock, config, context)

    score_index = _build_score_index_from_decorated_block_tree(
        block_tree, store, state, config
    )

    for block in _iter_block_tree_by_block(block_tree):
        # NOTE: we use the ``fork_choice_scoring`` fixture, it doesn't matter for this test
        chain_db.persist_block(block, SignedBeaconBlock, fork_choice_scoring)

    for block in _iter_block_tree_by_block(block_tree):
        score = store.scoring(block)
        expected_score = score_index[block.message.hash_tree_root]
        assert score == expected_score
