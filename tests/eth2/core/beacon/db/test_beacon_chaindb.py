import random

from eth.constants import GENESIS_PARENT_HASH
from eth.exceptions import BlockNotFound, ParentNotFound
from hypothesis import given
from hypothesis import strategies as st
import pytest

from eth2._utils.hash import hash_eth2
from eth2._utils.ssz import validate_ssz_equal
from eth2.beacon.db.exceptions import (
    AttestationRootNotFound,
    FinalizedHeadNotFound,
    HeadStateSlotNotFound,
    JustifiedHeadNotFound,
)
from eth2.beacon.db.schema import SchemaV1
from eth2.beacon.fork_choice.higher_slot import HigherSlotScore
from eth2.beacon.state_machines.forks.serenity.blocks import (
    BeaconBlock,
    SignedBeaconBlock,
)
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.checkpoints import Checkpoint
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import FromBlockParams


@pytest.fixture(params=[1, 10, 999])
def block(request):
    return SignedBeaconBlock.create(
        message=BeaconBlock.create(parent_root=GENESIS_PARENT_HASH, slot=request.param)
    )


@pytest.fixture()
def state(sample_beacon_state_params):
    return BeaconState.create(**sample_beacon_state_params)


@pytest.fixture()
def block_with_attestation(
    chaindb, genesis_block, sample_attestation_params, fork_choice_scoring
):
    sample_attestation = Attestation.create(**sample_attestation_params)

    chaindb.persist_block(genesis_block, genesis_block.__class__, fork_choice_scoring)
    block1 = SignedBeaconBlock.from_parent(
        genesis_block, FromBlockParams(slot=1)
    ).transform(("message", "body", "attestations"), (sample_attestation,))
    return block1, sample_attestation


@pytest.fixture()
def maximum_score_value():
    return 2 ** 64 - 1


def test_chaindb_add_block_number_to_root_lookup(chaindb, block, fork_choice_scoring):
    block_slot_to_root_key = SchemaV1.make_block_slot_to_root_lookup_key(
        block.message.slot
    )
    assert not chaindb.exists(block_slot_to_root_key)
    chaindb.persist_block(block, block.__class__, fork_choice_scoring)
    assert chaindb.exists(block_slot_to_root_key)


def test_chaindb_persist_block_and_slot_to_root(chaindb, block, fork_choice_scoring):
    with pytest.raises(BlockNotFound):
        chaindb.get_block_by_root(block.message.hash_tree_root, block.__class__)
    slot_to_root_key = SchemaV1.make_block_root_to_score_lookup_key(
        block.message.hash_tree_root
    )
    assert not chaindb.exists(slot_to_root_key)

    chaindb.persist_block(block, block.__class__, fork_choice_scoring)

    assert (
        chaindb.get_block_by_root(block.message.hash_tree_root, block.__class__)
        == block
    )
    assert chaindb.exists(slot_to_root_key)


@given(seed=st.binary(min_size=32, max_size=32))
def test_chaindb_persist_block_and_unknown_parent(
    chaindb, block, fork_choice_scoring, seed
):
    n_block = block.transform(("message", "parent_root"), hash_eth2(seed))
    with pytest.raises(ParentNotFound):
        chaindb.persist_block(n_block, n_block.__class__, fork_choice_scoring)


def test_chaindb_persist_block_and_block_to_root(chaindb, block, fork_choice_scoring):
    block_to_root_key = SchemaV1.make_block_root_to_score_lookup_key(
        block.message.hash_tree_root
    )
    assert not chaindb.exists(block_to_root_key)
    chaindb.persist_block(block, block.__class__, fork_choice_scoring)
    assert chaindb.exists(block_to_root_key)


def test_chaindb_get_score(
    chaindb, fixture_sm_class, genesis_block, fork_choice_scoring
):
    chaindb.persist_block(genesis_block, genesis_block.__class__, fork_choice_scoring)

    genesis_score_key = SchemaV1.make_block_root_to_score_lookup_key(
        genesis_block.message.hash_tree_root
    )
    genesis_score_data = chaindb.db.get(genesis_score_key)
    genesis_score_class = fork_choice_scoring.get_score_class()
    genesis_score = genesis_score_class.deserialize(genesis_score_data)

    expected_genesis_score = fork_choice_scoring.score(genesis_block.message)

    assert genesis_score == expected_genesis_score
    assert (
        chaindb.get_score(genesis_block.message.hash_tree_root, genesis_score_class)
        == expected_genesis_score
    )

    block1 = SignedBeaconBlock.from_parent(genesis_block, FromBlockParams(slot=1))

    chaindb.persist_block(block1, block1.__class__, fork_choice_scoring)

    block1_score_key = SchemaV1.make_block_root_to_score_lookup_key(
        block1.message.hash_tree_root
    )
    block1_score_data = chaindb.db.get(block1_score_key)
    block1_score = genesis_score_class.deserialize(block1_score_data)
    expected_block1_score = fork_choice_scoring.score(block1.message)
    assert block1_score == expected_block1_score
    assert (
        chaindb.get_score(block1.message.hash_tree_root, genesis_score_class)
        == expected_block1_score
    )


def test_chaindb_set_score(chaindb, block, maximum_score_value):
    score_data = random.randrange(0, maximum_score_value)
    # NOTE: using ``HigherSlotScore`` as an integer serializer
    score = HigherSlotScore(score_data)
    chaindb.set_score(block, score)

    block_score = chaindb.get_score(block.message.hash_tree_root, HigherSlotScore)

    assert block_score == score


def test_chaindb_get_block_by_root(chaindb, block, fork_choice_scoring):
    chaindb.persist_block(block, block.__class__, fork_choice_scoring)
    result_block = chaindb.get_block_by_root(
        block.message.hash_tree_root, block.__class__
    )
    validate_ssz_equal(result_block, block)


def test_chaindb_get_canonical_block_root(chaindb, block, fork_choice_scoring):
    chaindb.persist_block(block, block.__class__, fork_choice_scoring)
    block_root = chaindb.get_canonical_block_root(block.slot)
    assert block_root == block.message.hash_tree_root


def test_chaindb_get_genesis_block_root(chaindb, genesis_block, fork_choice_scoring):
    chaindb.persist_block(genesis_block, genesis_block.__class__, fork_choice_scoring)
    block_root = chaindb.get_genesis_block_root()
    assert block_root == genesis_block.message.hash_tree_root


def test_chaindb_get_head_state_slot_and_root(chaindb, state):
    # No head state slot stored in the beginning
    with pytest.raises(HeadStateSlotNotFound):
        chaindb.get_head_state_slot()
    current_slot = state.slot + 10
    current_state = state.set("slot", current_slot)
    chaindb.persist_state(current_state)
    assert chaindb.get_head_state_slot() == current_state.slot
    assert chaindb.get_head_state_root() == current_state.hash_tree_root


def test_chaindb_update_head_slot_and_state_root(chaindb, state):
    chaindb.persist_state(state)
    assert chaindb.get_head_state_slot() == state.slot
    assert chaindb.get_head_state_root() == state.hash_tree_root

    post_state = state.set("slot", state.slot + 1)
    chaindb.update_head_state(post_state.slot, post_state.hash_tree_root)
    assert chaindb.get_head_state_slot() == post_state.slot
    assert chaindb.get_head_state_root() == post_state.hash_tree_root


def test_chaindb_state(chaindb, state):
    chaindb.persist_state(state)
    state_class = BeaconState
    result_state = chaindb.get_state_by_root(state.hash_tree_root, state_class)
    assert result_state.hash_tree_root == state.hash_tree_root


def test_chaindb_get_finalized_head_at_genesis(chaindb_at_genesis, genesis_block):
    assert (
        chaindb_at_genesis.get_finalized_head(genesis_block.__class__) == genesis_block
    )


def test_chaindb_get_justified_head_at_genesis(chaindb_at_genesis, genesis_block):
    assert (
        chaindb_at_genesis.get_justified_head(genesis_block.__class__) == genesis_block
    )


def test_chaindb_get_finalized_head(
    chaindb_at_genesis,
    genesis_block,
    genesis_state,
    sample_beacon_block_params,
    fork_choice_scoring,
):
    chaindb = chaindb_at_genesis
    block = SignedBeaconBlock.from_parent(genesis_block, FromBlockParams(slot=1))

    assert chaindb.get_finalized_head(genesis_block.__class__) == genesis_block
    assert chaindb.get_justified_head(genesis_block.__class__) == genesis_block

    state_with_finalized_block = genesis_state.set(
        "finalized_checkpoint", Checkpoint.create(root=block.message.hash_tree_root)
    )
    chaindb.persist_state(state_with_finalized_block)
    chaindb.persist_block(block, SignedBeaconBlock, fork_choice_scoring)

    assert (
        chaindb.get_finalized_head(SignedBeaconBlock).message.hash_tree_root
        == block.message.hash_tree_root
    )
    assert chaindb.get_justified_head(genesis_block.__class__) == genesis_block


def test_chaindb_get_justified_head(
    chaindb_at_genesis,
    genesis_block,
    genesis_state,
    sample_beacon_block_params,
    fork_choice_scoring,
    config,
):
    chaindb = chaindb_at_genesis
    block = SignedBeaconBlock.from_parent(genesis_block, FromBlockParams(slot=1))

    assert chaindb.get_finalized_head(genesis_block.__class__) == genesis_block
    assert chaindb.get_justified_head(genesis_block.__class__) == genesis_block

    # test that there is only one justified head per epoch
    state_with_bad_epoch = genesis_state.set(
        "current_justified_checkpoint",
        Checkpoint.create(
            root=block.message.hash_tree_root, epoch=config.GENESIS_EPOCH
        ),
    )
    chaindb.persist_state(state_with_bad_epoch)
    chaindb.persist_block(block, SignedBeaconBlock, fork_choice_scoring)

    assert chaindb.get_finalized_head(genesis_block.__class__) == genesis_block
    assert chaindb.get_justified_head(genesis_block.__class__) == genesis_block

    # test that the we can update justified head if we satisfy the invariants
    state_with_justified_block = genesis_state.set(
        "current_justified_checkpoint",
        Checkpoint.create(
            root=block.message.hash_tree_root, epoch=config.GENESIS_EPOCH + 1
        ),
    )
    chaindb.persist_state(state_with_justified_block)

    assert chaindb.get_finalized_head(genesis_block.__class__) == genesis_block
    assert (
        chaindb.get_justified_head(SignedBeaconBlock).message.hash_tree_root
        == block.message.hash_tree_root
    )


def test_chaindb_get_finalized_head_at_init_time(chaindb):
    with pytest.raises(FinalizedHeadNotFound):
        chaindb.get_finalized_head(SignedBeaconBlock)


def test_chaindb_get_justified_head_at_init_time(chaindb):
    with pytest.raises(JustifiedHeadNotFound):
        chaindb.get_justified_head(SignedBeaconBlock)


def test_chaindb_get_canonical_head(chaindb, block, fork_choice_scoring):
    chaindb.persist_block(block, block.__class__, fork_choice_scoring)

    canonical_head_root = chaindb.get_canonical_head_root()
    assert canonical_head_root == block.message.hash_tree_root

    result_block = chaindb.get_canonical_head(block.__class__)
    assert result_block == block

    block_2 = SignedBeaconBlock.from_parent(
        block, FromBlockParams(slot=block.message.slot + 1)
    )
    chaindb.persist_block(block_2, block_2.__class__, fork_choice_scoring)
    result_block = chaindb.get_canonical_head(block.__class__)
    assert result_block == block_2

    block_3 = SignedBeaconBlock.from_parent(
        block_2, FromBlockParams(slot=block_2.message.slot + 1)
    )
    chaindb.persist_block(block_3, block_3.__class__, fork_choice_scoring)
    result_block = chaindb.get_canonical_head(block.__class__)
    assert result_block == block_3


def test_get_slot_by_root(chaindb, block, fork_choice_scoring):
    chaindb.persist_block(block, block.__class__, fork_choice_scoring)
    block_slot = block.slot
    result_slot = chaindb.get_slot_by_root(block.message.hash_tree_root)
    assert result_slot == block_slot


def test_chaindb_add_attestations_root_to_block_lookup(
    chaindb, block_with_attestation, fork_choice_scoring
):
    block, attestation = block_with_attestation
    assert not chaindb.attestation_exists(attestation.hash_tree_root)
    chaindb.persist_block(block, block.__class__, fork_choice_scoring)
    assert chaindb.attestation_exists(attestation.hash_tree_root)


def test_chaindb_get_attestation_key_by_root(
    chaindb, block_with_attestation, fork_choice_scoring
):
    block, attestation = block_with_attestation
    with pytest.raises(AttestationRootNotFound):
        chaindb.get_attestation_key_by_root(attestation.hash_tree_root)
    chaindb.persist_block(block, block.__class__, fork_choice_scoring)
    assert chaindb.get_attestation_key_by_root(attestation.hash_tree_root) == (
        block.message.hash_tree_root,
        0,
    )
