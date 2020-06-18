import pytest

from eth2.beacon.types.historical_batch import HistoricalBatch


@pytest.mark.parametrize(
    (
        "validator_count,"
        "slots_per_epoch,"
        "min_attestation_inclusion_delay,"
        "target_committee_size,"
        "max_committees_per_slot,"
        "state_slot,"
        "slots_per_historical_root"
    ),
    [
        (10, 10, 1, 2, 10, 2, 8192),
        # state.slot == SLOTS_PER_HISTORICAL_ROOT
        (6, 6, 1, 2, 6, 8, 8),
        # state.slot > SLOTS_PER_HISTORICAL_ROOT
        (7, 7, 1, 2, 7, 9, 8),
        # state.slot < SLOTS_PER_HISTORICAL_ROOT
        (7, 7, 1, 2, 7, 7, 8),
        # state.slot % SLOTS_PER_HISTORICAL_ROOT = 0
        (11, 4, 1, 2, 2, 16, 8),
        (16, 4, 1, 2, 4, 32, 8),
        # updated_state.slot == SLOTS_PER_HISTORICAL_ROOT
        (6, 4, 1, 2, 4, 7, 8),
        # updated_state.slot % SLOTS_PER_HISTORICAL_ROOT = 0
        (11, 4, 1, 2, 4, 15, 8),
        (16, 4, 1, 2, 4, 31, 8),
    ],
)
def test_per_slot_transition(
    chaindb_at_genesis,
    genesis_block,
    genesis_state,
    fixture_sm_class,
    config,
    state_slot,
    fork_choice_scoring,
    keymap,
):
    chaindb = chaindb_at_genesis
    sm = fixture_sm_class(chaindb)
    state = genesis_state

    updated_state, _ = sm.apply_state_transition(state, future_slot=state.slot + 1)

    # Ensure that slot gets increased by 1
    assert updated_state.slot == state.slot + 1

    # block_roots
    roots_index = (updated_state.slot - 1) % sm.config.SLOTS_PER_HISTORICAL_ROOT
    assert (
        updated_state.block_roots[roots_index] == genesis_block.message.hash_tree_root
    )

    # state_roots
    assert updated_state.state_roots[roots_index] == state.hash_tree_root

    # historical_roots
    if updated_state.slot % sm.config.SLOTS_PER_HISTORICAL_ROOT == 0:
        historical_batch = HistoricalBatch(
            block_roots=state.block_roots, state_roots=state.state_roots
        )
        assert updated_state.historical_roots[-1] == historical_batch.hash_tree_root
    else:
        assert updated_state.historical_roots == state.historical_roots
