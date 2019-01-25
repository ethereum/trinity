import pytest

from eth.constants import (
    ZERO_HASH32,
)

from eth2._utils.tuple import (
    update_tuple_item,
)
from eth2.beacon.helpers import (
    get_current_epoch_committee_count_per_slot,
)
from eth2.beacon.types.crosslink_records import CrosslinkRecord
from eth2.beacon.state_machines.forks.serenity.epoch_processing import (
    _check_if_update_validator_registry,
    process_final_updates,
    process_validator_registry,
)


@pytest.mark.parametrize(
    (
        'num_validators, epoch_length, target_committee_size, shard_count, state_slot,'
        'validator_registry_update_slot,'
        'finalized_slot,'
        'has_crosslink,'
        'crosslink_slot,'
        'expected_need_to_update,'
    ),
    [
        # state.finalized_slot <= state.validator_registry_update_slot
        (
            40, 4, 2, 2, 16,
            4, 4, False, 0, False
        ),
        # state.latest_crosslinks[shard].slot <= state.validator_registry_update_slot
        (
            40, 4, 2, 2, 16,
            4, 8, True, 0, False,
        ),
        # state.finalized_slot > state.validator_registry_update_slot and
        # state.latest_crosslinks[shard].slot > state.validator_registry_update_slot
        (
            40, 4, 2, 2, 16,
            4, 8, True, 8, True,
        ),
    ]
)
def test_check_if_update_validator_registry(genesis_state,
                                            state_slot,
                                            validator_registry_update_slot,
                                            finalized_slot,
                                            has_crosslink,
                                            crosslink_slot,
                                            expected_need_to_update,
                                            config):
    state = genesis_state.copy(
        slot=state_slot,
        finalized_slot=finalized_slot,
        validator_registry_update_slot=validator_registry_update_slot,
    )
    if has_crosslink:
        crosslink = CrosslinkRecord(
            slot=crosslink_slot,
            shard_block_root=ZERO_HASH32,
        )
        latest_crosslinks = state.latest_crosslinks
        for shard in range(config.SHARD_COUNT):
            latest_crosslinks = update_tuple_item(
                latest_crosslinks,
                shard,
                crosslink,
            )
        state = state.copy(
            latest_crosslinks=latest_crosslinks,
        )

    need_to_update, num_shards_in_committees = _check_if_update_validator_registry(state, config)

    assert need_to_update == expected_need_to_update
    if expected_need_to_update:
        expected_num_shards_in_committees = get_current_epoch_committee_count_per_slot(
            state,
            shard_count=config.SHARD_COUNT,
            epoch_length=config.EPOCH_LENGTH,
            target_committee_size=config.TARGET_COMMITTEE_SIZE,
        ) * config.EPOCH_LENGTH
        assert num_shards_in_committees == expected_num_shards_in_committees


@pytest.mark.parametrize(
    (
        'num_validators,'
        'state_slot,'
        'epoch_length,'
        'need_to_update,'
        'num_shards_in_committees,'
    ),
    [
        (10, 0, 5, True, 10),
    ]
)
def test_process_validator_registry(monkeypatch,
                                    genesis_state,
                                    state_slot,
                                    need_to_update,
                                    num_shards_in_committees,
                                    config):
    from eth2.beacon.state_machines.forks.serenity import epoch_processing

    def mock_check_if_update_validator_registry(state, config):
        return need_to_update, num_shards_in_committees

    monkeypatch.setattr(
        epoch_processing,
        '_check_if_update_validator_registry',
        mock_check_if_update_validator_registry
    )

    state = genesis_state.copy(
        slot=state_slot,
    )

    result_state = process_validator_registry(state, config)
    assert result_state.previous_epoch_calculation_slot == state.current_epoch_start_shard
    assert result_state.previous_epoch_start_shard == state.current_epoch_start_shard
    assert result_state.previous_epoch_randao_mix == state.current_epoch_randao_mix

    if need_to_update:
        assert result_state.current_epoch_calculation_slot == state_slot
        # TODO
    else:
        # TODO
        pass


@pytest.mark.parametrize(
    (
        'num_validators,'
        'state_slot,'
        'epoch_length'
    ),
    [
        (10, 0, 5),
        (10, 5, 5),
        (10, 6, 5),
    ]
)
def test_process_final_updates(genesis_state,
                               state_slot,
                               config):
    state = genesis_state.copy(
        slot=state_slot,
    )

    result_state = process_final_updates(state, config)

    # TODO: test latest_penalized_balances after we add `penalize_validator`

    for attestation in result_state.latest_attestations:
        assert attestation.data.slot >= state_slot - config.EPOCH_LENGTH
