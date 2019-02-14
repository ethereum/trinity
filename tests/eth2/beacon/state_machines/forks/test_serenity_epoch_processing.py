import pytest

from hypothesis import (
    given,
    settings,
    strategies as st,
)

from eth._utils.numeric import (
    int_to_bytes32,
)

from eth.constants import (
    ZERO_HASH32,
)

from eth2._utils.tuple import (
    update_tuple_item,
)
from eth2._utils.bitfield import (
    set_voted,
    get_empty_bitfield,
)
from eth2.beacon.committee_helpers import (
    get_crosslink_committees_at_slot,
    get_current_epoch_committee_count,
)
from eth2.beacon.configs import (
    CommitteeConfig,
)
from eth2.beacon.helpers import (
    get_active_validator_indices,
    get_randao_mix,
    slot_to_epoch,
)
from eth2.beacon._utils.hash import (
    hash_eth2,
)
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.attestation_data import AttestationData
from eth2.beacon.types.crosslink_records import CrosslinkRecord
from eth2.beacon.state_machines.forks.serenity.epoch_processing import (
    _check_if_update_validator_registry,
    _update_latest_index_roots,
    process_crosslinks,
    process_final_updates,
    process_validator_registry,
)


@pytest.mark.parametrize(
    (
        'num_validators, epoch_length, target_committee_size, shard_count, state_slot,'
        'validator_registry_update_epoch,'
        'finalized_epoch,'
        'has_crosslink,'
        'crosslink_epoch,'
        'expected_need_to_update,'
    ),
    [
        # state.finalized_epoch <= state.validator_registry_update_epoch
        (
            40, 4, 2, 2, 16,
            4, 4, False, 0, False
        ),
        # state.latest_crosslinks[shard].epoch <= state.validator_registry_update_epoch
        (
            40, 4, 2, 2, 16,
            4, 8, True, 4, False,
        ),
        # state.finalized_epoch > state.validator_registry_update_epoch and
        # state.latest_crosslinks[shard].epoch > state.validator_registry_update_epoch
        (
            40, 4, 2, 2, 16,
            4, 8, True, 6, True,
        ),
    ]
)
def test_check_if_update_validator_registry(genesis_state,
                                            state_slot,
                                            validator_registry_update_epoch,
                                            finalized_epoch,
                                            has_crosslink,
                                            crosslink_epoch,
                                            expected_need_to_update,
                                            config):
    state = genesis_state.copy(
        slot=state_slot,
        finalized_epoch=finalized_epoch,
        validator_registry_update_epoch=validator_registry_update_epoch,
    )
    if has_crosslink:
        crosslink = CrosslinkRecord(
            epoch=crosslink_epoch,
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
        expected_num_shards_in_committees = get_current_epoch_committee_count(
            state,
            shard_count=config.SHARD_COUNT,
            epoch_length=config.EPOCH_LENGTH,
            target_committee_size=config.TARGET_COMMITTEE_SIZE,
        )
        assert num_shards_in_committees == expected_num_shards_in_committees
    else:
        assert num_shards_in_committees == 0


@pytest.mark.parametrize(
    (
        'epoch_length,'
        'latest_index_roots_length,'
        'state_slot,'
    ),
    [
        (4, 16, 4),
        (4, 16, 64),
    ]
)
def test_update_latest_index_roots(genesis_state,
                                   committee_config,
                                   state_slot,
                                   epoch_length,
                                   latest_index_roots_length,
                                   entry_exit_delay):
    state = genesis_state.copy(
        slot=state_slot,
    )

    result_state = _update_latest_index_roots(state, committee_config)

    # TODO: chanege to hash_tree_root
    index_root = hash_eth2(
        b''.join(
            [
                index.to_bytes(32, 'big')
                for index in get_active_validator_indices(
                    state.validator_registry,
                    # TODO: change to `per-epoch` version
                    slot_to_epoch(state.slot, epoch_length),
                )
            ]
        )
    )

    assert result_state.latest_index_roots[
        (state.next_epoch(epoch_length) + entry_exit_delay) % latest_index_roots_length
    ] == index_root


@pytest.mark.parametrize(
    (
        'num_validators, epoch_length, target_committee_size, shard_count,'
        'latest_randao_mixes_length, seed_lookahead, state_slot,'
        'need_to_update,'
        'num_shards_in_committees,'
        'validator_registry_update_epoch,'
        'epochs_since_last_registry_change_is_power_of_two,'
        'current_calculation_epoch,'
        'latest_randao_mixes,'
        'expected_current_calculation_epoch,'
    ),
    [
        (
            40, 4, 2, 2,
            2**10, 4, 19,
            False,
            10,
            2,
            True,  # (state.current_epoch - state.validator_registry_update_epoch) is power of two
            0,
            [int_to_bytes32(i) for i in range(2**10)],
            5,  # expected current_calculation_epoch is state.next_epoch
        ),
        (
            40, 4, 2, 2,
            2**10, 4, 19,
            False,
            10,
            1,
            False,  # (state.current_epoch - state.validator_registry_update_epoch) != power of two
            0,
            [int_to_bytes32(i) for i in range(2**10)],
            0,  # expected_current_calculation_epoch is current_calculation_epoch because it will not be updated  # noqa: E501
        ),
    ]
)
def test_process_validator_registry(monkeypatch,
                                    genesis_state,
                                    epoch_length,
                                    state_slot,
                                    need_to_update,
                                    num_shards_in_committees,
                                    validator_registry_update_epoch,
                                    epochs_since_last_registry_change_is_power_of_two,
                                    current_calculation_epoch,
                                    latest_randao_mixes,
                                    expected_current_calculation_epoch,
                                    entry_exit_delay,
                                    config):
    # Mock check_if_update_validator_registry
    from eth2.beacon.state_machines.forks.serenity import epoch_processing

    def mock_check_if_update_validator_registry(state, config):
        return need_to_update, num_shards_in_committees

    monkeypatch.setattr(
        epoch_processing,
        '_check_if_update_validator_registry',
        mock_check_if_update_validator_registry
    )

    # Mock generate_seed
    new_seed = b'\x88' * 32

    def mock_generate_seed(state,
                           epoch,
                           epoch_length,
                           seed_lookahead,
                           entry_exit_delay,
                           latest_index_roots_length,
                           latest_randao_mixes_length):
        return new_seed

    monkeypatch.setattr(
        'eth2.beacon.helpers.generate_seed',
        mock_generate_seed
    )

    # Set state
    state = genesis_state.copy(
        slot=state_slot,
        validator_registry_update_epoch=validator_registry_update_epoch,
        current_calculation_epoch=current_calculation_epoch,
        latest_randao_mixes=latest_randao_mixes,
    )

    result_state = process_validator_registry(state, config)

    assert result_state.previous_calculation_epoch == state.current_calculation_epoch
    assert result_state.previous_epoch_start_shard == state.current_epoch_start_shard
    assert result_state.previous_epoch_seed == state.current_epoch_seed

    if need_to_update:
        assert result_state.current_calculation_epoch == slot_to_epoch(state_slot, epoch_length)
        assert result_state.current_epoch_seed == new_seed
        # TODO: Add test for validator registry updates
    else:
        assert (
            result_state.current_calculation_epoch ==
            expected_current_calculation_epoch
        )
        # state.current_epoch_start_shard is left unchanged.
        assert result_state.current_epoch_start_shard == state.current_epoch_start_shard

        if epochs_since_last_registry_change_is_power_of_two:
            assert result_state.current_epoch_seed == new_seed
        else:
            assert result_state.current_epoch_seed != new_seed


@pytest.mark.parametrize(
    (
        'num_validators,'
        'state_slot,'
        'attestation_slot,'
        'len_latest_attestations,'
        'expected_result_len_latest_attestations,'
        'epoch_length'
    ),
    [
        (10, 4, 4, 2, 2, 4),  # slot_to_epoch(attestation.data.slot) >= state.current_epoch, -> expected_result_len_latest_attestations = len_latest_attestations  # noqa: E501
        (10, 4, 8, 2, 2, 4),  # slot_to_epoch(attestation.data.slot) >= state.current_epoch, -> expected_result_len_latest_attestations = len_latest_attestations  # noqa: E501
        (10, 16, 8, 2, 0, 4),  # slot_to_epoch(attestation.data.slot) < state.current_epoch, -> expected_result_len_latest_attestations = 0  # noqa: E501
    ]
)
def test_process_final_updates(genesis_state,
                               state_slot,
                               attestation_slot,
                               len_latest_attestations,
                               expected_result_len_latest_attestations,
                               config,
                               sample_attestation_params):
    state = genesis_state.copy(
        slot=state_slot,
    )
    current_index = state.next_epoch(config.EPOCH_LENGTH) % config.LATEST_PENALIZED_EXIT_LENGTH
    previous_index = state.current_epoch(config.EPOCH_LENGTH) % config.LATEST_PENALIZED_EXIT_LENGTH

    # Assume `len_latest_attestations` attestations in state.latest_attestations
    # with attestation.data.slot = attestation_slot
    attestation = Attestation(**sample_attestation_params)
    latest_attestations = [
        attestation.copy(
            data=attestation.data.copy(
                slot=attestation_slot
            )
        )
        for i in range(len_latest_attestations)
    ]

    # Fill latest_penalized_balances
    penalized_balance_of_previous_epoch = 100
    latest_penalized_balances = update_tuple_item(
        state.latest_penalized_balances,
        previous_index,
        penalized_balance_of_previous_epoch,
    )
    state = state.copy(
        latest_penalized_balances=latest_penalized_balances,
        latest_attestations=latest_attestations,
    )

    result_state = process_final_updates(state, config)

    assert (
        (
            result_state.latest_penalized_balances[current_index] ==
            penalized_balance_of_previous_epoch
        ) and (
            result_state.latest_randao_mixes[current_index] == get_randao_mix(
                state=state,
                epoch=state.current_epoch(config.EPOCH_LENGTH),
                epoch_length=config.EPOCH_LENGTH,
                latest_randao_mixes_length=config.LATEST_RANDAO_MIXES_LENGTH,
            )
        )
    )

    assert len(result_state.latest_attestations) == expected_result_len_latest_attestations
    for attestation in result_state.latest_attestations:
        assert attestation.data.slot >= state_slot - config.EPOCH_LENGTH


@settings(max_examples=1)
@given(random=st.randoms())
@pytest.mark.parametrize(
    (
        'n,'
        'epoch_length,'
        'target_committee_size,'
        'shard_count,'
        'success_crosslink_in_cur_epoch,'
    ),
    [
        (
            90,
            10,
            9,
            10,
            False,
        ),
        (
            90,
            10,
            9,
            10,
            True,
        ),
    ]
)
def test_process_crosslinks(
        random,
        n_validators_state,
        config,
        epoch_length,
        target_committee_size,
        shard_count,
        success_crosslink_in_cur_epoch,
        sample_attestation_data_params,
        sample_attestation_params):
    shard = 1
    shard_block_root = hash_eth2(b'shard_block_root')
    current_slot = config.EPOCH_LENGTH * 2 - 1

    initial_crosslinks = tuple([
        CrosslinkRecord(epoch=config.GENESIS_EPOCH, shard_block_root=ZERO_HASH32)
        for _ in range(shard_count)
    ])
    state = n_validators_state.copy(
        slot=current_slot,
        latest_crosslinks=initial_crosslinks,
    )

    # Generate current epoch attestations
    cur_epoch_attestations = []
    for slot_in_cur_epoch in range(state.slot - config.EPOCH_LENGTH, state.slot):
        if len(cur_epoch_attestations) > 0:
            break
        for committee, _shard in get_crosslink_committees_at_slot(
            state,
            slot_in_cur_epoch,
            CommitteeConfig(config),
        ):
            if _shard == shard:
                # Sample validators attesting to this shard.
                # Number of attesting validators sampled depends on `success_crosslink_in_cur_epoch`
                # if True, have >2/3 committee attest
                if success_crosslink_in_cur_epoch:
                    attesting_validators = random.sample(committee, (2 * len(committee) // 3 + 1))
                else:
                    attesting_validators = random.sample(committee, (2 * len(committee) // 3 - 1))
                # Generate the bitfield
                aggregation_bitfield = get_empty_bitfield(len(committee))
                for v_index in attesting_validators:
                    aggregation_bitfield = set_voted(
                        aggregation_bitfield, committee.index(v_index))
                # Generate the attestation
                cur_epoch_attestations.append(
                    Attestation(**sample_attestation_params).copy(
                        data=AttestationData(**sample_attestation_data_params).copy(
                            slot=slot_in_cur_epoch,
                            shard=shard,
                            shard_block_root=shard_block_root,
                        ),
                        aggregation_bitfield=aggregation_bitfield,
                    )
                )

    state = state.copy(
        latest_attestations=cur_epoch_attestations,
    )
    assert (state.latest_crosslinks[shard].epoch == config.GENESIS_EPOCH and
            state.latest_crosslinks[shard].shard_block_root == ZERO_HASH32)

    new_state = process_crosslinks(state, config)
    crosslink_record = new_state.latest_crosslinks[shard]
    if success_crosslink_in_cur_epoch:
        attestation = cur_epoch_attestations[0]
        assert (crosslink_record.epoch == slot_to_epoch(current_slot, epoch_length) and
                crosslink_record.shard_block_root == attestation.data.shard_block_root and
                attestation.data.shard_block_root == shard_block_root)
    else:
        assert (crosslink_record.epoch == config.GENESIS_EPOCH and
                crosslink_record.shard_block_root == ZERO_HASH32)
