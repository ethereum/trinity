import pytest

from eth2._utils.bitfield import get_empty_bitfield, set_voted
from eth2.beacon.committee_helpers import (
    compute_epoch_at_slot,
    iterate_committees_at_epoch,
)
from eth2.beacon.signature_domain import SignatureDomain
from eth2.beacon.tools.builder.aggregator import (
    TARGET_AGGREGATORS_PER_COMMITTEE,
    get_aggregate_from_valid_committee_attestations,
    get_slot_signature,
    is_aggregator,
)
from eth2.beacon.tools.builder.validator import sign_transaction
from eth2.beacon.types.attestations import Attestation
from eth2.configs import CommitteeConfig


@pytest.mark.slow
@pytest.mark.parametrize(
    ("validator_count", "target_committee_size", "slots_per_epoch"), [(1000, 100, 10)]
)
def test_aggregator_selection(validator_count, privkeys, genesis_state, config):
    config = CommitteeConfig(config)
    state = genesis_state
    epoch = compute_epoch_at_slot(state.slot, config.SLOTS_PER_EPOCH)

    sum_aggregator_count = 0
    for committee, committee_index, slot in iterate_committees_at_epoch(
        state, epoch, config
    ):
        assert config.TARGET_COMMITTEE_SIZE == len(committee)
        aggregator_count = 0
        for index in range(validator_count):
            if index in committee:
                signature = get_slot_signature(
                    genesis_state, slot, privkeys[index], config
                )
                attester_is_aggregator = is_aggregator(
                    state, slot, committee_index, signature, config
                )
                if attester_is_aggregator:
                    aggregator_count += 1
        assert aggregator_count > 0
        sum_aggregator_count += aggregator_count
    # The average aggregator count per slot should be around
    # `TARGET_AGGREGATORS_PER_COMMITTEE`.
    average_aggregator_count = sum_aggregator_count / config.SLOTS_PER_EPOCH
    assert (
        TARGET_AGGREGATORS_PER_COMMITTEE - 3
        < average_aggregator_count
        < TARGET_AGGREGATORS_PER_COMMITTEE + 3
    )


def test_get_aggregate_from_valid_committee_attestations(
    sample_attestation_params, privkeys, genesis_state, config
):
    committee_size = 16
    empty_bitfield = get_empty_bitfield(committee_size)
    base_attestation = Attestation.create(**sample_attestation_params)
    message_hash = base_attestation.data.hash_tree_root
    attestations = []
    expected_bitfield = empty_bitfield

    for i in range(4, 16, 2):
        attestations.append(
            base_attestation.mset(
                "aggregation_bits",
                set_voted(empty_bitfield, i),
                "signature",
                sign_transaction(
                    message_hash=message_hash,
                    privkey=privkeys[i],
                    state=genesis_state,
                    slot=genesis_state.slot,
                    signature_domain=SignatureDomain.DOMAIN_BEACON_ATTESTER,
                    slots_per_epoch=config.SLOTS_PER_EPOCH,
                ),
            )
        )
        expected_bitfield = set_voted(expected_bitfield, i)

    aggregate_attestation = get_aggregate_from_valid_committee_attestations(
        attestations
    )

    assert aggregate_attestation.aggregation_bits == expected_bitfield
