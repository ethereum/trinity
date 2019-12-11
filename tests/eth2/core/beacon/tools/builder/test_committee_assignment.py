import pytest

from eth2.beacon.exceptions import NoCommitteeAssignment
from eth2.beacon.helpers import compute_start_slot_at_epoch
from eth2.beacon.tools.builder.committee_assignment import get_committee_assignment


@pytest.mark.parametrize(
    (
        "validator_count,"
        "slots_per_epoch,"
        "target_committee_size,"
        "max_committees_per_slot,"
        "state_epoch,"
        "epoch,"
    ),
    [
        (40, 16, 1, 16, 0, 0),  # genesis
        (40, 16, 1, 16, 1, 1),  # current epoch
        (40, 16, 1, 16, 1, 0),  # previous epoch
        (40, 16, 1, 16, 1, 2),  # next epoch
    ],
)
def test_get_committee_assignment(
    genesis_state,
    slots_per_epoch,
    max_committees_per_slot,
    config,
    validator_count,
    state_epoch,
    epoch,
):
    state_slot = compute_start_slot_at_epoch(state_epoch, slots_per_epoch)
    state = genesis_state.set("slot", state_slot)
    committee_validator_count = [0 for _ in range(max_committees_per_slot)]
    slots = []

    epoch_start_slot = compute_start_slot_at_epoch(epoch, slots_per_epoch)

    for validator_index in range(validator_count):
        assignment = get_committee_assignment(state, config, epoch, validator_index)
        assert assignment.slot >= epoch_start_slot
        assert assignment.slot < epoch_start_slot + slots_per_epoch

        committee_validator_count[assignment.committee_index] += 1
        slots.append(assignment.slot)

    assert sum(committee_validator_count) == validator_count


@pytest.mark.parametrize(
    (
        "validator_count,"
        "slots_per_epoch,"
        "target_committee_size,"
        "max_committees_per_slot,"
    ),
    [(40, 16, 1, 16)],
)
def test_get_committee_assignment_no_assignment(
    genesis_state, genesis_epoch, slots_per_epoch, config
):
    state = genesis_state
    validator_index = 1
    current_epoch = state.current_epoch(slots_per_epoch)
    validator = state.validators[validator_index].set("exit_epoch", genesis_epoch)
    state = state.transform(["validators", validator_index], validator)
    assert not validator.is_active(current_epoch)

    with pytest.raises(NoCommitteeAssignment):
        get_committee_assignment(state, config, current_epoch, validator_index)
