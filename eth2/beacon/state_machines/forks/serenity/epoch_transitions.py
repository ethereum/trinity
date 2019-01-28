from typing import (
    Tuple,
    Sequence,
)
from eth2.beacon.typing import (
    Ether,
    Gwei,
    EpochNumber,
)
from eth2.beacon.types.validator_records import ValidatorRecord
from eth2.beacon.types.states import BeaconState
from eth2.beacon.state_machines.configs import BeaconConfig
from eth2.beacon.helpers import (
    get_effective_balance,
    get_active_validator_indices,
    get_attestation_participants,
    get_block_root,
    total_balance,
    get_current_epoch,
    slot_to_epoch,
    get_epoch_start_slot,
)


def get_epoch_boundary_attesting_balances(
        current_epoch,
        previous_epoch,
        state: BeaconState,
        config: BeaconConfig) -> Tuple[Gwei, Gwei]:
    """
    Return attesting balances for previous epoch boundary and current epoch boundary.
    They are sum of balances, of unique validators, who have sent attestations
    satisfying these constraints:

    Previous epoch boundary attestations:
        - slot in latest 2 epochs, and
        - justified_epoch is previous_justified_epoch, and
        - epoch_boundary_root is exactly 2 epoch ago

    Current epoch boundary attestations:
        - slot in latest 1 epoch, and
        - justified_epoch is justified_epoch, and
        - epoch_boundary_root is exactly 1 epoch ago
    """

    EPOCH_LENGTH = config.EPOCH_LENGTH
    MAX_DEPOSIT = config.MAX_DEPOSIT
    LATEST_BLOCK_ROOTS_LENGTH = config.LATEST_BLOCK_ROOTS_LENGTH
    SHARD_COUNT = config.SHARD_COUNT
    TARGET_COMMITTEE_SIZE = config.TARGET_COMMITTEE_SIZE

    current_epoch_attestations = tuple(
        attestation
        for attestation in state.latest_attestations
        if current_epoch == slot_to_epoch(attestation.data.slot, EPOCH_LENGTH)
    )
    previous_epoch_attestations = tuple(
        attestation
        for attestation in state.latest_attestations
        if previous_epoch <= slot_to_epoch(attestation.data.slot, EPOCH_LENGTH)
    )

    previous_justified_epoch = state.previous_justified_epoch

    previous_epoch_justified_attestations = tuple(
        attestation
        for attestation in current_epoch_attestations + previous_epoch_attestations
        if attestation.justified_epoch == previous_justified_epoch
    )

    previous_epoch_boundary_root = get_block_root(
        state,
        get_epoch_start_slot(previous_epoch, EPOCH_LENGTH),
        LATEST_BLOCK_ROOTS_LENGTH,
    )
    previous_epoch_boundary_attestations = tuple(
        attestation
        for attestation in previous_epoch_justified_attestations
        if attestation.epoch_boundary_root == previous_epoch_boundary_root
    )

    sets_of_previous_epoch_boundary_participants = tuple(
        frozenset(get_attestation_participants(
            state,
            attestation.data,
            attestation.participation_bitfield,
            EPOCH_LENGTH,
            TARGET_COMMITTEE_SIZE,
            SHARD_COUNT,
        ))
        for attestation in previous_epoch_boundary_attestations
    )

    previous_epoch_boundary_attester_indices = frozenset.union(
        frozenset(),
        *sets_of_previous_epoch_boundary_participants,
    )

    previous_epoch_boundary_attesting_balance = Gwei(sum(
        get_effective_balance(state, index, MAX_DEPOSIT)
        for index in previous_epoch_boundary_attester_indices
    ))

    current_epoch_start_slot = get_epoch_start_slot(current_epoch, EPOCH_LENGTH)
    current_epoch_boundary_root = get_block_root(
        state,
        current_epoch_start_slot,
        LATEST_BLOCK_ROOTS_LENGTH,
    )

    justified_epoch = state.justified_epoch
    current_epoch_boundary_attestations = tuple(
        attestation
        for attestation in current_epoch_attestations
        if attestation.epoch_boundary_root == current_epoch_boundary_root and
        attestation.data.justified_epoch == justified_epoch
    )

    sets_of_current_epoch_boundary_participants = tuple(
        frozenset(get_attestation_participants(
            state,
            attestation.data,
            attestation.participation_bitfield,
            EPOCH_LENGTH,
            TARGET_COMMITTEE_SIZE,
            SHARD_COUNT,
        ))
        for attestation in current_epoch_boundary_attestations
    )

    current_epoch_boundary_attester_indices = frozenset.union(
        frozenset(),
        *sets_of_current_epoch_boundary_participants,
    )

    current_epoch_boundary_attesting_balance = Gwei(sum(
        get_effective_balance(state, index, MAX_DEPOSIT)
        for index in current_epoch_boundary_attester_indices
    ))
    return previous_epoch_boundary_attesting_balance, current_epoch_boundary_attesting_balance


def get_total_balance(
        validator_registry: Sequence[ValidatorRecord],
        validator_balances: Sequence[Gwei],
        epoch: EpochNumber,
        max_deposit: Ether) -> Gwei:

    active_validator_indices = get_active_validator_indices(validator_registry, epoch)

    return total_balance(active_validator_indices, validator_balances, max_deposit)


def process_justification(state: BeaconState, config: BeaconConfig) -> BeaconState:
    EPOCH_LENGTH = config.EPOCH_LENGTH

    current_epoch = get_current_epoch(state, EPOCH_LENGTH)
    previous_epoch = current_epoch - 1 if current_epoch > config.GENESIS_EPOCH else current_epoch

    current_total_balance = get_total_balance(
        state.validator_registry,
        state.validator_balances,
        current_epoch,
        config.MAX_DEPOSIT,
    )
    previous_total_balance = get_total_balance(
        state.validator_registry,
        state.validator_balances,
        previous_epoch,
        config.MAX_DEPOSIT,
    )
    (
        previous_epoch_boundary_attesting_balance,
        current_epoch_boundary_attesting_balance
    ) = get_epoch_boundary_attesting_balances(current_epoch, previous_epoch, state, config)

    new_justified_epoch = state.justified_epoch
    justification_bitfield = state.justification_bitfield << 1

    previous_epoch_justifiable = (
        3 * current_epoch_boundary_attesting_balance >= 2 * previous_total_balance
    )
    current_epoch_justifiable = (
        3 * previous_epoch_boundary_attesting_balance >= 2 * current_total_balance
    )

    # TODO: refactor this
    if previous_epoch_justifiable:
        justification_bitfield |= 2
        new_justified_epoch = previous_epoch

    if current_epoch_justifiable:
        justification_bitfield |= 1
        new_justified_epoch = current_epoch

    # TODO: refactor this
    if (
        (justification_bitfield >> 1) % 8 == 0b111 and
        state.previous_justified_epoch == previous_epoch - 2
    ):
        finalized_epoch = state.previous_justified_epoch
    elif (
        (justification_bitfield >> 1) % 4 == 0b11 and
        state.previous_justified_epoch == previous_epoch - 1
    ):
        finalized_epoch = state.previous_justified_epoch
    elif (
        (justification_bitfield >> 0) % 8 == 0b111 and
        state.justified_epoch == previous_epoch - 1
    ):
        finalized_epoch = state.justified_epoch
    elif (
        (justification_bitfield >> 0) % 4 == 0b11 and
        state.justified_epoch == previous_epoch
    ):
        finalized_epoch = state.justified_epoch
    else:
        finalized_epoch = state.finalized_epoch

    return state.copy(
        previous_justified_epoch=state.justified_epoch,
        justified_epoch=new_justified_epoch,
        justification_bitfield=justification_bitfield,
        finalized_epoch=finalized_epoch,
    )
