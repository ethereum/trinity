from eth_utils import ValidationError
import pytest

from eth2.beacon.committee_helpers import get_beacon_proposer_index
from eth2.beacon.constants import FAR_FUTURE_EPOCH
from eth2.beacon.helpers import compute_start_slot_at_epoch
from eth2.beacon.state_machines.forks.serenity.blocks import SerenityBeaconBlock
from eth2.beacon.state_machines.forks.serenity.operation_processing import (
    process_attestations,
    process_attester_slashings,
    process_proposer_slashings,
    process_voluntary_exits,
)
from eth2.beacon.tools.builder.validator import (
    create_mock_attester_slashing_is_double_vote,
    create_mock_proposer_slashing_at_block,
    create_mock_signed_attestations_at_slot,
    create_mock_voluntary_exit,
)
from eth2.beacon.types.blocks import BeaconBlockBody
from eth2.configs import CommitteeConfig


@pytest.mark.parametrize(
    (
        "validator_count",
        "slots_per_epoch",
        "target_committee_size",
        "max_committees_per_slot",
        "block_root_1",
        "block_root_2",
        "success",
    ),
    [
        (10, 2, 2, 2, b"\x11" * 32, b"\x22" * 32, True),
        (10, 2, 2, 2, b"\x11" * 32, b"\x11" * 32, False),
    ],
)
def test_process_proposer_slashings(
    genesis_state,
    sample_beacon_block_params,
    sample_beacon_block_body_params,
    config,
    keymap,
    block_root_1,
    block_root_2,
    success,
):
    current_slot = config.GENESIS_SLOT + 1
    state = genesis_state.set("slot", current_slot)
    whistleblower_index = get_beacon_proposer_index(state, CommitteeConfig(config))
    slashing_proposer_index = (whistleblower_index + 1) % len(state.validators)
    proposer_slashing = create_mock_proposer_slashing_at_block(
        state,
        config,
        keymap,
        block_root_1=block_root_1,
        block_root_2=block_root_2,
        proposer_index=slashing_proposer_index,
    )
    proposer_slashings = (proposer_slashing,)

    block_body = BeaconBlockBody.create(**sample_beacon_block_body_params).set(
        "proposer_slashings", proposer_slashings
    )
    block = SerenityBeaconBlock.create(**sample_beacon_block_params).mset(
        "slot", current_slot, "body", block_body
    )

    if success:
        new_state = process_proposer_slashings(state, block, config)
        # Check if slashed
        assert (
            new_state.balances[slashing_proposer_index]
            < state.balances[slashing_proposer_index]
        )
    else:
        with pytest.raises(ValidationError):
            process_proposer_slashings(state, block, config)


@pytest.mark.parametrize(
    (
        "validator_count",
        "slots_per_epoch",
        "target_committee_size",
        "max_committees_per_slot",
        "min_attestation_inclusion_delay",
    ),
    [(100, 2, 2, 2, 1)],
)
@pytest.mark.parametrize(("success"), [(True), (False)])
def test_process_attester_slashings(
    genesis_state,
    sample_beacon_block_params,
    sample_beacon_block_body_params,
    config,
    keymap,
    min_attestation_inclusion_delay,
    success,
):
    attesting_state = genesis_state.mset(
        "slot",
        genesis_state.slot + config.SLOTS_PER_EPOCH,
        "block_roots",
        tuple(
            i.to_bytes(32, "little") for i in range(config.SLOTS_PER_HISTORICAL_ROOT)
        ),
    )
    valid_attester_slashing = create_mock_attester_slashing_is_double_vote(
        attesting_state, config, keymap, attestation_epoch=0
    )
    state = attesting_state.set(
        "slot", attesting_state.slot + min_attestation_inclusion_delay
    )

    if success:
        block_body = BeaconBlockBody.create(**sample_beacon_block_body_params).set(
            "attester_slashings", (valid_attester_slashing,)
        )
        block = SerenityBeaconBlock.create(**sample_beacon_block_params).mset(
            "slot", state.slot, "body", block_body
        )

        attester_index = valid_attester_slashing.attestation_1.attesting_indices[0]

        new_state = process_attester_slashings(state, block, config)
        # Check if slashed
        assert not state.validators[attester_index].slashed
        assert new_state.validators[attester_index].slashed
    else:
        invalid_attester_slashing = valid_attester_slashing.transform(
            ["attestation_2", "data"], valid_attester_slashing.attestation_1.data
        )
        block_body = BeaconBlockBody.create(**sample_beacon_block_body_params).set(
            "attester_slashings", (invalid_attester_slashing,)
        )
        block = SerenityBeaconBlock.create(**sample_beacon_block_params).mset(
            "slot", state.slot, "body", block_body
        )

        with pytest.raises(ValidationError):
            process_attester_slashings(state, block, config)


@pytest.mark.parametrize(
    (
        "validator_count,"
        "slots_per_epoch,"
        "min_attestation_inclusion_delay,"
        "target_committee_size,"
        "max_committees_per_slot,"
        "success,"
    ),
    [(10, 2, 1, 2, 2, True), (10, 2, 1, 2, 2, False), (40, 4, 2, 3, 5, True)],
)
def test_process_attestations(
    genesis_state,
    genesis_block,
    sample_beacon_block_params,
    sample_beacon_block_body_params,
    config,
    keymap,
    fixture_sm_class,
    chaindb,
    genesis_fork_choice_context,
    success,
):

    attestation_slot = 0
    current_slot = attestation_slot + config.MIN_ATTESTATION_INCLUSION_DELAY
    state = genesis_state.set("slot", current_slot)

    attestations = create_mock_signed_attestations_at_slot(
        state=state,
        config=config,
        state_machine=fixture_sm_class(chaindb, genesis_fork_choice_context),
        attestation_slot=attestation_slot,
        beacon_block_root=genesis_block.signing_root,
        keymap=keymap,
        voted_attesters_ratio=1.0,
    )

    assert len(attestations) > 0

    if not success:
        # create invalid attestation
        # i.e. wrong slot
        invalid_attestation_data = attestations[-1].data.set("slot", state.slot + 1)
        invalid_attestation = attestations[-1].set("data", invalid_attestation_data)
        attestations = attestations[:-1] + (invalid_attestation,)

    block_body = BeaconBlockBody.create(**sample_beacon_block_body_params).set(
        "attestations", attestations
    )
    block = SerenityBeaconBlock.create(**sample_beacon_block_params).mset(
        "slot", current_slot, "body", block_body
    )

    if success:
        new_state = process_attestations(state, block, config)

        assert len(new_state.current_epoch_attestations) == len(attestations)
    else:
        with pytest.raises(ValidationError):
            process_attestations(state, block, config)


@pytest.mark.parametrize(
    (
        "validator_count",
        "slots_per_epoch",
        "target_committee_size",
        "max_seed_lookahead",
    ),
    [(40, 2, 2, 2)],
)
@pytest.mark.parametrize(("success",), [(True,), (False,)])
def test_process_voluntary_exits(
    genesis_state,
    sample_beacon_block_params,
    sample_beacon_block_body_params,
    config,
    keymap,
    success,
):
    state = genesis_state.set(
        "slot",
        compute_start_slot_at_epoch(
            config.GENESIS_EPOCH + config.PERSISTENT_COMMITTEE_PERIOD,
            config.SLOTS_PER_EPOCH,
        ),
    )
    validator_index = 0
    validator = state.validators[validator_index].set(
        "activation_epoch", config.GENESIS_EPOCH
    )
    state = state.transform(["validators", validator_index], validator)
    valid_voluntary_exit = create_mock_voluntary_exit(
        state, config, keymap, validator_index
    )

    if success:
        block_body = BeaconBlockBody.create(**sample_beacon_block_body_params).set(
            "voluntary_exits", (valid_voluntary_exit,)
        )
        block = SerenityBeaconBlock.create(**sample_beacon_block_params).mset(
            "slot", state.slot, "body", block_body
        )

        new_state = process_voluntary_exits(state, block, config)
        updated_validator = new_state.validators[validator_index]
        assert updated_validator.exit_epoch != FAR_FUTURE_EPOCH
        assert updated_validator.exit_epoch > state.current_epoch(
            config.SLOTS_PER_EPOCH
        )
        assert updated_validator.withdrawable_epoch == (
            updated_validator.exit_epoch + config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY
        )
    else:
        invalid_voluntary_exit = valid_voluntary_exit.set(
            "signature", b"\x12" * 96  # Put wrong signature
        )
        block_body = BeaconBlockBody.create(**sample_beacon_block_body_params).set(
            "voluntary_exits", (invalid_voluntary_exit,)
        )
        block = SerenityBeaconBlock.create(**sample_beacon_block_params).mset(
            "slot", state.slot, "body", block_body
        )

        with pytest.raises(ValidationError):
            process_voluntary_exits(state, block, config)
