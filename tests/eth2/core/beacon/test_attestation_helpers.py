import copy
import random

from eth_utils import ValidationError
from eth_utils.toolz import assoc
import pytest

from eth2._utils.bls import bls
from eth2.beacon.attestation_helpers import (
    is_slashable_attestation_data,
    validate_indexed_attestation,
    validate_indexed_attestation_aggregate_signature,
)
from eth2.beacon.helpers import get_domain
from eth2.beacon.signature_domain import SignatureDomain
from eth2.beacon.types.attestation_data import AttestationData
from eth2.beacon.types.attestations import IndexedAttestation
from eth2.beacon.types.forks import Fork


@pytest.mark.parametrize(("validator_count",), [(40,)])
def test_verify_indexed_attestation_signature(
    slots_per_epoch,
    validator_count,
    genesis_state,
    config,
    privkeys,
    sample_beacon_state_params,
    genesis_validators,
    genesis_balances,
    sample_indexed_attestation_params,
    sample_fork_params,
):
    state = genesis_state.set("fork", Fork.create(**sample_fork_params))

    # NOTE: we can do this before "correcting" the params as they
    # touch disjoint subsets of the provided params
    message_hash = _create_indexed_attestation_messages(
        sample_indexed_attestation_params
    )

    valid_params = _correct_indexed_attestation_params(
        validator_count,
        message_hash,
        sample_indexed_attestation_params,
        privkeys,
        state,
        config,
    )
    valid_votes = IndexedAttestation.create(**valid_params)

    validate_indexed_attestation_aggregate_signature(
        state, valid_votes, slots_per_epoch
    )

    invalid_params = _corrupt_signature(slots_per_epoch, valid_params, state.fork)
    invalid_votes = IndexedAttestation.create(**invalid_params)
    with pytest.raises(ValidationError):
        validate_indexed_attestation_aggregate_signature(
            state, invalid_votes, slots_per_epoch
        )


def _get_indices_and_signatures(validator_count, state, config, message_hash, privkeys):
    num_indices = 5
    assert validator_count >= num_indices
    indices = random.sample(range(validator_count), num_indices)
    indices.sort()

    privkeys = [privkeys[i] for i in indices]
    signature_domain = SignatureDomain.DOMAIN_BEACON_ATTESTER
    domain = get_domain(
        state=state,
        signature_domain=signature_domain,
        slots_per_epoch=config.SLOTS_PER_EPOCH,
    )
    signatures = tuple(map(lambda key: bls.sign(message_hash, key, domain), privkeys))
    return (indices, signatures)


def _run_verify_indexed_vote(
    slots_per_epoch, params, state, max_validators_per_committee, should_succeed
):
    votes = IndexedAttestation.create(**params)
    if should_succeed:
        validate_indexed_attestation(
            state, votes, max_validators_per_committee, slots_per_epoch
        )
    else:
        with pytest.raises(ValidationError):
            validate_indexed_attestation(
                state, votes, max_validators_per_committee, slots_per_epoch
            )


def _correct_indexed_attestation_params(
    validator_count, message_hash, params, privkeys, state, config
):
    valid_params = copy.deepcopy(params)

    (attesting_indices, signatures) = _get_indices_and_signatures(
        validator_count, state, config, message_hash, privkeys
    )

    valid_params["attesting_indices"] = attesting_indices

    signature = bls.aggregate_signatures(signatures)

    valid_params["signature"] = signature

    return valid_params


def _corrupt_attesting_indices(params):
    corrupt_attesting_indices = (
        params["attesting_indices"][1],
        params["attesting_indices"][0],
    ) + tuple(params["attesting_indices"][2:])

    return assoc(params, "attesting_indices", corrupt_attesting_indices)


def _corrupt_attesting_indices_max(max_validators_per_committee, params):
    corrupt_attesting_indices = [i for i in range(max_validators_per_committee + 1)]
    return assoc(params, "attesting_indices", corrupt_attesting_indices)


def _corrupt_signature(slots_per_epoch, params, fork):
    return assoc(params, "signature", b"\x12" * 96)


def _create_indexed_attestation_messages(params):
    attestation = IndexedAttestation.create(**params)
    return attestation.data.hash_tree_root


@pytest.mark.parametrize(("validator_count",), [(40,)])
@pytest.mark.parametrize(
    ("param_mapper", "should_succeed", "needs_fork", "is_testing_max_length"),
    [
        (lambda params: params, True, False, False),
        (_corrupt_attesting_indices, False, False, False),
        (_corrupt_signature, False, True, False),
    ],
)
def test_validate_indexed_attestation(
    slots_per_epoch,
    validator_count,
    genesis_state,
    param_mapper,
    should_succeed,
    needs_fork,
    is_testing_max_length,
    privkeys,
    sample_beacon_state_params,
    genesis_validators,
    genesis_balances,
    sample_indexed_attestation_params,
    sample_fork_params,
    max_validators_per_committee,
    config,
):
    state = genesis_state.set("fork", Fork.create(**sample_fork_params))

    # NOTE: we can do this before "correcting" the params as they
    # touch disjoint subsets of the provided params
    message_hash = _create_indexed_attestation_messages(
        sample_indexed_attestation_params
    )

    params = _correct_indexed_attestation_params(
        validator_count,
        message_hash,
        sample_indexed_attestation_params,
        privkeys,
        state,
        config,
    )
    if needs_fork:
        params = param_mapper(slots_per_epoch, params, state.fork)
    elif is_testing_max_length:
        params = param_mapper(max_validators_per_committee, params)

    else:
        params = param_mapper(params)
    _run_verify_indexed_vote(
        slots_per_epoch, params, state, max_validators_per_committee, should_succeed
    )


@pytest.mark.parametrize(("validator_count",), [(40,)])
def test_verify_indexed_attestation_after_fork(
    genesis_state,
    slots_per_epoch,
    validator_count,
    privkeys,
    sample_beacon_state_params,
    genesis_validators,
    genesis_balances,
    sample_indexed_attestation_params,
    sample_fork_params,
    config,
    max_validators_per_committee,
):
    # Test that indexed data is still valid after fork
    # Indexed data slot = 10, fork slot = 15, current slot = 20
    past_fork_params = {
        "previous_version": (0).to_bytes(4, "little"),
        "current_version": (1).to_bytes(4, "little"),
        "epoch": 15,
    }

    state = genesis_state.mset("slot", 20, "fork", Fork.create(**past_fork_params))

    message_hash = _create_indexed_attestation_messages(
        sample_indexed_attestation_params
    )

    valid_params = _correct_indexed_attestation_params(
        validator_count,
        message_hash,
        sample_indexed_attestation_params,
        privkeys,
        state,
        config,
    )
    _run_verify_indexed_vote(
        slots_per_epoch, valid_params, state, max_validators_per_committee, True
    )


@pytest.mark.parametrize(
    ("is_double_vote," "is_surround_vote,"),
    [(False, False), (False, True), (True, False), (True, True)],
)
def test_is_slashable_attestation_data(
    sample_attestation_data_params, is_double_vote, is_surround_vote
):
    data_1 = AttestationData.create(**sample_attestation_data_params)
    data_2 = AttestationData.create(**sample_attestation_data_params)

    if is_double_vote:
        data_2 = data_2.set(
            "beacon_block_root",
            (int.from_bytes(data_1.beacon_block_root, "little") + 1).to_bytes(
                32, "little"
            ),
        )

    if is_surround_vote:
        data_1 = data_1.transform(
            ("source", "epoch"), data_2.source.epoch - 1
        ).transform(("target", "epoch"), data_2.target.epoch + 1)

    assert is_slashable_attestation_data(data_1, data_2) == (
        is_double_vote or is_surround_vote
    )
