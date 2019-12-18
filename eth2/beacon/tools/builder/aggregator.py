from typing import Sequence

from eth_typing import BLSSignature
from eth_utils import ValidationError
from ssz import get_hash_tree_root, uint64

from eth2._utils.bls import bls
from eth2._utils.hash import hash_eth2
from eth2.beacon.attestation_helpers import (
    validate_indexed_attestation_aggregate_signature,
)
from eth2.beacon.committee_helpers import get_beacon_committee
from eth2.beacon.epoch_processing_helpers import (
    get_attesting_indices,
    get_indexed_attestation,
)
from eth2.beacon.helpers import compute_epoch_at_slot, get_domain
from eth2.beacon.signature_domain import SignatureDomain
from eth2.beacon.types.aggregate_and_proof import AggregateAndProof
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Bitfield, CommitteeIndex, Slot
from eth2.configs import CommitteeConfig

# TODO: TARGET_AGGREGATORS_PER_COMMITTEE is not in Eth2Config now.
TARGET_AGGREGATORS_PER_COMMITTEE = 16


def get_slot_signature(
    state: BeaconState, slot: Slot, privkey: int, config: CommitteeConfig
) -> BLSSignature:
    """
    Sign on ``slot`` and return the signature.
    """
    domain = get_domain(
        state,
        SignatureDomain.DOMAIN_BEACON_ATTESTER,
        config.SLOTS_PER_EPOCH,
        message_epoch=compute_epoch_at_slot(slot, config.SLOTS_PER_EPOCH),
    )
    return bls.sign(get_hash_tree_root(slot, sedes=uint64), privkey, domain)


def is_aggregator(
    state: BeaconState,
    slot: Slot,
    index: CommitteeIndex,
    signature: BLSSignature,
    config: CommitteeConfig,
) -> bool:
    """
    Check if the validator is one of the aggregators of the given ``slot``.

      .. note::
        - Probabilistically, with enought validators, the aggregator count should
        approach ``TARGET_AGGREGATORS_PER_COMMITTEE``.

        - With ``len(committee)`` is 128 and ``TARGET_AGGREGATORS_PER_COMMITTEE`` is 16,
        the expected length of selected validators is 16.

        - It's possible that this algorithm selects *no one* as the aggregator, but with the
        above parameters, the chance of having no aggregator has a probability of 3.78E-08.

        - Chart analysis: https://docs.google.com/spreadsheets/d/1C7pBqEWJgzk3_jesLkqJoDTnjZOODnGTOJUrxUMdxMA  # noqa: E501
    """
    committee = get_beacon_committee(state, slot, index, config)
    modulo = max(1, len(committee) // TARGET_AGGREGATORS_PER_COMMITTEE)
    return int.from_bytes(hash_eth2(signature)[0:8], byteorder="little") % modulo == 0


def get_aggregate_from_valid_committee_attestations(
    attestations: Sequence[Attestation]
) -> Attestation:
    """
    Return the aggregate attestation.

    The given attestations SHOULD have the same `data: AttestationData` and are valid.
    """
    signatures = [attestation.signature for attestation in attestations]
    aggregate_signature = bls.aggregate_signatures(signatures)

    all_aggregation_bits = [
        attestation.aggregation_bits for attestation in attestations
    ]
    aggregation_bits = tuple(map(any, zip(*all_aggregation_bits)))

    assert len(attestations) > 0

    return Attestation.create(
        data=attestations[0].data,
        aggregation_bits=Bitfield(aggregation_bits),
        signature=aggregate_signature,
    )


#
# Validation
#


def validate_aggregate_and_proof(
    state: BeaconState,
    aggregate_and_proof: AggregateAndProof,
    attestation_propagation_slot_range: int,
    config: CommitteeConfig,
) -> None:
    """
    Validate aggregate_and_proof

    Reference: https://github.com/ethereum/eth2.0-specs/blob/master/specs/networking/p2p-interface.md#global-topics  # noqa: E501
    """
    attestation = aggregate_and_proof.aggregate

    validate_attestation_propagation_slot_range(
        state, attestation, attestation_propagation_slot_range
    )

    attesting_indices = get_attesting_indices(
        state, attestation.data, attestation.aggregation_bits, config
    )
    if aggregate_and_proof.aggregator_index not in attesting_indices:
        raise ValidationError(
            f"The aggregator index ({aggregate_and_proof.aggregator_index}) is not within"
            f" the aggregate's committee {attesting_indices}"
        )

    if not is_aggregator(
        state,
        attestation.data.slot,
        attestation.data.index,
        aggregate_and_proof.selection_proof,
        config,
    ):
        raise ValidationError(
            f"The given validator {aggregate_and_proof.aggregator_index}"
            " is not a selected aggregator"
        )

    validate_aggregator_proof(state, aggregate_and_proof, config)

    validate_attestation_signature(state, attestation, config)


def validate_attestation_propagation_slot_range(
    state: BeaconState,
    attestation: Attestation,
    attestation_propagation_slot_range: int,
) -> None:
    if (
        attestation.data.slot + attestation_propagation_slot_range < state.slot
        or attestation.data.slot > state.slot
    ):
        raise ValidationError(
            "attestation.data.slot should be within the last"
            " {attestation_propagation_slot_range} slots. Got"
            f" attestationdata.slot={attestation.data.slot},"
            f" current slot={state.slot}"
        )


def validate_aggregator_proof(
    state: BeaconState, aggregate_and_proof: AggregateAndProof, config: CommitteeConfig
) -> None:
    slot = aggregate_and_proof.aggregate.data.slot
    pubkey = state.validators[aggregate_and_proof.aggregator_index].pubkey
    domain = get_domain(
        state,
        SignatureDomain.DOMAIN_BEACON_ATTESTER,
        config.SLOTS_PER_EPOCH,
        message_epoch=compute_epoch_at_slot(slot, config.SLOTS_PER_EPOCH),
    )
    message_hash = get_hash_tree_root(slot, sedes=uint64)

    bls.validate(
        message_hash=message_hash,
        pubkey=pubkey,
        signature=aggregate_and_proof.selection_proof,
        domain=domain,
    )


def validate_attestation_signature(
    state: BeaconState, attestation: Attestation, config: CommitteeConfig
) -> None:
    indexed_attestation = get_indexed_attestation(state, attestation, config)
    validate_indexed_attestation_aggregate_signature(
        state, indexed_attestation, config.SLOTS_PER_EPOCH
    )
