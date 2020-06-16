from eth_utils import ValidationError

from eth2._utils.bls import bls
from eth2.beacon.exceptions import SignatureError
from eth2.beacon.helpers import compute_signing_root, get_domain
from eth2.beacon.signature_domain import SignatureDomain
from eth2.beacon.types.attestation_data import AttestationData
from eth2.beacon.types.attestations import IndexedAttestation
from eth2.beacon.types.states import BeaconState


def validate_indexed_attestation_aggregate_signature(
    state: BeaconState, indexed_attestation: IndexedAttestation, slots_per_epoch: int
) -> None:
    public_keys = tuple(
        state.validators[i].pubkey for i in indexed_attestation.attesting_indices
    )
    domain = get_domain(
        state,
        SignatureDomain.DOMAIN_BEACON_ATTESTER,
        slots_per_epoch,
        indexed_attestation.data.target.epoch,
    )
    signing_root = compute_signing_root(indexed_attestation.data, domain)
    bls.validate(signing_root, indexed_attestation.signature, *public_keys)


def validate_indexed_attestation(
    state: BeaconState,
    indexed_attestation: IndexedAttestation,
    slots_per_epoch: int,
    validate_signature: bool = True,
) -> None:
    """
    Derived from spec: `is_valid_indexed_attestation`.

    Option ``validate_signature`` is used in some testing scenarios, like some fork choice tests.
    """
    attesting_indices = indexed_attestation.attesting_indices

    if list(attesting_indices) != sorted(set(attesting_indices)):
        raise ValidationError(
            f"Indices should be sorted; the attesting indices are not: {attesting_indices}."
        )

    if validate_signature:
        try:
            validate_indexed_attestation_aggregate_signature(
                state, indexed_attestation, slots_per_epoch
            )
        except SignatureError as error:
            raise ValidationError(
                f"Incorrect aggregate signature on the {indexed_attestation}", error
            )


def is_slashable_attestation_data(
    data_1: AttestationData, data_2: AttestationData
) -> bool:
    """
    Check if ``data_1`` and ``data_2`` are slashable according to Casper FFG rules.
    """
    # NOTE: checking 'double vote' OR 'surround vote'
    return (data_1 != data_2 and data_1.target.epoch == data_2.target.epoch) or (
        data_1.source.epoch < data_2.source.epoch
        and data_2.target.epoch < data_1.target.epoch
    )
