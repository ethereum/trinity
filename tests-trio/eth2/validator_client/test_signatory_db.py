import pytest

from eth2.beacon.types.attestations import Attestation, AttestationData
from eth2.beacon.types.blocks import BeaconBlock
from eth2.clock import Tick
from eth2.validator_client.duty import AttestationDuty, BlockProposalDuty
from eth2.validator_client.signatory_db import InMemorySignatoryDB


def _mk_resolved_attestation_duty(slot, public_key):
    return (
        AttestationDuty(
            public_key, Tick(0, slot, 0, 1), Tick(0, 0, 0, 1), committee_index=22
        ),
        Attestation.create(data=AttestationData.create(slot=slot)),
    )


@pytest.mark.trio
@pytest.mark.parametrize(
    ("slots_to_make_duties_at," "is_slashable,"), [((1, 2), False), ((2, 2), True)]
)
async def test_signatory_db_attestation_slashing_conditions(
    slots_to_make_duties_at, is_slashable, sample_bls_public_key
):
    """
    NOTE: these are not the actual attestation slashing conditions
    DO _NOT_ USE IN PRODUCTION.

    This test will be updated w/ the appropratie test cases once the attestation slashing conditions
    are installed in the validator client.
    """

    first_resolved_duty, second_resolved_duty = tuple(
        _mk_resolved_attestation_duty(slot, sample_bls_public_key)
        for slot in slots_to_make_duties_at
    )
    db = InMemorySignatoryDB()

    first_duty, first_operation = first_resolved_duty
    assert not await db.is_slashable(first_duty, first_operation)
    await db.record_signature_for(first_duty, first_operation)
    # NOTE: once we have recorded the first duty, it is slashable wrt the db
    assert await db.is_slashable(first_duty, first_operation)

    second_duty, second_operation = second_resolved_duty
    is_slashable_in_db = await db.is_slashable(second_duty, second_operation)
    assert is_slashable_in_db == is_slashable


def _mk_resolved_block_proposal_duty(slot, public_key):
    return (
        BlockProposalDuty(public_key, Tick(0, slot, 0, 0), Tick(0, 0, 0, 0)),
        BeaconBlock.create(slot=slot),
    )


@pytest.mark.trio
@pytest.mark.parametrize(
    ("slots_to_make_duties_at," "is_slashable,"), [((1, 2), False), ((2, 2), True)]
)
async def test_signatory_db_block_proposal_slashing_conditions(
    slots_to_make_duties_at, is_slashable, sample_bls_public_key
):
    first_resolved_duty, second_resolved_duty = tuple(
        _mk_resolved_block_proposal_duty(slot, sample_bls_public_key)
        for slot in slots_to_make_duties_at
    )
    db = InMemorySignatoryDB()

    first_duty, first_operation = first_resolved_duty
    assert not await db.is_slashable(first_duty, first_operation)
    await db.record_signature_for(first_duty, first_operation)
    # NOTE: once we have recorded the first duty, it is slashable wrt the db
    assert await db.is_slashable(first_duty, first_operation)

    second_duty, second_operation = second_resolved_duty
    is_slashable_in_db = await db.is_slashable(second_duty, second_operation)
    assert is_slashable_in_db == is_slashable
