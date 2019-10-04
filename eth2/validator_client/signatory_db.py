from enum import Enum, unique
import logging
from typing import Dict

import ssz

from eth2.beacon.typing import Operation
from eth2.validator_client.abc import SignatoryDatabaseAPI
from eth2.validator_client.duty import Duty, DutyType


@unique
class OperationTag(Enum):
    Attestation = b"\x00"
    BlockProposal = b"\x01"


def _key_for_attestation(duty: Duty) -> bytes:
    return (
        OperationTag.Attestation.value
        + duty.validator_public_key
        + ssz.encode(duty.tick_for_execution.slot, ssz.sedes.uint64)
    )


async def _record_attestation(
    duty: Duty, operation: Operation, db: SignatoryDatabaseAPI
) -> None:
    # TODO temporary; will need more sophisticated logic to avoid slashing
    db.insert(_key_for_attestation(duty), operation.hash_tree_root)


async def _is_attestation_slashable(
    duty: Duty, operation: Operation, db: SignatoryDatabaseAPI
) -> bool:
    # TODO temporary; will need more sophisticated logic to avoid slashing
    return _key_for_attestation(duty) in db


def _key_for_block_proposal(duty: Duty) -> bytes:
    return (
        OperationTag.BlockProposal.value
        + duty.validator_public_key
        + ssz.encode(duty.tick_for_execution.slot, ssz.sedes.uint64)
    )


async def _record_block_proposal(
    duty: Duty, operation: Operation, db: SignatoryDatabaseAPI
) -> None:
    db.insert(_key_for_block_proposal(duty), operation.hash_tree_root)


async def _is_block_proposal_slashable(
    duty: Duty, operation: Operation, db: SignatoryDatabaseAPI
) -> bool:
    """
    A block proposal is invalid if we already have produced a signature for the proposal's slot.
    """
    return _key_for_block_proposal(duty) in db


SLASHING_VALIDATIONS = {
    DutyType.Attestation: _is_attestation_slashable,
    DutyType.BlockProposal: _is_block_proposal_slashable,
}

SLASHING_RECORDERS = {
    DutyType.Attestation: _record_attestation,
    DutyType.BlockProposal: _record_block_proposal,
}


class InMemorySignatoryDB(SignatoryDatabaseAPI):
    logger = logging.getLogger("eth2.validator_client.signatory_db")

    def __init__(self) -> None:
        self._store: Dict[bytes, bytes] = {}

    async def record_signature_for(self, duty: Duty, operation: Operation) -> None:
        self.logger.debug("recording signature for duty %s", duty)
        recorder = SLASHING_RECORDERS.get(duty.duty_type, None)
        if not recorder:
            raise NotImplementedError(
                f"missing a signature recorder handler for the duty type {duty.duty_type}"
            )

        await recorder(duty, operation, self)

    async def is_slashable(self, duty: Duty, operation: Operation) -> bool:
        validation = SLASHING_VALIDATIONS.get(duty.duty_type, None)
        if not validation:
            raise NotImplementedError(
                f"missing a slashing validation handler for the duty type {duty.duty_type}"
            )

        return await validation(duty, operation, self)

    def insert(self, key: bytes, value: bytes) -> None:
        self._store[key] = value

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, bytes):
            raise Exception("element type is ``bytes``")
        return key in self._store
