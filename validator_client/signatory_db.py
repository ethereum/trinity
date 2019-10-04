from abc import abstractmethod
from enum import Enum, unique
import logging

import ssz

from validator_client.duty import Duty, DutyType

logger = logging.getLogger("validator_client.signatory_db")
logger.setLevel(logging.DEBUG)


@unique
class OperationTag(Enum):
    Attestation = b"0"
    BlockProposal = b"1"


class BaseSignatoryDB:
    """
    Provides persistence for actions of the client to prevent
    the publishing of slashable signatures.
    """

    @abstractmethod
    async def record_signature_for(self, duty: Duty, signable) -> None:
        ...

    @abstractmethod
    async def is_slashable(self, duty: Duty) -> bool:
        ...

    @abstractmethod
    def insert(self, key: bytes, value: bytes) -> None:
        ...

    @abstractmethod
    def __contains__(self, key: bytes) -> bool:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...


def _key_for_attestation(duty: Duty) -> bytes:
    return (
        OperationTag.Attestation.value
        + duty.validator_public_key
        + ssz.encode(duty.slot, ssz.sedes.uint64)
    )


async def _record_attestation(duty: Duty, signable, db: BaseSignatoryDB) -> None:
    # TODO temporary; will need more sophisticated logic to avoid slashing
    db.insert(_key_for_attestation(duty), signable.hash_tree_root)


async def _is_attestation_slashable(duty: Duty, signable, db: BaseSignatoryDB) -> bool:
    # TODO temporary; will need more sophisticated logic to avoid slashing
    return _key_for_attestation(duty) in db


def _key_for_block_proposal(duty: Duty) -> bytes:
    return (
        OperationTag.BlockProposal.value
        + duty.validator_public_key
        + ssz.encode(duty.slot, ssz.sedes.uint64)
    )


async def _record_block_proposal(duty: Duty, signable, db: BaseSignatoryDB) -> None:
    db.insert(_key_for_block_proposal(duty), signable.hash_tree_root)


async def _is_block_proposal_slashable(
    duty: Duty, signable, db: BaseSignatoryDB
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


class InMemorySignatoryDB(BaseSignatoryDB):
    def __init__(self) -> None:
        self._store = {}

    async def record_signature_for(self, duty: Duty, signable) -> None:
        logger.debug("recording signature for duty %s", duty)
        recorder = SLASHING_RECORDERS.get(duty.duty_type, None)
        if not recorder:
            raise NotImplementedError(
                f"missing a signature recorder handler for the duty type {duty.duty_type}"
            )

        await recorder(duty, signable, self)

    async def is_slashable(self, duty: Duty, signable) -> bool:
        validation = SLASHING_VALIDATIONS.get(duty.duty_type, None)
        if not validation:
            raise NotImplementedError(
                f"missing a slashing validation handler for the duty type {duty.duty_type}"
            )

        return await validation(duty, signable, self)

    def insert(self, key: bytes, value: bytes) -> None:
        self._store[key] = value

    def __contains__(self, key: bytes) -> bool:
        return key in self._store

    async def close(self) -> None:
        logger.debug("closing signatory db...")
