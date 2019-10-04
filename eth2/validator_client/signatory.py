import logging

from eth_typing import BLSSignature
from eth_utils import ValidationError
from py_ecc.bls.typing import Domain

from eth2._utils.bls import bls
from eth2._utils.humanize import humanize_bytes
from eth2.beacon.helpers import signature_domain_to_domain_type
from eth2.beacon.typing import Operation
from eth2.validator_client.abc import BeaconNodeAPI, SignatoryDatabaseAPI
from eth2.validator_client.duty import Duty
from eth2.validator_client.typing import PrivateKeyProvider

logger = logging.getLogger("eth2.validator_client.signatory")


async def _validate_duty(
    duty: Duty, operation: Operation, db: SignatoryDatabaseAPI
) -> None:
    """
    ``db`` contains a persistent record of all signatures with
    enough information to prevent the triggering of any slashing conditions.
    """
    if await db.is_slashable(duty, operation):
        raise ValidationError(
            f"signing the duty {duty} would result in a slashable signature"
        )


def sign(
    duty: Duty, operation: Operation, private_key_provider: PrivateKeyProvider
) -> BLSSignature:
    message = operation.hash_tree_root
    private_key = private_key_provider(duty.validator_public_key)
    # TODO use correct ``domain`` value
    # NOTE currently only uses part of the domain value
    # need to get fork from the state and compute the full domain value locally
    domain = Domain(
        b"\x00" * 4 + signature_domain_to_domain_type(duty.signature_domain)
    )
    return bls.sign(message, private_key, domain)


async def sign_and_broadcast_operation_if_valid(
    duty: Duty,
    operation: Operation,
    signature_store: SignatoryDatabaseAPI,
    beacon_node: BeaconNodeAPI,
    private_key_provider: PrivateKeyProvider,
) -> None:
    try:
        await _validate_duty(duty, operation, signature_store)
    except ValidationError as e:
        logger.warn("a duty %s was not valid: %s", duty, e)
        return
    else:
        logger.debug(
            "received a valid duty %s for the operation with hash tree root %s; signing...",
            duty,
            humanize_bytes(operation.hash_tree_root),
        )

    await signature_store.record_signature_for(duty, operation)
    signature = sign(duty, operation, private_key_provider)

    logger.info("got signature %s for duty %s", humanize_bytes(signature), duty)
    await beacon_node.publish(duty, signature)
