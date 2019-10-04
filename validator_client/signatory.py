import logging
from typing import AsyncIterable, Tuple

from async_service.base import Service
from eth_typing import BLSSignature
from eth_utils import ValidationError, humanize_hash
import trio

from eth2._utils.bls import bls
from eth2.beacon.helpers import signature_domain_to_domain_type
from validator_client.context import Context
from validator_client.duty import Duty
from validator_client.signatory_db import BaseSignatoryDB, InMemorySignatoryDB

logger = logging.getLogger("validator_client.signatory")
logger.setLevel(logging.DEBUG)


async def _validate_duty(duty: Duty, signable, db: BaseSignatoryDB) -> None:
    """
    ``db`` contains a persistent record of all signatures with
    enough information to prevent the triggering of any slashing conditions.
    """
    if await db.is_slashable(duty, signable):
        raise ValidationError(
            f"signing the duty {duty} would result in a slashable signature"
        )


class Signatory(Service):
    def __init__(self, context: Context, duty_provider: AsyncIterable[Duty]) -> None:
        self._context = context
        self._duty_provider = duty_provider
        self._signature_store = InMemorySignatoryDB()
        self._send_channel, self._recv_channel = trio.open_memory_channel(0)

    async def _validate(self, duty: Duty, signable) -> None:
        await _validate_duty(duty, signable, self._signature_store)

    async def _sign(self, duty: Duty, signable) -> BLSSignature:
        message = signable.hash_tree_root
        private_key = self._context.private_key_for(duty.validator_public_key)
        # TODO use correct ``domain`` value
        # NOTE currently only uses part of the domain value
        # need to get fork from the state and compute the full domain value locally
        domain = b"\x00" * 4 + signature_domain_to_domain_type(duty.domain_type)
        return bls.sign(message, private_key, domain)

    async def _record_signature_for(self, duty: Duty, signable) -> None:
        await self._signature_store.record_signature_for(duty, signable)

    async def _stop(self) -> None:
        logger.debug("stopping signatory...")
        await self._send_channel.aclose()
        await self._signature_store.close()

    async def _process_duty(self, duty: Duty, signable) -> None:
        try:
            await self._validate(duty, signable)
        except ValidationError as e:
            logger.warn("a duty %s was not valid: %s", duty, e)
            return
        else:
            logger.debug(
                "received a valid duty %s for the operation with hash tree root %s; signing...",
                duty,
                humanize_hash(signable.hash_tree_root),
            )

        await self._record_signature_for(duty, signable)
        signature = await self._sign(duty, signable)

        logger.info("signed %s", duty)

        await self._send_channel.send((duty, signature))

    async def run(self) -> None:
        try:
            async for duties in self._duty_provider:
                for duty, signable in duties:
                    self.manager.run_task(self._process_duty, duty, signable)
        finally:
            with trio.CancelScope(shield=True):
                await self._stop()

    def signatures(self) -> AsyncIterable[Tuple[Duty, BLSSignature]]:
        return self._recv_channel
